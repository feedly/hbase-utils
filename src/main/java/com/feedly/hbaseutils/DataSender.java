package com.feedly.hbaseutils;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * This class receives data from the network and optionally writes it out to a file. It is meant to be used in conjunction with the {@link DataReceiver}
 * class to do networking and file system performance checks.
 * <p>
 * See the {@link #run(String[])} method for the various options available.
 * 
 */
public final class DataSender extends Configured implements Tool {

    private final MetricsRegistry _metrics = new MetricsRegistry();

    private final Meter _connections = _metrics.newMeter(DataSender.class, "connectionsMeter", "connections", TimeUnit.SECONDS);
    private final Meter _opsMeter = _metrics.newMeter(DataSender.class, "opsMeter", "ops", TimeUnit.SECONDS);
    private final Meter _txMeter = _metrics.newMeter(DataSender.class, "txMeter", "bytes", TimeUnit.SECONDS);
    
    private final Histogram _rxLatencyHistogram = _metrics.newHistogram(DataSender.class, "rxLatencyHistogramAll", "nanos", true);
    private final Histogram _latencyHistogram = _metrics.newHistogram(DataSender.class, "txLatencyHistogramAll", "nanos", true);
    private final Histogram _largeLatencyHistogram = _metrics.newHistogram(DataSender.class, "txLatencyHistogramLarge", "nanos", true);
    private final Histogram _normalLatencyHistogram = _metrics.newHistogram(DataSender.class, "txLatencyHistogramSmall", "nanos", true);

    private byte[] _bytes;
    private volatile boolean _stop;
    private CountDownLatch _latch;
    
    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
    }

    class AckReceiver extends Thread {
        private InputStream _in;
        
        public AckReceiver(InputStream in) { 
            _in = in;
        }
        
        @Override
        public void run() {
            byte[] buffer = new byte[256];
            int acked = 0;
            while(!_stop && _latch.getCount() > 0) {
                try {
                    long before = System.nanoTime();
                    Message.receive(_in, buffer); //ack
                    long latency = System.nanoTime() - before;
                    _rxLatencyHistogram.update(latency);
                    acked++;
                    _latch.countDown();
                } catch(Exception ex) {
                    break;
                }
                
            }
            System.out.println("received " + acked + " acks");
        }
    }
    
    
    @Override
    public int run(String[] args) throws Exception {

        int report = 20;
        int iterations = 10000;
        int longWriteWarningMillis = 1000;
        int largeValueSize = 3*1024*1024;
        int valueSize = 512;
        int keySize = 16;
        String remoteHost = "localhost";
        int remotePort = 8484, localPort = 0;
        Configuration conf = getConf();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.setInt("hbase.regionserver.hlog.tolerable.lowreplication", 1);
        
        Integer replication = null;
        String opsPath = null;
        int largeValueInterval = 1000;
        
        for (int i = 0; i < args.length; i++) {
            String cmd = args[i];
            try {
                if (cmd.equals("-largeValueSize")) {
                    largeValueSize = Integer.parseInt(args[++i]);
                    System.out.println("large value size " + largeValueSize);
                } else if (cmd.equals("-largeValueInterval")) {
                    largeValueInterval = Integer.parseInt(args[++i]);
                    System.out.println("iters between large value " + largeValueInterval);
                } else if (cmd.equals("-valueSize")) {
                    valueSize = Integer.parseInt(args[++i]);
                    System.out.println("value size " + largeValueSize);
                } else if (cmd.equals("-keySize")) {
                    keySize = Integer.parseInt(args[++i]);
                    System.out.println("key size " + largeValueSize);
                } else if (cmd.equals("-longOp")) {
                    longWriteWarningMillis = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-iterations")) {
                     iterations = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-report")) {
                    report = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-replication")) {
                    replication = new Integer(args[++i]);
                    System.out.println("replication " + replication);
                } else if (cmd.equals("-opLog")) {
                    opsPath = args[++i];
                } else if (cmd.equals("-host")) {
                    remoteHost = args[++i];
                } else if (cmd.equals("-port")) {
                    remotePort = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-localPort")) {
                    localPort = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("UNEXPECTED: " + cmd);
                    printUsageAndExit();
                }
            } catch (Exception e) {
                printUsageAndExit();
            }
        }
        
        _latch = new CountDownLatch(iterations);
        _bytes = new byte[ Math.max(10*valueSize, 3*largeValueSize) + keySize + 17];
        Random rand = new Random();
        rand.nextBytes(_bytes);

        try {
            ConsoleReporter.enable(_metrics, report, TimeUnit.SECONDS);
            
            final ConsoleReporter reporter = new ConsoleReporter(_metrics,
                                                                 System.out,
                                                                 MetricPredicate.ALL);

            final OpLogWriter lw;
            if(opsPath != null) {
                lw = new OpLogWriter(opsPath);
                lw.start();
            } else
                lw = null;

            int start = rand.nextInt(_bytes.length/2);
            long longSyncNs = TimeUnit.NANOSECONDS.convert(longWriteWarningMillis, TimeUnit.MILLISECONDS);

            byte[] sendBuffer = new byte[64*1024];
            Socket socket;
            
            if(localPort > 0)
                socket = new Socket(remoteHost, remotePort, InetAddress.getLocalHost(), localPort);
            else
                socket = new Socket(remoteHost, remotePort);
            
            _connections.mark();
            try(InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
                AckReceiver ack = new AckReceiver(in);
                ack.start();
                for(int i = 0; i < iterations; i++) {
                    boolean writeLargeValue = largeValueInterval > 0 && i % largeValueInterval == 0;
                    int currValueSize = writeLargeValue ? largeValueSize : valueSize;
                    byte[] sendBytes = new byte[keySize + currValueSize];
                    if(start + keySize > _bytes.length)
                        start = (start + keySize) - _bytes.length;
                    
                    System.arraycopy(_bytes, start, sendBytes, 0, keySize);
                    start += keySize;
                    
                    if(start + currValueSize > _bytes.length)
                        start = (start + currValueSize) - _bytes.length;
                    
                    System.arraycopy(_bytes, start, sendBytes, keySize, currValueSize);
                    start += currValueSize;
                    
                    Message msg = new Message(i, sendBytes);
                    long before = System.nanoTime();
                    msg.send(out, sendBuffer);
                    long latency = System.nanoTime() - before;
                    
                    _latencyHistogram.update(latency);
                    if(writeLargeValue)
                        _largeLatencyHistogram.update(latency);
                    else
                        _normalLatencyHistogram.update(latency);
                        
                    _opsMeter.mark();
                    _txMeter.mark(sendBytes.length);

                    if(latency >= longSyncNs) 
                        System.out.println("long transmit of " + sendBytes.length + " in " + latency/1e9);
                }

                System.out.println("*** all data sent ***");
                reporter.run();
                _latch.await();
                System.out.println("*** all acks received ***");
                _stop = true;
                socket.close();
                ack.interrupt();
                ack.join();
            } finally {
            }
            
            System.out.println("shutting down");
            System.out.println("*** FINAL metric numbers (may be slower than actual) ***");
            reporter.run();
            
            if(lw != null) {
                lw.requestStop();
                lw.join();
            }
        } finally {
        }

        return 0;
    }

    private void printUsageAndExit() {
        System.out.println("not implemented...");
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {

        //verify logging is configured as desired
        BasicConfigurator.configure();
        Log log = LogFactory.getLog(HLog.class);
        System.out.println(log.getClass().getName());
        log.trace("trace");
        log.debug("debug");
        log.info("info");
        log.warn("warn");
        log.error("error");
        
        System.out.println("ROOT LEVEL");
        
        log = LogFactory.getLog("");
        System.out.println(log.getClass().getName());
        log.trace("trace");
        log.debug("debug");
        log.info("info");
        log.warn("warn");
        log.error("error");

        ToolRunner.run(HBaseConfiguration.create(), new DataSender(), args);
    }
}