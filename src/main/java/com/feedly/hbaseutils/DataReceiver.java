package com.feedly.hbaseutils;

import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.FSUtils;
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
public final class DataReceiver extends Configured implements Tool {

    private final MetricsRegistry _metrics = new MetricsRegistry();

    private final Meter _connections = _metrics.newMeter(DataReceiver.class, "connectionsMeter", "connections", TimeUnit.SECONDS);
    private volatile Meter _opsMeter;
    private volatile Meter _rxMeter;
    
    private volatile Histogram _txlatencyHistogram;
    private volatile Histogram _latencyHistogram;
    private volatile Histogram _largeLatencyHistogram;
    private volatile Histogram _normalLatencyHistogram;

    private volatile Histogram _writeHistogram;
    private volatile Histogram _largeWriteHistogram;
    private volatile Histogram _normalWriteHistogram;

    private final BlockingQueue<Socket> _sockets = new LinkedBlockingQueue<>();
    private AtomicInteger _fileNum = new AtomicInteger();
    
    private int _longReadWarningMillis = 1000;
    private int _largeValueSize = 1024;
    
    private String _receiveLogPath;
    volatile boolean _stop = false;

    private int _iterations;
    private CountDownLatch _latch;

    private Path _path;

    private short _replication;

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
    }
    
    class SocketReceiver extends Thread {
        private ServerSocket _socket;
        
        public SocketReceiver(int port) throws IOException {
            _socket = new ServerSocket(port);
        }
        
        @Override
        public void run() {
            
            while(!_socket.isClosed()) {
                try {
                    Socket conn = _socket.accept();
                    
                    if(_opsMeter == null) {
                        initMeters();
                    }
                    _sockets.add(conn);
                } catch(Exception ex) {
                    if(!_socket.isClosed())
                        ex.printStackTrace();
                }
            }
        }


        public void requestStop() throws IOException {
            _socket.close();
        }
    }
    
    private void initMeters() {
        System.out.println("initializing meters");
        _opsMeter = _metrics.newMeter(DataReceiver.class, "opsMeter", "ops", TimeUnit.SECONDS);
        _rxMeter = _metrics.newMeter(DataReceiver.class, "rxMeter", "bytes", TimeUnit.SECONDS);
        
        _txlatencyHistogram = _metrics.newHistogram(DataReceiver.class, "txLatencyHistogramAll", "nanos", true);
        _latencyHistogram = _metrics.newHistogram(DataReceiver.class, "rxLatencyHistogramAll", "nanos", true);
        _largeLatencyHistogram = _metrics.newHistogram(DataReceiver.class, "rxLatencyHistogramLarge", "nanos", true);
        _normalLatencyHistogram = _metrics.newHistogram(DataReceiver.class, "rxLatencyHistogramSmall", "nanos", true);

        _writeHistogram = _metrics.newHistogram(DataReceiver.class, "writeHistogramAll", "nanos", true);
        _largeWriteHistogram = _metrics.newHistogram(DataReceiver.class, "writeHistogramLarge", "nanos", true);
        _normalWriteHistogram = _metrics.newHistogram(DataReceiver.class, "writeHistogramSmall", "nanos", true);
    }
    
    class SocketReader extends Thread {

        private static final int HEADER_LEN = 12;
        private OpLogWriter _opsWriter;

        public SocketReader(OpLogWriter lw) {
            _opsWriter = lw;
        }
        
        @Override
        public void run() {
            
            byte[] buf = new byte[64*1024];
            long logCutoff = TimeUnit.NANOSECONDS.convert(_longReadWarningMillis, TimeUnit.MILLISECONDS);
            boolean stopAcks = false;
            while(!_stop || !_sockets.isEmpty()) {
                Socket socket;
                try {
                    socket = _sockets.poll(1, TimeUnit.SECONDS);
                    if(socket != null) {
                        Meter opsMeter = _opsMeter;
                        Meter rxMeter = _rxMeter;
                        
                        Histogram txlatencyHistogram = _txlatencyHistogram;
                        Histogram latencyHistogram = _latencyHistogram;
                        Histogram largeLatencyHistogram = _largeLatencyHistogram;
                        Histogram normalLatencyHistogram = _normalLatencyHistogram;

                        Histogram writeHistogram = _writeHistogram;
                        Histogram largeWriteHistogram = _largeWriteHistogram;
                        Histogram normalWriteHistogram = _normalWriteHistogram;

                        int total = 0;
                        _connections.mark();
                        
                        FSDataOutputStream fileStream = null;
                        if(_path != null) {
                            int fileNum = _fileNum.incrementAndGet();
                            Path filePath = new Path(_path, "file"+fileNum);
                            FileSystem fileSystem = FileSystem.get(filePath.toUri(), getConf());
                            fileStream = fileSystem.create(filePath, _replication);
                        }
                        
                        try(InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
                            while(true) {
                                
                                long start = System.nanoTime();
                                Message msg =  Message.receive(in, buf);
                                if(msg != null) {
                                    long latency = System.nanoTime() - start;
                                    int len = msg.bytes().length + HEADER_LEN;
                                    
                                    rxMeter.mark(len);
                                    opsMeter.mark();
                                    latencyHistogram.update(latency);
                                    if(len >= _largeValueSize)
                                        largeLatencyHistogram.update(latency);
                                    else
                                        normalLatencyHistogram.update(latency);
                                    
                                    if(_opsWriter != null)
                                        _opsWriter.log(len, latency);
                                    
                                    if(latency >= logCutoff)
                                        System.out.println(format("long receive %,d bytes in %.2f", len, latency/1e9));
                                    
                                    try {
                                        if(!stopAcks) {
                                            Message ack = new Message(msg.seq(), new byte[] {0});
                                            start = System.nanoTime();
                                            ack.send(out, buf);
                                            out.flush();
                                            latency = System.nanoTime() - start;
                                            txlatencyHistogram.update(latency);
                                        }
                                    } catch(Exception ex) {
                                        stopAcks = true;
                                    }
                                    total += len;

                                    if(fileStream != null) {
                                        start = System.nanoTime();
                                        fileStream.writeLong(msg.seq());
                                        fileStream.writeInt(len);
                                        fileStream.write(msg.bytes());
                                        fileStream.sync();
                                        latency = System.nanoTime() - start;
                                        writeHistogram.update(latency);
                                        if(len >= _largeValueSize)
                                            largeWriteHistogram.update(latency);
                                        else
                                            normalWriteHistogram.update(latency);
                                        
                                        if(latency >= logCutoff)
                                            System.out.println(format("long write %,d bytes in %.2f", len, latency/1e9));
                                    }
                                    
                                    _latch.countDown();
                                    
                                } else {
                                    break;
                                }

                            } 
                        } catch(Exception ex) {
                            ex.printStackTrace();
                        } finally {
                            if(fileStream != null)
                                fileStream.close();
                        }

                        System.out.println("read " + total);
                    } 
                } catch (Exception ex1) {
                    ex1.printStackTrace();
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        int report = 20;
        int port = 8484;
        
        Configuration conf = getConf();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.setInt("hbase.regionserver.hlog.tolerable.lowreplication", 1);
        
        _replication = 1;
        Integer blockSize = null;
        _receiveLogPath = null;
        _iterations = 10000;
        _path = null;//new Path("hdfs://localhost:9000/tmp/writetest2");
        for (int i = 0; i < args.length; i++) {
            String cmd = args[i];
            try {
                if (cmd.equals("-largeValueSize")) {
                    _largeValueSize = Integer.parseInt(args[++i]);
                    System.out.println("large value size " + _largeValueSize);
                } else if (cmd.equals("-longOp")) {
                    _longReadWarningMillis = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-report")) {
                    report = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-replication")) {
                    _replication = Short.parseShort(args[++i]);
                    System.out.println("replication " + _replication);
                } else if (cmd.equals("-opLog")) {
                    _receiveLogPath = args[++i];
                } else if (cmd.equals("-port")) {
                    port = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-path")) {
                    _path = new Path(args[++i]);
                } else if (cmd.equals("-iterations")) {
                    _iterations = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("UNEXPECTED: " + cmd);
                    printUsageAndExit();
                }
            } catch (Exception e) {
                e.printStackTrace();
                printUsageAndExit();
            }
        }
        
        if(_path != null) {
            FSUtils.setFsDefault(conf, _path);
            FileSystem fs = FileSystem.get(conf);
            System.out.println("FileSystem: " + fs);

            _path = _path.makeQualified(fs);
            if (fs.exists(_path)) {
                fs.delete(_path, true);
            }
            fs.mkdirs(_path);
            FSUtils.setRootDir(conf, _path);
        }
        
        _latch = new CountDownLatch(_iterations);
        ConsoleReporter.enable(_metrics, report, TimeUnit.SECONDS);
        
        final ConsoleReporter reporter = new ConsoleReporter(_metrics,
                                                             System.out,
                                                             MetricPredicate.ALL);

        
        final SocketReceiver receiver = new SocketReceiver(port);
        receiver.start();
        System.out.println("started receiver on " + port);
        
        final OpLogWriter lw;
        if(_receiveLogPath != null) {
            lw = new OpLogWriter(_receiveLogPath);
            lw.start();
        } else
            lw = null;

        final SocketReader reader = new SocketReader(lw);
        reader.start();

        _latch.await();
        
        _stop = true;
        receiver.requestStop();
        receiver.join();
        reader.join();
        System.out.println("shutting down");
        System.out.println("*** FINAL metric numbers (may be slower than actual) ***");
        reporter.run();
        if(lw != null) {
            lw.requestStop();
            lw.join();
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

        ToolRunner.run(HBaseConfiguration.create(), new DataReceiver(), args);
    }
}