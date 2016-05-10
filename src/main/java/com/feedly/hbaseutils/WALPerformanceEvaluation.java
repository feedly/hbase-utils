package com.feedly.hbaseutils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * This class runs performance benchmarks for {@link WAL}. See usage for this
 * tool by running:
 * <code>$ hbase org.apache.hadoop.hbase.wal.WALPerformanceEvaluation -h</code>
 */
public final class WALPerformanceEvaluation extends Configured implements Tool {
    private static final Log _logger = LogFactory.getLog(WALPerformanceEvaluation.class);

    private final MetricsRegistry metrics = new MetricsRegistry();
    private final Meter syncMeter = metrics.newMeter(WALPerformanceEvaluation.class, "syncMeter", "syncs",
                                                     TimeUnit.SECONDS);

    //sync-ing every single time, these aren't needed
//    private final Histogram syncHistogram = metrics.newHistogram(WALPerformanceEvaluation.class, "syncHistogram",
//                                                                 "nanos-between-syncs", true);
//    private final Histogram syncCountHistogram = metrics.newHistogram(WALPerformanceEvaluation.class,
//                                                                      "syncCountHistogram", "countPerSync", true);

    private final Meter appendMeter = metrics.newMeter(WALPerformanceEvaluation.class, "appendMeter", "bytes",
                                                       TimeUnit.SECONDS);
    
    private final Histogram latencyHistogram = metrics.newHistogram(WALPerformanceEvaluation.class, "latencyHistogramAll",
                                                                    "nanos", true);

    private final Histogram largeLatencyHistogram = metrics.newHistogram(WALPerformanceEvaluation.class, "latencyHistogramLarge",
                                                                    "nanos", true);
    private final Histogram normalLatencyHistogram = metrics.newHistogram(WALPerformanceEvaluation.class, "latencyHistogramSmall",
                                                                    "nanos", true);

    static final String TABLE_NAME = "WALPerformanceEvaluation";
    static final String QUALIFIER_PREFIX = "q";
    static final String FAMILY_PREFIX = "cf";

    private int numQualifiers = 1;
    private int largeValueInterval = 1000;
    private int largeValueSize = 3*1024*1024;
    private int longSyncWarningMillis = 1000;
    
    private int valueSize = 512;
    private int keySize = 16;
    private byte[] bytes;
    private Throttle syncThrottle;
    private final BlockingQueue<HRegion> _rollRequests = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> _syncLog = new LinkedBlockingQueue<>();
    long roll = Long.MAX_VALUE;

    private String _syncLogPath;

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
    }
    
    class SyncLogWriter extends Thread {
        volatile boolean stop = false;
        
        public SyncLogWriter() throws IOException {
            if(_syncLogPath != null) {
                File f = new File(_syncLogPath);
                f.createNewFile();
                if(!f.canWrite()) {
                    throw new IllegalArgumentException("can't write sync logs, invalid path");
                }
            }
        }
        
        @Override
        public void run() {
            if(_syncLogPath != null) {
                try(BufferedWriter w = new BufferedWriter(new FileWriter(_syncLogPath))) {
                    while(!stop || !_syncLog.isEmpty()) {
                        String syncLine = _syncLog.poll(1, TimeUnit.SECONDS);
                        if(syncLine != null) {
                            w.write(syncLine);
                            w.write("\n");
                        }
                    }
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
    
    class LogRoller implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    HRegion region = _rollRequests.take();
                    System.out.println(new Date() + ": start LOG ROLL for " + region.getRegionInfo().getEncodedName());
                    HLog log = region.getLog();
                    
                    //let the roller know we can remove old files
                    byte[] name = region.getRegionInfo().getEncodedNameAsBytes();
                    log.startCacheFlush(name);
                    log.completeCacheFlush(name, "ignored".getBytes(), 100, false);
                    
                    log.rollWriter(true);
                    System.out.println(new Date() + ": end LOG ROLL for " + region.getRegionInfo().getEncodedName());
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
                
            }
        }
    }

    class Throttle implements Runnable {
        final double rate;
        private volatile int _pos;
        private int[] hist = new int[60*10];
        private volatile int len = 1;
        
        public Throttle(double maxRate) {
            rate = maxRate;
        }
        
        public void mark() {
            hist[_pos]++;
        }
      
        public void check() {
            while(ratePerSec() > rate) {
                try {
                    Thread.sleep(100);
                } catch(Exception ex) {}
            }
        }
        
        public double ratePerSec() {
            int total = 0;
            for(int i = 0; i < hist.length; i++) {
                int val = hist[i];
                if(val > 0) {
                    total += val;
                }
            }
            
            return total / (double) len;
        }
        
        public void run() {
            while(true) {
                try {
                    Thread.sleep(1000);
                    int pos = (_pos+1) % hist.length;
                    hist[pos] = 0;
                    _pos = pos;
                    len = Math.min(len+1, hist.length);
                } catch(Exception ex) {}
            }
        }
    }
    
    /**
     * Perform WAL.append() of Put object, for the number of iterations
     * requested. Keys and Vaues are generated randomly, the number of column
     * families, qualifiers and key/value size is tunable by the user.
     */
    class WALPutBenchmark implements Runnable {
        private final long numIterations;
        private final int numFamilies;
        private final HRegion region;
        private final HTableDescriptor htd;
        WALPutBenchmark(final HRegion region, final HTableDescriptor htd, final long numIterations, final double traceFreq) {
            this.numIterations = numIterations;
            this.numFamilies = htd.getColumnFamilies().length;
            this.region = region;
            this.htd = htd;
        }

        @Override
        public void run() {
            byte[] key = new byte[keySize];
            byte[] value = new byte[valueSize];
            byte[] largeValue = new byte[largeValueSize];
            Random rand = new Random(Thread.currentThread().getId());
            int start = rand.nextInt(bytes.length/2);
            HLog wal = region.getLog();
            
            long longSyncNs = TimeUnit.NANOSECONDS.convert(longSyncWarningMillis, TimeUnit.MILLISECONDS);
            
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
                long startTime = System.currentTimeMillis();
                for (int i = 0; i < numIterations; ++i) {
                    try {
                        byte[] colValue = value;
                        
                        int pos = i+start;
                        boolean writeLargeValue = largeValueInterval > 0 && pos % largeValueInterval == 0;
                        if(writeLargeValue) {
                            colValue = largeValue;
                        } 
                        
                        Put put = setupPut(bytes, key, colValue, numFamilies, pos);
                        long len = 0;
                        for(List<KeyValue> l : put.getFamilyMap().values())
                            for(KeyValue kv : l)
                                len += kv.getLength();
                        
                        WALEdit walEdit = new WALEdit();
                        addFamilyMapToWALEdit(put.getFamilyMap(), walEdit);
                        HRegionInfo hri = region.getRegionInfo();
                        long now = System.nanoTime();
                        final HLogKey logkey = new HLogKey(hri.getEncodedNameAsBytes(), hri.getTableName(), i, now, HConstants.DEFAULT_CLUSTER_ID);
                        wal.append(hri, logkey, walEdit, htd, true);
                        long latency = System.nanoTime() - now;
                        latencyHistogram.update(latency);
                        appendMeter.mark(len);
                        syncMeter.mark(1);

                        if(writeLargeValue) {
                            largeLatencyHistogram.update(latency);
                        }
                        else {
                            normalLatencyHistogram.update(latency);
                        }
                        
                        if(_syncLogPath != null)
                            _syncLog.add(String.format("%s %d %d", sdf.format(new Date()), len, latency));
                        if(latency > longSyncNs)
                            System.out.println(new Date() + ": long sync: " + latency/1e9 + " of len " + colValue.length);
                        
                        if(syncThrottle != null) {
                            syncThrottle.mark();
                            syncThrottle.check();
                        }
                        if(i % roll == 0) {
                            roll();
                        }
                    } finally {
                    }
                }
                long totalTime = (System.currentTimeMillis() - startTime);
                logBenchmarkResult(Thread.currentThread().getName() + " THREAD COMPLETE", numIterations, totalTime);
            } catch (Exception e) {
                _logger.error(getClass().getSimpleName() + " Thread failed", e);
            } finally {
            }
        }

        private void roll() {
            _rollRequests.add(region);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Path rootRegionDir = null;
        int numThreads = 1;
        long numIterations = 1000000;
        int numFamilies = 1;
        int maxRate = Integer.MAX_VALUE;

        boolean compress = false;
        String cipher = null;
        int numRegions = 1;
        int report = 20;
        Configuration conf = getConf();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.setInt("hbase.regionserver.hlog.tolerable.lowreplication", 1);
        
        String spanReceivers = conf.get("hbase.trace.spanreceiver.classes");
        boolean trace = spanReceivers != null && !spanReceivers.isEmpty();
        double traceFreq = 1.0;
        Integer replication = null;
        Integer blockSize = null;
        _syncLogPath = null;
        
        if(args.length == 0)
            printUsageAndExit();
        
        for (int i = 0; i < args.length; i++) {
            String cmd = args[i];
            try {
                if (cmd.equals("-threads")) {
                    numThreads = Integer.parseInt(args[++i]);
                    System.out.println("threads " + numThreads);
                } else if (cmd.equals("-iterations")) {
                    String s = args[++i];
                    numIterations = "forever".equals(s) ? Long.MAX_VALUE : Long.parseLong(s);
                    System.out.println("iterations " + s);
                } else if (cmd.equals("-path")) {
                    rootRegionDir = new Path(args[++i]);
                    System.out.println("writing to " + rootRegionDir);
                } else if (cmd.equals("-families")) {
                    numFamilies = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-qualifiers")) {
                    numQualifiers = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-keySize")) {
                    keySize = Integer.parseInt(args[++i]);
                    System.out.println("key size " + keySize);
                } else if (cmd.equals("-valueSize")) {
                    valueSize = Integer.parseInt(args[++i]);
                    System.out.println("value size " + valueSize);
                } else if (cmd.equals("-largeValueSize")) {
                    largeValueSize = Integer.parseInt(args[++i]);
                    System.out.println("large value size " + largeValueSize);
                } else if (cmd.equals("-largeValueInterval")) {
                    largeValueInterval = Integer.parseInt(args[++i]);
                    System.out.println("iters between large value " + largeValueInterval);
                } else if (cmd.equals("-maxRate")) {
                    maxRate = Integer.parseInt(args[++i]);
                    System.out.println("max syncs per sec " + maxRate);
                } else if (cmd.equals("-longSync")) {
                    longSyncWarningMillis = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-report")) {
                    report = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-replication")) {
                    replication = new Integer(args[++i]);
                    System.out.println("replication " + replication);
                } else if (cmd.equals("-blockSize")) {
                    blockSize = new Integer(args[++i]);
                    System.out.println("blockSize " + blockSize);
                } else if (cmd.equals("-roll")) {
                    roll = Long.parseLong(args[++i]);
                } else if (cmd.equals("-compress")) {
                    compress = true;
                } else if (cmd.equals("-encryption")) {
                    cipher = args[++i];
                } else if (cmd.equals("-regions")) {
                    numRegions = Integer.parseInt(args[++i]);
                } else if (cmd.equals("-syncLog")) {
                    _syncLogPath = args[++i];
                } else if (cmd.equals("-traceFreq")) {
                    traceFreq = Double.parseDouble(args[++i]);
                } else if (cmd.equals("-h")) {
                    printUsageAndExit();
                } else if (cmd.equals("--help")) {
                    printUsageAndExit();
                } else {
                    System.err.println("UNEXPECTED: " + cmd);
                    printUsageAndExit();
                }
            } catch (Exception e) {
                printUsageAndExit();
            }
        }
        
        if(replication != null)
            conf.setInt("hbase.regionserver.hlog.replication", replication);
        if(blockSize != null)
            conf.setLong("hbase.regionserver.hlog.blocksize", blockSize);
        if(compress)
            conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
        
        this.bytes = new byte[ Math.max(10*(keySize+valueSize), largeValueSize)];
        new Random().nextBytes(bytes);
//        if (compress) {
//            Configuration conf = getConf();
//            conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
//        }

        if (cipher != null) {
            throw new IllegalArgumentException("disabled");
            // Set up WAL for encryption
            // Configuration conf = getConf();
            // conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY,
            // KeyProviderForTesting.class.getName());
            // conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
            // conf.setClass("hbase.regionserver.hlog.reader.impl",
            // SecureProtobufLogReader.class, HLog.Reader.class);
            // conf.setClass("hbase.regionserver.hlog.writer.impl",
            // SecureProtobufLogWriter.class, Writer.class);
            // conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
            // conf.set(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, cipher);
        }

        if (numThreads < numRegions) {
            _logger.warn("Number of threads is less than the number of regions; some regions will sit idle.");
        }

        // Internal config. goes off number of threads; if more threads than
        // handlers, stuff breaks.
        // In regionserver, number of handlers == number of threads.
        conf.setInt("hbase.regionserver.handler.count", numThreads);


        if (trace)
            throw new IllegalArgumentException("disabled");
        // SpanReceiverHost receiverHost = trace ?
        // SpanReceiverHost.getInstance(getConf()) : null;
//        org.apache.htrace.TraceScope scope = org.apache.htrace.Trace.startSpan("WALPerfEval",
//                                                                               org.apache.htrace.Sampler.NEVER);

        try {
            if (rootRegionDir == null) {
                System.out.println("-path required");
                System.exit(1);
                // rootRegionDir =
                // TEST_UTIL.getDataTestDirOnTestFS("WALPerformanceEvaluation");
            }
            
            SyncLogWriter slw = null;
            if(_syncLogPath != null) {
                slw = new SyncLogWriter();
                slw.start();
            }
            
            // Run WAL Performance Evaluation
            // First set the fs from configs. In case we are on hadoop1
            FSUtils.setFsDefault(conf, rootRegionDir);
            FileSystem fs = FileSystem.get(conf);
            _logger.info("FileSystem: " + fs);

            rootRegionDir = rootRegionDir.makeQualified(fs);
            cleanRegionRootDir(fs, rootRegionDir);
            FSUtils.setRootDir(conf, rootRegionDir);
            // final WALFactory wals = new WALFactory(getConf(), null, "wals");
            final HRegion[] regions = new HRegion[numRegions];
            final Runnable[] benchmarks = new Runnable[numRegions];

            if(maxRate > 0) {
                syncThrottle = new Throttle(maxRate);
                Thread t = new Thread(syncThrottle);
                t.setDaemon(true);
                t.start();
            }
            
            Thread rollerThread = new Thread(new LogRoller());
            rollerThread.setDaemon(true);
            rollerThread.start();
            
            try {
                for (int i = 0; i < numRegions; i++) {
                    // Initialize Table Descriptor
                    // a table per desired region means we can avoid carving up
                    // the key space
                    final HTableDescriptor htd = createHTableDescriptor(i, numFamilies);
                    HRegionInfo regionInfo = new HRegionInfo(htd.getName());
                    regions[i] = HRegion.createHRegion(regionInfo, rootRegionDir, conf, htd, null);
                    benchmarks[i] = new WALPutBenchmark(regions[i], htd, numIterations, traceFreq);
                }
                ConsoleReporter.enable(this.metrics, report, TimeUnit.SECONDS);
                
                final ConsoleReporter reporter = new ConsoleReporter(this.metrics,
                                                                     System.out,
                                                                     MetricPredicate.ALL);
                reporter.start(report, TimeUnit.SECONDS);

                long putTime = runBenchmark(benchmarks, numThreads);
                System.out.println("*** FINAL metric numbers (may be slower than actual) ***");
                reporter.run();
                logBenchmarkResult(String.format("Summary: threads=%,d, iterations=%,d", numThreads, numIterations), numIterations * numThreads, putTime);
                for (int i = 0; i < numRegions; i++) {
                    if (regions[i] != null) {
                        closeRegion(regions[i]);
                        regions[i] = null;
                    }
                }
//                if (verify) {
//                    LOG.info("verifying written log entries.");
//                    Path dir = new Path(FSUtils.getRootDir(getConf()), DefaultWALProvider.getWALDirectoryName("wals"));
//                    long editCount = 0;
//                    FileStatus[] fsss = fs.listStatus(dir);
//                    if (fsss.length == 0)
//                        throw new IllegalStateException("No WAL found");
//                    for (FileStatus fss : fsss) {
//                        Path p = fss.getPath();
//                        if (!fs.exists(p))
//                            throw new IllegalStateException(p.toString());
//                        editCount += verify(wals, p, verbose);
//                    }
//                    long expected = numIterations * numThreads;
//                    if (editCount != expected) {
//                        throw new IllegalStateException("Counted=" + editCount + ", expected=" + expected);
//                    }
//                }
            } finally {
                for (int i = 0; i < numRegions; i++) {
                    if (regions[i] != null) {
                        closeRegion(regions[i]);
                    }
                }
                
                if(slw != null) {
                    slw.stop = true;
                    slw.join();
                }
//                wals.shutdown();
                // Remove the root dir for this test region
//                if (cleanup)
//                    cleanRegionRootDir(fs, rootRegionDir);
            }
        } finally {
            // We may be called inside a test that wants to keep on using the
            // fs.
//            if (!noclosefs)
//                fs.close();
//            scope.close();

            // if (receiverHost != null) receiverHost.closeReceivers();
        }

        return 0;
    }

    private static HTableDescriptor createHTableDescriptor(final int regionNum, final int numFamilies) {
        HTableDescriptor htd = new HTableDescriptor(TABLE_NAME + "-" + regionNum);
        for (int i = 0; i < numFamilies; ++i) {
            HColumnDescriptor colDef = new HColumnDescriptor(FAMILY_PREFIX + i);
            htd.addFamily(colDef);
        }
        return htd;
    }

    /**
     * Verify the content of the WAL file. Verify that the file has expected
     * number of edits.
     * 
     * @param wals
     *            may not be null
     * @param wal
     * @return Count of edits.
     * @throws IOException
     */
//    private long verify(final WALFactory wals, final Path wal, final boolean verbose) throws IOException {
//        WAL.Reader reader = wals.createReader(wal.getFileSystem(getConf()), wal);
//        long count = 0;
//        Map<String, Long> sequenceIds = new HashMap<String, Long>();
//        try {
//            while (true) {
//                WAL.Entry e = reader.next();
//                if (e == null) {
//                    LOG.debug("Read count=" + count + " from " + wal);
//                    break;
//                }
//                count++;
//                long seqid = e.getKey().getLogSeqNum();
//                if (sequenceIds.containsKey(Bytes.toString(e.getKey().getEncodedRegionName()))) {
//                    // sequenceIds should be increasing for every regions
//                    if (sequenceIds.get(Bytes.toString(e.getKey().getEncodedRegionName())) >= seqid) {
//                        throw new IllegalStateException("wal = " + wal.getName() + ", " + "previous seqid = "
//                                + sequenceIds.get(Bytes.toString(e.getKey().getEncodedRegionName()))
//                                + ", current seqid = " + seqid);
//                    }
//                }
//                // update the sequence Id.
//                sequenceIds.put(Bytes.toString(e.getKey().getEncodedRegionName()), seqid);
//                if (verbose)
//                    LOG.info("seqid=" + seqid);
//            }
//        } finally {
//            reader.close();
//        }
//        return count;
//    }

    private static void logBenchmarkResult(String testName, long numTests, long totalTime) {
        float tsec = totalTime / 1000.0f;
        System.out.println(String.format("%s took %.3fs %.3fops/s", testName, tsec, numTests / tsec));
    }

    private void printUsageAndExit() {
        System.err.printf("Usage: bin/hbase %s [options]\n", getClass().getName());
        System.err.println(" where [options] are:");
        System.err.println("  -h|-help                  Show this help and exit.");
        System.err.println("  -threads <N>              Number of threads writing on the WAL.");
        System.err.println("  -regions <N>              Number of regions to open in the WAL. Default: 1");
        System.err.println("  -iterations <N>           Number of iterations per thread (or \"forever\").");
        System.err.println("  -path <PATH>              Path where region's root directory is created.");
        System.err.println("  -longSync <N>             syncs longer than this (in millis) will be logged");
        System.err.println("  -keySize <N>              Row key size in byte.");
        System.err.println("  -valueSize <N>            Row/Col value size in byte (def 512).");
        System.err.println("  -largeValueSize <N>       Row/Col value size in byte (def: 3mb).");
        System.err.println("  -largeValueInterval <N>   syncs between large value writes (def: 1000).");
        System.err.println("  -families <N>             Number of column families to write.");
        System.err.println("  -qualifiers <N>           Number of qualifiers to write.");
        System.err.println("  -roll <N>                 Roll the way every N appends");
        System.err.println("  -replication <N>          replication count");
        System.err.println("  -blockSize <N>            block size (bytes)");
        System.err.println("  -report <N>               report interval for stats (seconds)");
        System.err.println("  -syncLog <N>              file name to record sync timings");
//        System.err.println("  -nocleanup                Do NOT remove test data when done.");
//        System.err.println("  -noclosefs                Do NOT close the filesystem when done.");
//        System.err.println("  -nosync                   Append without syncing");
//        System.err.println("  -syncInterval <N>          Append N edits and then sync. " + "Default=0, i.e. sync every edit.");
//        System.err.println("  -verify                   Verify edits written in sequence");
//        System.err.println("  -verbose                  Output extra info; " + "e.g. all edit seq ids when verifying");
//        System.err.println("  -encryption <A>           Encrypt the WAL with algorithm A, e.g. AES");
//        System.err.println("  -traceFreq <N>            Rate of trace sampling. Default: 1.0, "
//                + "only respected when tracing is enabled, ie -Dhbase.trace.spanreceiver.classes=...");
//        System.err.println("");
        System.err.println("Examples:");
        System.err.println("");
        System.err.println(" To run 1 threads on hdfs with log rolling every 10k edits and "
                + "verification afterward do:");
        System.err.println(" $ -nocleanup -path /tmp/hlog -threads 1 -roll 35000 -iterations forever");
//        System.err.println("    -conf ./core-site.xml -path hdfs://example.org:7000/tmp "
//                + "-threads 100 -roll 10000 -verify");
        System.exit(1);
    }

    private void closeRegion(final HRegion region) throws IOException {
        if (region != null) {
            region.close();
            HLog wal = region.getLog();
            if (wal != null) {
                wal.close();
            }
        }
    }

    private void cleanRegionRootDir(final FileSystem fs, final Path dir) throws IOException {
        if (fs.exists(dir)) {
            fs.delete(dir, true);
        }
    }

    private Put setupPut(byte[] rand, byte[] key, byte[] value, final int numFamilies, int pos) {
        
        if(pos > rand.length)
            pos = 0;
        int len = Math.min(rand.length - pos, key.length);
        System.arraycopy(rand, pos, key, 0, len);
        Put put = new Put(key);
        for (int cf = 0; cf < numFamilies; ++cf) {
            for (int q = 0; q < numQualifiers; ++q) {
                pos++;
                if(pos > rand.length)
                    pos = 0;
                len = Math.min(rand.length - pos, value.length);
                System.arraycopy(rand, pos, value, 0, len);
                
                put.add(Bytes.toBytes(FAMILY_PREFIX + cf), Bytes.toBytes(QUALIFIER_PREFIX + q), value);
            }
        }
        
        return put;
    }

    private void addFamilyMapToWALEdit(Map<byte[], List<KeyValue>> familyMap, WALEdit walEdit) {
        for (List<KeyValue> edits : familyMap.values()) {
            for (KeyValue cell : edits) {
                walEdit.add(cell);
            }
        }
    }

    private long runBenchmark(Runnable[] runnable, final int numThreads) throws InterruptedException {
        Thread[] threads = new Thread[numThreads];
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numThreads; ++i) {
            threads[i] = new Thread(runnable[i % runnable.length], "t" + i + ",r" + (i % runnable.length));
            threads[i].start();
        }
        for (Thread t : threads)
            t.join();
        long endTime = System.currentTimeMillis();
        return (endTime - startTime);
    }

    /**
     * The guts of the {@link #main} method. Call this method to avoid the
     * {@link #main(String[])} System.exit.
     * 
     * @param args
     * @return errCode
     * @throws Exception
     */
    static int innerMain(final Configuration c, final String[] args) throws Exception {
        return ToolRunner.run(c, new WALPerformanceEvaluation(), args);
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

        System.exit(innerMain(HBaseConfiguration.create(), args));
    }
}