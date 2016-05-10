# hbase-utils
At feedly we ran into a situation where hbase's log sync latency kept incresasing. We used the code in this repo to help diagnose the problem.
It runs on hbase 0.94/hadoop 1.1.

As a brief refresher, the hbase write path consists of immediately writing a log entry of the data to hdfs and then buffering the write in 
memory in the memstore. At some later point, the memstore is flushed to disk as immutable files. The purpose of the log sync is to be able to
gracefully recover from region server crashes.

There will be many, many more log sync operations than memstore flushes. Our particular issue was a faulty network connection to a couple of
data nodes, so this showed up most clearly in the log sync latency. It was hard to pin down the problematic nodes as in hbase 0.94 the hdfs
write pipeline is fairly opaque. You can get this information by running the regionserver and data node at the DEBUG log level. This does
produce a huge amount of log data, so having a separate test made debugging much easier. 

There are 2 tests we used:
## WALPerformanceEvaluation
This is a rough backport of the 1.x code used to benchmark wal writes. It configures the HLog class so as to run outside of hbase. 
Interesting things to do are to run this code on various data nodes (remember that one of the hdfs replica writes will be local) and with
varying replication levels. Writes can be done to the local file system (file:/) or hdfs (hdfs://).We first found our problematic data nodes
by running this test with hbase and hdfs logging set to DEBUG. The hdfs client code will log the write pipeline when opening a new stream. 
Thus by cross referencing the test output for slow performance with the current pipeline noted in the logs, we were able to find the
machines that were always involved in the write pipeline when things slowed down. This is easiest done by setting the -longSync option
appropriately when running the test and then tailing the test output and a grep for pipeline in the log file.

## DataReceiver/DataSender
These classes were used to do very basic network/disk tests. They basically mimic the HLog write pattern, but use java sockets and file APIs
directly to avoid the randomness of hdfs block placement. To run these tests, simply start the DataReceiver class on one machine and then 
DataSender class on another machine. The receiver can be optionally configured to write to the local filesystem (file:/) or hdfs (hdfs://).
This test allowed us to exactly pinpoint where the slowdown was occurring. In our case it was network -- we ran the test without writing to 
disk and noticed that transfers involving healthy nodes were consistently much faster than those involving the problematic nodes. If the 
problem had been disk I/O, the network only test would have been fine but the disk write would have been relatively much slower. 


To run the tests, simply package the project up (mvn assembly:assembly). Then copy the zip to the data nodes in question, unzip and run. 
See the source code for various options (number of writes, replication level, etc.). A sample logging config file is in 
conf/log4j.properties (include the conf dir in the classpath to use the configuration).
