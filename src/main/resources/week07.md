# 作业一

## 环境准备

下载提供的spark文件，但其源代码部分不全，放弃使用直接从github clone。



https://spark.apache.org/docs/latest/building-spark.html



### 运行 JavaSparkPi example

通过学习spark 提供的例子熟悉spark编程。



第一次run遇到如下问题

```less
- src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala:39:45
object SqlBaseParser is not a member of package org.apache.spark.sql.catalyst.parser
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
```

看起来像antlr的问题，根据课程指引单独执行 spark project catalyst antlr4 插件，并且增加jvm option 和 provided 依赖

再次执行，遇到了新的问题

```less
	at org.apache.spark.examples.JavaSparkPi.main(JavaSparkPi.java:37)
org.apache.spark.SparkException: Could not find spark-version-info.properties
```

寻找问题的解决方案

https://stackoverflow.com/questions/42751816/spark-version-info-properties-not-found-in-jenkins

通过在`./core/target/extra-resources` 增加spark-version-info.properties解决，成功运行。

#### Parallelized collections

注意到例子中使用了`parallelize`方法，学习其作用

Parallelized collections are created by calling `SparkContext`’s `parallelize` method on an existing collection in your driver program.The elements of the collection are copied to form a distributed dataset that can be operated on in parallel. For example, here is how to create a parallelized collection holding the numbers 1 to 5:

```java
List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
JavaRDD<Integer> distData = sc.parallelize(data);
```



Normally, Spark tries to set the number of partitions automatically based on your cluster. However, you can also set it manually by passing it as a second parameter to `parallelize` (e.g. `sc.parallelize(data, 10)`). Note: some places in the code use the term slices (a synonym for partitions) to maintain backward compatibility.



可以看出slices 用来手动设置并行度。



### 在作业工程增加spark 依赖

参照例子编写作业，首先搞定环境，根据课程提示，增加了spark-core 和 spark-hive 依赖

```xml
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-core_${scala.binary.version}</artifactId>
  <version>3.2.0</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-hive_${scala.binary.version}</artifactId>
  <version>3.2.0</version>
  <scope>provided</scope>
</dependency>
```



尝试在作业工程跑PI，第一次执行遇到一个奇怪的问题

```less
Exception in thread "main" java.lang.ArrayIndexOutOfBoundsException: 34826
	at com.thoughtworks.paranamer.BytecodeReadingParanamer$ClassReader.accept(BytecodeReadingParanamer.java:532)
```

看起来像是jdk8 和 paranamer组件有关的一个缺陷 https://github.com/paul-hammant/paranamer/issues/17

解决办法是升级组件，引入新的版本 https://stackoverflow.com/questions/53787624/spark-throwing-arrayindexoutofboundsexception-when-parallelizing-list

```xml
<dependency>
    <groupId>com.thoughtworks.paranamer</groupId>
    <artifactId>paranamer</artifactId>
    <version>2.8</version>
</dependency>
```

解决完上面问题后，可以在作业环境编写简单的spark程序了。



## 解题

### 要求

使用 RDD API 实现带词频的倒排索引
倒排索引（Inverted index），也被称为反向索引。它是文档检索系统中最常用的数据结构。被广泛地应用于全文搜索引擎。

例子如下，被索引的文件为（0，1，2 代表文件名）

```less
“it is what it is”
“what is it”
“it is a banana”
我们就能得到下面的反向文件索引：
“a”: {2}
“banana”: {2}
“is”: {0, 1, 2}
“it”: {0, 1, 2}
“what”: {0, 1}
再加上词频为：
“a”: {(2,1)}
“banana”: {(2,1)}
“is”: {(0,2), (1,1), (2,1)}
“it”: {(0,2), (1,1), (2,1)}
“what”: {(0,1), (1,1)}`
```



### 思路

输入为 1 2 3 三个文件，输出为倒排索引。

倒排索引中需要包含文件名，即 0 1 2，初步思考下来没有找到比较好的自动便利获取文件名及文件内容的方式，打算采取分别创建rdd最终union的方式实现。



### 代码

```java
public class InvertedIndex {

    public static class Stats implements Serializable {
        private final String word;
        private final List<Tuple2<String, Integer>> list;

        public Stats(String word, List<Tuple2<String, Integer>> list) {
            this.word = word;
            this.list = list;
        }

        public Stats merge(Stats other) {
            // 用Arrays.asList 得到的list是fix size的，需要新建一个list来存放结果
            List<Tuple2<String, Integer>> l = new ArrayList<>(list);
            l.addAll(other.list);
            return new Stats(word, l);
        }

        @Override
        public String toString() {
            return "\"" + word + "\" :" + list;
        }
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
//                .config("spark.master", "local")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
        JavaRDD<Stats> zero = jsc.textFile("src/main/resources/week07/0")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((i1, i2) -> i1 + i2)
                .map(tuple -> new Stats(tuple._1, Arrays.asList(new Tuple2<String, Integer>("0", tuple._2))));
        JavaRDD<Stats> one = jsc.textFile("src/main/resources/week07/1")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((i1, i2) -> i1 + i2)
                .map(tuple -> new Stats(tuple._1, Arrays.asList(new Tuple2<String, Integer>("1", tuple._2))));
        JavaRDD<Stats> two = jsc.textFile("src/main/resources/week07/2")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((i1, i2) -> i1 + i2)
                .map(tuple -> new Stats(tuple._1, Arrays.asList(new Tuple2<String, Integer>("2", tuple._2))));

        List<Tuple2<String, Stats>> collect = zero.union(one).union(two)
                .keyBy(stats -> stats.word)
                .reduceByKey(Stats::merge).collect();

        for (Tuple2<String, Stats> t : collect)
            System.out.println(t._2);

        spark.stop();
    }
}
```

运行结果

```less
"is" :[(0,2), (1,1), (2,1)]
"a" :[(2,1)]
"what" :[(0,1), (1,1)]
"banana" :[(2,1)]
"it" :[(0,2), (1,1), (2,1)]
```

上面的Stats似乎并不需要定义，使用pairRdd就能完成任务，存在优化空间。





# 作业二

Distcp 的 Spark 实现
使用 Spark 实现 Hadoop 分布式数据传输工具 DistCp (distributed copy)，只要求实现最基础的 copy 功能，对于 -update、-diff、-p 不做要求。

对于 HadoopDistCp 的功能与实现，可以参考：
https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html

https://github.com/apache/hadoop/tree/release-2.7.1/hadoop-tools/hadoop-distcp

Hadoop 使用 MapReduce 框架来实现分布式 copy，在 Spark 中应使用 RDD 来实现分布式 copy，应实现的功能为：

sparkDistCp hdfs://xxx/source hdfs://xxx/target
得到的结果为：启动多个 task/executor，将 hdfs://xxx/source 目录复制到 hdfs://xxx/target，得到 hdfs://xxx/target/source
需要支持 source 下存在多级子目录
需支持 -i Ignore failures 参数
需支持 -m max concurrence 参数，控制同时 copy 的最大并发 task 数



## 思路

这道题目的难度显著大了一些，有几个地方需要学习和实现

- 参数识别和传递
- spark 递归读/写 hdfs目录及文件
- map并行度手工控制



## 环境准备

在本地启动 hdfs docker 环境，创建source文件夹及文件

```shell
bash-4.1# cd $HADOOP_PREFIX
bash-4.1# bin/hadoop fs -mkdir -p /sparkdata/dir1
bash-4.1# bin/hadoop fs -mkdir -p /sparkdata/dir2

bash-4.1# echo "spark distcp" > 0.txt
bash-4.1# cp 0.txt 1.txt
bash-4.1# bin/hadoop fs -copyFromLocal 0.txt /sparkdata/
bash-4.1# bin/hadoop fs -copyFromLocal 1.txt /sparkdata/dir1/
bash-4.1# bin/hadoop fs -ls -R /sparkdata
-rw-r--r--   1 root supergroup         13 2022-04-19 20:56 /sparkdata/0.txt
drwxr-xr-x   - root supergroup          0 2022-04-19 20:57 /sparkdata/dir1
-rw-r--r--   1 root supergroup         13 2022-04-19 20:57 /sparkdata/dir1/1.txt
```



## 解答

### spark java api 访问hdfs

尝试通过如下代码读取 hdfs的文件内容

```java
SparkContext context = new SparkContext(new SparkConf().setAppName("sparkDistCp").setMaster("local[*]")
                .set("spark.hadoop.fs.default.name", "hdfs://localhost:50070").set("spark.hadoop.fs.defaultFS", "hdfs://localhost:50070")
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));
        SparkSession session = SparkSession.builder().sparkContext(context).getOrCreate();
        
        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        FileSystem fileSystem = FileSystem.get(jsc.hadoopConfiguration()); // 42
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(new Path("hdfs://localhost:50070/sparkdata"), true); 

        while (it.hasNext()) {
            LocatedFileStatus lfs = it.next();
            System.out.println(lfs.getPath());
            System.out.println(lfs.getPath().getParent());
            System.out.println(lfs.getPath().getName());
            System.out.println(lfs.getPath().depth());
        }
```

#### 排错 hdfs依赖版本

执行遇到如下错误

```less
Exception in thread "main" java.lang.VerifyError: Bad return type
Exception Details:
  Location:
    org/apache/hadoop/hdfs/DFSClient.getQuotaUsage(Ljava/lang/String;)Lorg/apache/hadoop/fs/QuotaUsage; @160: areturn
  Reason:
    Type 'org/apache/hadoop/fs/ContentSummary' (current frame, stack[0]) is not assignable to 'org/apache/hadoop/fs/QuotaUsage' (from method signature)
  Current Frame:
    bci: @160
    flags: { }
    locals: { 'org/apache/hadoop/hdfs/DFSClient', 'java/lang/String', 'org/apache/hadoop/ipc/RemoteException', 'java/io/IOException' }
    stack: { 'org/apache/hadoop/fs/ContentSummary' }
  Bytecode:
    0x0000000: 2ab6 0316 2a13 07ac 2bb6 031b 4d01 4e2a
    0x0000010: b401 8a2b b907 ae02 003a 042c c600 1d2d
    0x0000020: c600 152c b603 21a7 0012 3a05 2d19 05b6
    0x0000030: 0325 a700 072c b603 2119 04b0 3a04 1904
    0x0000040: 4e19 04bf 3a06 2cc6 001d 2dc6 0015 2cb6
    0x0000050: 0321 a700 123a 072d 1907 b603 25a7 0007
    0x0000060: 2cb6 0321 1906 bf4d 2c07 bd03 7e59 0313
    0x0000070: 0380 5359 0413 03b0 5359 0513 03b2 5359
    0x0000080: 0613 07b2 53b6 0384 4e2d c107 b299 0014
    0x0000090: b201 2713 07b4 b901 4102 002a 2bb6 07b5
    0x00000a0: b02d bf                                
  Exception Handler Table:
    bci [35, 39] => handler: 42
    bci [15, 27] => handler: 60
    bci [15, 27] => handler: 68
    bci [78, 82] => handler: 85
    bci [60, 70] => handler: 68
    bci [4, 57] => handler: 103
    bci [60, 103] => handler: 103
  Stackmap Table:
    full_frame(@42,{Object[#2],Object[#575],Object[#800],Object[#677],Object[#1968]},{Object[#677]})
    same_frame(@53)
    same_frame(@57)
    full_frame(@60,{Object[#2],Object[#575],Object[#800],Object[#677]},{Object[#677]})
    same_locals_1_stack_item_frame(@68,Object[#677])
    full_frame(@85,{Object[#2],Object[#575],Object[#800],Object[#677],Top,Top,Object[#677]},{Object[#677]})
    same_frame(@96)
    same_frame(@100)
    full_frame(@103,{Object[#2],Object[#575]},{Object[#880]})
    append_frame(@161,Object[#880],Object[#205])

	at org.apache.hadoop.hdfs.DistributedFileSystem.initDFSClient(DistributedFileSystem.java:201)
	at org.apache.hadoop.hdfs.DistributedFileSystem.initialize(DistributedFileSystem.java:186)
	at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:2653)
	at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:92)
	at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:2687)
	at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:2669)
	at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:371)
	at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:170)
	at week07.sparkDistCp.main(sparkDistCp.java:37)
```

判断可能是`spark.hadoop.fs.hdfs.` 版本与hadoop版本不匹配导致。原本根据docker上的hadoop版本，引入了2.7.0的hdfs依赖，*ContentSummary*这个类是从hadoop 2.8以后发生了变化。

将hdfs版本改为3.3.2，此问题没有再出现。



#### 排错 netty依赖版本

不过执行之后又遇到了新的问题，在定义SparkContext的位置报了netty的问题：

```less
Exception in thread "main" java.lang.NoSuchMethodError: io.netty.buffer.PooledByteBufAllocator.<init>(ZIIIIIIZ)V
```

在网上搜索，大多提到可能是引入不同版本的netty导致的问题，需要处理netty依赖，通过`mvn dependency:tree`分析解决了问题。



#### 排错 端口用错

再次尝试运行，遇到了又一个问题

```less
Exception in thread "main" org.apache.hadoop.ipc.RpcException: RPC response exceeds maximum data length
	at org.apache.hadoop.ipc.Client$IpcStreams.readResponse(Client.java:1906)
	at org.apache.hadoop.ipc.Client$Connection.receiveRpcResponse(Client.java:1202)
	at org.apache.hadoop.ipc.Client$Connection.run(Client.java:1098)
```

发现是使用错了端口，将50070 改为 **9000**成功拿到了数据。



这样我们就通过spark获取到了source 文件夹的信息 uri、parent dir、file name、depth

```less
hdfs://localhost:9000/sparkdata/0.txt
hdfs://localhost:9000/sparkdata
0.txt
2
hdfs://localhost:9000/sparkdata/dir1/1.txt
hdfs://localhost:9000/sparkdata/dir1
1.txt
3
2022-04-20 14:44:17 INFO  AbstractConnector:381 - Stopped Spark@290b1b2e{HTTP/1.1, (http/1.1)}{0.0.0.0:4040}
```



### 实现copy



前面已经拿到了`LocatedFileStatus lfs = it.next();`根据获得的path信息，利用hadoop.FileUtil尝试copy操作

```java
     while (it.hasNext()) {
            LocatedFileStatus lfs = it.next();
            System.out.println(lfs.getPath());
            System.out.println(lfs.getPath().getParent());
            System.out.println(lfs.getPath().getName());
            System.out.println(lfs.getPath().depth());
            // fileSystem.mkdirs(new Path("hdfs://localhost:9000/" + target + "/"), new FsPermission("777"));
            FileUtil.copy(fileSystem, lfs.getPath(),
                     fileSystem, new Path("hdfs://localhost:9000/" + target + "/" + lfs.getPath().getName()),
                     false, false, jsc.hadoopConfiguration());
```

这里/target使用了已经创建好的路径，并且修改了用户组，否则会出现访问权限的问题，暂时不去解决。



执行之后，发现运行到中间会卡住（第一次执行到FileUtil.copy后），遇到了`hadoop.net.ConnectTimeoutException`

```less
2022-04-20 20:23:53 INFO  ContextHandler:915 - Started o.s.j.s.ServletContextHandler@57bd2029{/metrics/json,null,AVAILABLE,@Spark}
hdfs://localhost:9000/sparkdata/0.txt
hdfs://localhost:9000/sparkdata
0.txt
2
2022-04-20 20:24:53 WARN  BlockReaderFactory:769 - I/O error constructing remote block reader.
org.apache.hadoop.net.ConnectTimeoutException: 60000 millis timeout while waiting for channel to be ready for connect. ch : java.nio.channels.SocketChannel[connection-pending remote=/172.17.0.2:50010]
```

发现target下已经创建了文件，但是文件内容为空，报错信息看似乎取不到block

```less
bash-4.1# bin/hadoop fs -ls /target
Found 1 items
-rw-r--r--   3 fanlu fanlu          0 2022-04-20 08:23 /target/0.txt
bash-4.1# bin/hadoop fs -cat /target/0.txt
bash-4.1#
```



docker 容器内的Datanode 服务没有看出明显问题

```less
bash-4.1# bin/hadoop fs -cp /sparkdata/0.txt /test.txt
22/04/20 08:33:39 WARN hdfs.DFSClient: DFSInputStream has been closed already
bash-4.1# bin/hadoop fs -cat /test.txt
spark distcp


bash-4.1# ifconfig
eth0      Link encap:Ethernet  HWaddr 02:42:AC:11:00:02
          inet addr:172.17.0.2  Bcast:172.17.255.255  Mask:255.255.0.0

bash-4.1# netstat -anp | grep 50010
tcp        0      0 0.0.0.0:50010               0.0.0.0:*                   LISTEN      254/java
bash-4.1# ps -ef | grep 254
root       254     1  0 01:38 ?        00:02:45 /usr/java/default/bin/java -Dproc_datanode -Xmx1000m -Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=/usr/local/hadoop/logs -Dhadoop.log.file=hadoop.log -Dhadoop.home.dir= -Dhadoop.id.str=root -Dhadoop.root.logger=INFO,console -Djava.library.path= -Dhadoop.policy.file=hadoop-policy.xml -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=/usr/local/hadoop/logs -Dhadoop.log.file=hadoop-root-datanode-27c12f12e43b.log -Dhadoop.home.dir=/usr/local/hadoop -Dhadoop.id.str=root -Dhadoop.root.logger=INFO,RFA -Djava.library.path=/usr/local/hadoop/lib/native -Dhadoop.policy.file=hadoop-policy.xml -Djava.net.preferIPv4Stack=true -server -Dhadoop.security.logger=ERROR,RFAS -Dhadoop.security.logger=ERROR,RFAS -Dhadoop.security.logger=ERROR,RFAS -Dhadoop.security.logger=INFO,RFAS org.apache.hadoop.hdfs.server.datanode.DataNode
```



```less
2022-04-20 20:15:31 INFO  ContextHandler:915 - Started o.s.j.s.ServletContextHandler@60f2e0bd{/metrics/json,null,AVAILABLE,@Spark}
hdfs://localhost:9000/sparkdata/0.txt
hdfs://localhost:9000/sparkdata
0.txt
2
2022-04-20 20:16:31 WARN  BlockReaderFactory:769 - I/O error constructing remote block reader.
org.apache.hadoop.net.ConnectTimeoutException: 60000 millis timeout while waiting for channel to be ready for connect. ch : java.nio.channels.SocketChannel[connection-pending remote=/172.17.0.2:50010]
	at org.apache.hadoop.net.NetUtils.connect(NetUtils.java:589)
	at org.apache.hadoop.hdfs.DFSClient.newConnectedPeer(DFSClient.java:3025)
	at org.apache.hadoop.hdfs.client.impl.BlockReaderFactory.nextTcpPeer(BlockReaderFactory.java:826)
	at org.apache.hadoop.hdfs.client.impl.BlockReaderFactory.getRemoteBlockReaderFromTcp(BlockReaderFactory.java:751)
	at org.apache.hadoop.hdfs.client.impl.BlockReaderFactory.build(BlockReaderFactory.java:381)
	at org.apache.hadoop.hdfs.DFSInputStream.getBlockReader(DFSInputStream.java:755)
	at org.apache.hadoop.hdfs.DFSInputStream.blockSeekTo(DFSInputStream.java:685)
	at org.apache.hadoop.hdfs.DFSInputStream.readWithStrategy(DFSInputStream.java:884)
	at org.apache.hadoop.hdfs.DFSInputStream.read(DFSInputStream.java:957)
	at java.io.DataInputStream.read(DataInputStream.java:100)
	at org.apache.hadoop.io.IOUtils.copyBytes(IOUtils.java:94)
	at org.apache.hadoop.io.IOUtils.copyBytes(IOUtils.java:68)
	at org.apache.hadoop.io.IOUtils.copyBytes(IOUtils.java:129)
	at org.apache.hadoop.fs.FileUtil.copy(FileUtil.java:418)
	at org.apache.hadoop.fs.FileUtil.copy(FileUtil.java:390)
	at week07.sparkDistCp.main(sparkDistCp.java:51)
2022-04-20 20:16:31 WARN  DFSClient:707 - Failed to connect to /172.17.0.2:50010 for file /sparkdata/0.txt for block BP-581371184-172.17.13.14-1437578119536:blk_1073741861_1037, add to deadNodes and continue. 
org.apache.hadoop.net.ConnectTimeoutException: 60000 millis timeout while waiting for channel to be ready for connect. ch : java.nio.channels.SocketChannel[connection-pending remote=/172.17.0.2:50010]
	at org.apache.hadoop.net.NetUtils.connect(NetUtils.java:589)
	at org.apache.hadoop.hdfs.DFSClient.newConnectedPeer(DFSClient.java:3025)
	at org.apache.hadoop.hdfs.client.impl.BlockReaderFactory.nextTcpPeer(BlockReaderFactory.java:826)
	at org.apache.hadoop.hdfs.client.impl.BlockReaderFactory.getRemoteBlockReaderFromTcp(BlockReaderFactory.java:751)
	at org.apache.hadoop.hdfs.client.impl.BlockReaderFactory.build(BlockReaderFactory.java:381)
	at org.apache.hadoop.hdfs.DFSInputStream.getBlockReader(DFSInputStream.java:755)
	at org.apache.hadoop.hdfs.DFSInputStream.blockSeekTo(DFSInputStream.java:685)
	at org.apache.hadoop.hdfs.DFSInputStream.readWithStrategy(DFSInputStream.java:884)
	at org.apache.hadoop.hdfs.DFSInputStream.read(DFSInputStream.java:957)
	at java.io.DataInputStream.read(DataInputStream.java:100)
	at org.apache.hadoop.io.IOUtils.copyBytes(IOUtils.java:94)
	at org.apache.hadoop.io.IOUtils.copyBytes(IOUtils.java:68)
	at org.apache.hadoop.io.IOUtils.copyBytes(IOUtils.java:129)
	at org.apache.hadoop.fs.FileUtil.copy(FileUtil.java:418)
	at org.apache.hadoop.fs.FileUtil.copy(FileUtil.java:390)
	at week07.sparkDistCp.main(sparkDistCp.java:51)
2022-04-20 20:16:31 WARN  DFSClient:1104 - No live nodes contain block BP-581371184-172.17.13.14-1437578119536:blk_1073741861_1037 after checking nodes = [DatanodeInfoWithStorage[172.17.0.2:50010,DS-a38d90cd-97c9-499b-914f-c710b2f0574a,DISK]], ignoredNodes = null
2022-04-20 20:16:31 INFO  DFSClient:1014 - Could not obtain BP-581371184-172.17.13.14-1437578119536:blk_1073741861_1037 from any node:  No live nodes contain current block Block locations: DatanodeInfoWithStorage[172.17.0.2:50010,DS-a38d90cd-97c9-499b-914f-c710b2f0574a,DISK] Dead nodes:  DatanodeInfoWithStorage[172.17.0.2:50010,DS-a38d90cd-97c9-499b-914f-c710b2f0574a,DISK]. Will get new block locations from namenode and retry...
2022-04-20 20:16:31 WARN  DFSClient:1033 - DFS chooseDataNode: got # 1 IOException, will wait for 1396.6303117231496 msec.
```

暂时没有解决思路。
