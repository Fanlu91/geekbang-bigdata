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



