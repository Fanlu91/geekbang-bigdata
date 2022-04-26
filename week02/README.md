# 问题
统计每一个手机号耗费的总上行流量、下行流量、总流量。

数据准备： src/main/resources/HTTP_20130313143750.dat
输入数据格式：
时间戳、电话号码、基站的物理地址、访问网址的 ip、网站域名、数据包、接包数、上行 / 传流量、下行 / 载流量、响应码。

最终输出的数据格式：
手机号码 上行流量 下行流量 总流量

# 解答
首先根据hadoop文档查看如何编写mr作业

https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html



# 搭建环境

## 在本地使用docker

```sh
docker pull sequenceiq/hadoop-docker
docker run -p 50070:50070 -p 9000:9000 -p 8088:8088 -it sequenceiq/hadoop-docker /etc/bootstrap.sh -bash

docker container ls
docker cp HTTP_20130313143750.dat 23bf5440b3c4:/
```

```shell
# inside container
sh-4.1# cd $HADOOP_PREFIX
sh-4.1# pwd
/usr/local/hadoop


sh-4.1# bin/hadoop fs -copyFromLocal HTTP_20130313143750.dat /
sh-4.1# bin/hadoop fs -ls /
Found 2 items
-rw-r--r--   1 root supergroup       2229 2022-03-29 01:46 /HTTP_20130313143750.dat
drwxr-xr-x   - root supergroup          0 2015-07-22 11:17 /user
```



## 测试wordcount例子

编译并copy jar包

```java
docker cp geekbang_bigdata-1.0-SNAPSHOT.jar 23bf5440b3c4:/
  
bin/hadoop jar /geekbang_bigdata-1.0-SNAPSHOT.jar WordCount /HTTP_20130313143750.dat /output
```

开始遇到了java 版本的问题，此docker image默认的java版本是7，调整了pom来适应重新编译。

```less
sh-4.1# bin/hadoop fs -cat /output/part-r-00000
0	6
00-1F-64-E1-E6-9A:CMCC	1
00-FD-07-A2-EC-BA:CMCC	1
00-FD-07-A4-72-B8:CMCC	2
00-FD-07-A4-7B-08:CMCC	1
102	1
110349	1
```

可以看出wordCount例子正常运行。



# 编写自己的mr应用

```java

public class PhoneTrafficRunner {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, PhoneTraffic> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PhoneTraffic>.Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            String phone = fields[1];
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);
            context.write(new Text(phone), new PhoneTraffic(upFlow, downFlow));
        }
    }

    public static class MyReducer extends Reducer<Text, PhoneTraffic, Text, PhoneTraffic> {
        @Override
        protected void reduce(Text key, Iterable<PhoneTraffic> values, Reducer<Text, PhoneTraffic, Text, PhoneTraffic>.Context context) throws IOException, InterruptedException {
            long sumUp = 0, sumDown = 0;
            for (PhoneTraffic phoneTraffic : values) {
                sumUp += phoneTraffic.getUp();
                sumDown += phoneTraffic.getDown();
            }
            context.write(key, new PhoneTraffic(sumUp, sumDown));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PhoneTrafficRunner.class);
        job.setJobName("fanlu-countFlow");

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneTraffic.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneTraffic.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
```

执行

```less
bin/hadoop jar /geekbang_bigdata-1.0-SNAPSHOT.jar week02.PhoneTrafficRunner /HTTP_20130313143750.dat /output1

sh-4.1# bin/hadoop fs -cat /output1/part-r-00000
13480253104	week02.PhoneTraffic@1b54905
13502468823	week02.PhoneTraffic@24cce823
13560436666	week02.PhoneTraffic@89c64b5

```

重写toString()再次处理



```less
sh-4.1# bin/hadoop fs -rm -r /output1
bin/hadoop jar /geekbang_bigdata-1.0-SNAPSHOT.jar week02.PhoneTrafficRunner /HTTP_20130313143750.dat /output1

sh-4.1# bin/hadoop fs -cat /output1/part-r-00000
13480253104	PhoneTraffic{up=180, down=180, sum=360}
13502468823	PhoneTraffic{up=7335, down=110349, sum=117684}
13560436666	PhoneTraffic{up=1116, down=954, sum=2070}
13560439658	PhoneTraffic{up=2034, down=5892, sum=7926}
13602846565	PhoneTraffic{up=1938, down=2910, sum=4848}
13660577991	PhoneTraffic{up=6960, down=690, sum=7650}
13719199419	PhoneTraffic{up=240, down=0, sum=240}
13726230503	PhoneTraffic{up=2481, down=24681, sum=27162}
13726238888	PhoneTraffic{up=2481, down=24681, sum=27162}
13760778710	PhoneTraffic{up=120, down=120, sum=240}
13826544101	PhoneTraffic{up=264, down=0, sum=264}
13922314466	PhoneTraffic{up=3008, down=3720, sum=6728}
13925057413	PhoneTraffic{up=11058, down=48243, sum=59301}
13926251106	PhoneTraffic{up=240, down=0, sum=240}
13926435656	PhoneTraffic{up=132, down=1512, sum=1644}
15013685858	PhoneTraffic{up=3659, down=3538, sum=7197}
15920133257	PhoneTraffic{up=3156, down=2936, sum=6092}
15989002119	PhoneTraffic{up=1938, down=180, sum=2118}
18211575961	PhoneTraffic{up=1527, down=2106, sum=3633}
18320173382	PhoneTraffic{up=9531, down=2412, sum=11943}
84138413	PhoneTraffic{up=4116, down=1432, sum=5548}
```



# 上传github

```less
➜  geekbang_bigdata git init
➜  geekbang_bigdata git:(master) ✗ git remote add origin https://github.com/Fanlu91/geekbang-bigdata.git
➜  geekbang_bigdata git:(master) ✗ git pull
^C
➜  geekbang_bigdata git:(master) ✗ git branch -m master main                                            
➜  geekbang_bigdata git:(main) ✗ git pull
➜  geekbang_bigdata git:(main) ✗ git pull origin main  
➜  geekbang_bigdata git:(main) ✗ git add .

➜  geekbang_bigdata git:(main) ✗ git commit -m "initial commit"

➜  geekbang_bigdata git:(main) git push origin main

  

```

