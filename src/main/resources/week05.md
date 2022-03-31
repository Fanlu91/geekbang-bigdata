# 作业



# hive docker 环境

```less
docker pull nagasuga/docker-hive

docker run -i -t nagasuga/docker-hive /bin/bash -c 'cd /usr/local/hive && ./bin/hive'
```

https://hub.docker.com/r/nagasuga/docker-hive





# hive cli 练习

## 数据

```less
t_movie 电影表（共 3000+ 条数据）
字段为：MovieID, MovieName, MovieType
字段中文解释：电影 ID，电影名，电影类型
[student3@emr-header-1 hive]$ head movies.dat
1::Toy Story (1995)::Animation|Children's|Comedy
2::Jumanji (1995)::Adventure|Children's|Fantasy
3::Grumpier Old Men (1995)::Comedy|Romance
4::Waiting to Exhale (1995)::Comedy|Drama
5::Father of the Bride Part II (1995)::Comedy
6::Heat (1995)::Action|Crime|Thriller
7::Sabrina (1995)::Comedy|Romance
8::Tom and Huck (1995)::Adventure|Children's
9::Sudden Death (1995)::Action
10::GoldenEye (1995)::Action|Adventure|Thriller

t_user 观众表（6000+ 条数据）
字段为：UserID, Sex, Age, Occupation, Zipcode
字段中文解释：用户 id，性别，年龄，职业，邮编
[student3@emr-header-1 hive]$ head users.dat
1::F::1::10::48067
2::M::56::16::70072
3::M::25::15::55117
4::M::45::7::02460
5::M::25::20::55455
6::F::50::9::55117
7::M::35::1::06810
8::M::25::12::11413
9::M::25::17::61614
10::F::35::1::95370


t_rating 影评表（100 万 + 条数据）
字段为：UserID, MovieID, Rate, Times
字段中文解释：用户 ID，电影 ID，评分，评分时间
[student3@emr-header-1 hive]$ head ratings.dat
1::1193::5::978300760
1::661::3::978302109
1::914::3::978301968
1::3408::4::978300275
1::2355::5::978824291
1::1197::3::978302268
1::1287::5::978302039
1::2804::5::978300719
1::594::4::978302268
1::919::4::978301368
```



## 创建表

注意这里采用的是多分隔符`::`，

```sql
CREATE EXTERNAL TABLE `t_movie`(
    `MovieID` varchar(8) COMMENT '电影 ID' ,
    `MovieName` varchar(32) COMMENT '电影名称' ,
    `MovieType` varchar(32) COMMENT '电影类型' 
)COMMENT '电影表'
row format delimited fields terminated by "::"
STORED AS TEXTFILE
LOCATION '/fanlu/hivedata/movies.dat'

CREATE EXTERNAL TABLE `t_user`(
    `UserID` varchar(8),
    `Sex` char(1) ,
    `Age` varchar(4), 
    `Occupation` varchar(8), 
    `Zipcode` varchar(8) 
)
row format delimited fields terminated by "::"
STORED AS TEXTFILE
LOCATION '/fanlu/hivedata/t_user';

CREATE EXTERNAL TABLE `t_rating`(
    `UserID` varchar(8),
    `MovieID` varchar(8),
    `Rate` TINYINT,
    `Times` string
)
row format delimited fields terminated by "::"
STORED AS TEXTFILE
LOCATION '/fanlu/hivedata/t_rating';



load data local inpath 'movies.dat' into table fanlu.t_movie;
```

发现两个问题

- 中文乱码，这部分应该需要修改hive site配置解决，由于使用的是实验环境，暂不调整，规避此问题。

- 另外一个问题是，load数据后发现列匹配存在问题，排查发现分隔符"::" 需要使用`MultiDelimitSerDe`做序列化，上面的默认方式使用的是`org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`。



改用managed table降低修改时的复杂度

```sql

CREATE TABLE `t_movie`(
    `MovieID` varchar(8),
    `MovieName` varchar(32) ,
    `MovieType` varchar(32) 
)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
STORED AS TEXTFILE;

CREATE TABLE `t_user`(
    `UserID` varchar(8),
    `Sex` char(1) ,
    `Age` varchar(4), 
    `Occupation` varchar(8), 
    `Zipcode` varchar(8) 
)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
STORED AS TEXTFILE;


CREATE TABLE `t_rating`(
    `UserID` varchar(8),
    `MovieID` varchar(8),
    `Rate` TINYINT,
    `Times` VARCHAR(32)
)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
STORED AS TEXTFILE;



load data local inpath 'movies.dat' into table fanlu.t_movie;
load data local inpath 'users.dat' into table fanlu.t_user;
load data local inpath 'ratings.dat' into table fanlu.t_rating;


```

注意，hive会把所有的大些字母转换为小写。



## 题目一（简单）

展示电影 ID 为 2116 这部电影各年龄段的平均影评分。

### 解答

```sql
explain CBO select u.age,avg(r.rate)
from t_user as u, t_rating as r
where u.userid = r.userid and movieid=2116
group by u.age;


25	3.436548223350254
45	2.8275862068965516
1	3.2941176470588234
18	3.3580246913580245
35	3.2278481012658227
50	3.32
56	3.5
Time taken: 10.135 seconds, Fetched: 7 row(s)

```

### 尝试explain分析

参考 https://cloud.tencent.com/developer/article/1789749

https://www.docs4dev.com/docs/zh/apache-hive/3.1.1/reference/LanguageManual_Explain.html

```less
EXPLAIN [EXTENDED|CBO|AST|DEPENDENCY|AUTHORIZATION|LOCKS|VECTORIZATION|ANALYZE] query

AST 目前因为缺陷被删除
```



执行计划信息分两部分

- stage dependencies： 各个stage之间的依赖性
- stage plan： 各个stage的执行计划



实际看到的执行计划结构与参考的教程有所不同，比如教程中Operator主要分为

- Map Operator Tree
- Reduce Operator Tree

但是如下执行计划包含的Operator更细，猜测可能是因为使用了TEZ引擎因此与MR有所不同？后续再研究。

```sql
hive> explain select u.age,avg(r.rate)
    > from t_user as u, t_rating as r
    > where u.userid = r.userid and movieid=2116
    > group by u.age;
OK
Plan optimized by CBO. ## 这里看出是 Cost Based Optimization

Vertex dependency in root stage
Map 2 <- Map 1 (BROADCAST_EDGE)
Reducer 3 <- Map 2 (SIMPLE_EDGE)

Stage-0
  Fetch Operator
    limit:-1
    Stage-1
      Reducer 3
      File Output Operator [FS_14]
        Select Operator [SEL_13] (rows=1 width=1478048)
          Output:["_col0","_col1"]
          Group By Operator [GBY_12] (rows=1 width=1478048)
            Output:["_col0","_col1","_col2"],aggregations:["sum(VALUE._col0)","count(VALUE._col1)"],keys:KEY._col0
          <-Map 2 [SIMPLE_EDGE]
            SHUFFLE [RS_11]
              PartitionCols:_col0
              Group By Operator [GBY_10] (rows=1 width=1478048)
                Output:["_col0","_col1","_col2"],aggregations:["sum(_col4)","count(_col4)"],keys:_col1
                Map Join Operator [MAPJOIN_19] (rows=1 width=1478048)
                  Conds:RS_6._col0=SEL_5._col0(Inner),Output:["_col1","_col4"]
                <-Map 1 [BROADCAST_EDGE]
                  BROADCAST [RS_6]
                    PartitionCols:_col0
                    Select Operator [SEL_2] (rows=1 width=1343680)
                      Output:["_col0","_col1"]
                      Filter Operator [FIL_17] (rows=1 width=1343680)
                        predicate:userid is not null
                        TableScan [TS_0] (rows=1 width=1343680)
                          fanlu@t_user,u,Tbl:COMPLETE,Col:NONE,Output:["userid","age"]
                <-Select Operator [SEL_5] (rows=1 width=245941312)
                    Output:["_col0","_col2"]
                    Filter Operator [FIL_18] (rows=1 width=245941312)
                      predicate:((UDFToDouble(movieid) = 2116.0D) and userid is not null)
                      TableScan [TS_3] (rows=1 width=245941312)
                        fanlu@t_rating,r,Tbl:COMPLETE,Col:NONE,Output:["userid","movieid","rate"]

Time taken: 0.153 seconds, Fetched: 39 row(s)
```



### explain extended

使用extended会显著提供更多信息

```sql
hive> explain extended select u.age,avg(r.rate)
    > from t_user as u, t_rating as r
    > where u.userid = r.userid and movieid=2116
    > group by u.age;
OK
STAGE DEPENDENCIES:
  Stage-1 is a root stage
  Stage-0 depends on stages: Stage-1

STAGE PLANS:
  Stage: Stage-1
    Tez
      DagId: student3_20220331113438_94cd8e81-7a5f-4861-ad7d-983a8252f19e:4
      Edges:
        Map 2 <- Map 1 (BROADCAST_EDGE)
        Reducer 3 <- Map 2 (SIMPLE_EDGE)
      DagName: student3_20220331113438_94cd8e81-7a5f-4861-ad7d-983a8252f19e:4
      Vertices:
        Map 1
            Map Operator Tree:
                TableScan
                  alias: u
                  Statistics: Num rows: 1 Data size: 1343680 Basic stats: COMPLETE Column stats: NONE
                  GatherStats: false
                  Filter Operator
                    isSamplingPred: false
                    predicate: userid is not null (type: boolean)
                    Statistics: Num rows: 1 Data size: 1343680 Basic stats: COMPLETE Column stats: NONE
                    Select Operator
                      expressions: userid (type: varchar(8)), age (type: varchar(4))
                      outputColumnNames: _col0, _col1
                      Statistics: Num rows: 1 Data size: 1343680 Basic stats: COMPLETE Column stats: NONE
                      Reduce Output Operator
                        key expressions: _col0 (type: varchar(8))
                        null sort order: a
                        sort order: +
                        Map-reduce partition columns: _col0 (type: varchar(8))
                        Statistics: Num rows: 1 Data size: 1343680 Basic stats: COMPLETE Column stats: NONE
                        tag: 0
                        value expressions: _col1 (type: varchar(4))
                        auto parallelism: true
            Path -> Alias:
              hdfs://emr-header-1.cluster-285604:9000/user/hive/warehouse/fanlu.db/t_user [u]
            Path -> Partition:
              hdfs://emr-header-1.cluster-285604:9000/user/hive/warehouse/fanlu.db/t_user
                Partition
                  base file name: t_user
                  input format: org.apache.hadoop.mapred.TextInputFormat
                  output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                  properties:
                    bucket_count -1
                    bucketing_version 2
                    column.name.delimiter ,
                    columns userid,sex,age,occupation,zipcode
                    columns.comments
                    columns.types varchar(8):char(1):varchar(4):varchar(8):varchar(8)
                    field.delim ::
                    file.inputformat org.apache.hadoop.mapred.TextInputFormat
                    file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                    location hdfs://emr-header-1.cluster-285604:9000/user/hive/warehouse/fanlu.db/t_user
                    name fanlu.t_user
                    numFiles 1
                    numRows 0
                    rawDataSize 0
                    serialization.ddl struct t_user { varchar(8) userid, char(1) sex, varchar(4) age, varchar(8) occupation, varchar(8) zipcode}
                    serialization.format 1
                    serialization.lib org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe
                    totalSize 134368
                    transient_lastDdlTime 1648696826
                  serde: org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe

                    input format: org.apache.hadoop.mapred.TextInputFormat
                    output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                    properties:
                      bucket_count -1
                      bucketing_version 2
                      column.name.delimiter ,
                      columns userid,sex,age,occupation,zipcode
                      columns.comments
                      columns.types varchar(8):char(1):varchar(4):varchar(8):varchar(8)
                      field.delim ::
                      file.inputformat org.apache.hadoop.mapred.TextInputFormat
                      file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                      location hdfs://emr-header-1.cluster-285604:9000/user/hive/warehouse/fanlu.db/t_user
                      name fanlu.t_user
                      numFiles 1
                      numRows 0
                      rawDataSize 0
                      serialization.ddl struct t_user { varchar(8) userid, char(1) sex, varchar(4) age, varchar(8) occupation, varchar(8) zipcode}
                      serialization.format 1
                      serialization.lib org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe
                      totalSize 134368
                      transient_lastDdlTime 1648696826
                    serde: org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe
                    name: fanlu.t_user
                  name: fanlu.t_user
            Truncated Path -> Alias:
              /fanlu.db/t_user [u]
        Map 2
            Map Operator Tree:
                TableScan
                  alias: r
                  Statistics: Num rows: 1 Data size: 245941312 Basic stats: COMPLETE Column stats: NONE
                  GatherStats: false
                  Filter Operator
                    isSamplingPred: false
                    predicate: ((UDFToDouble(movieid) = 2116.0D) and userid is not null) (type: boolean)
                    Statistics: Num rows: 1 Data size: 245941312 Basic stats: COMPLETE Column stats: NONE
                    Select Operator
                      expressions: userid (type: varchar(8)), rate (type: tinyint)
                      outputColumnNames: _col0, _col2
                      Statistics: Num rows: 1 Data size: 245941312 Basic stats: COMPLETE Column stats: NONE
                      Map Join Operator
                        condition map:
                             Inner Join 0 to 1
                        Estimated key counts: Map 1 => 1
                        keys:
                          0 _col0 (type: varchar(8))
                          1 _col0 (type: varchar(8))
                        outputColumnNames: _col1, _col4
                        input vertices:
                          0 Map 1
                        Position of Big Table: 1
                        Statistics: Num rows: 1 Data size: 1478048 Basic stats: COMPLETE Column stats: NONE
                        Group By Operator
                          aggregations: sum(_col4), count(_col4)
                          keys: _col1 (type: varchar(4))
                          mode: hash
                          outputColumnNames: _col0, _col1, _col2
                          Statistics: Num rows: 1 Data size: 1478048 Basic stats: COMPLETE Column stats: NONE
                          Reduce Output Operator
                            key expressions: _col0 (type: varchar(4))
                            null sort order: a
                            sort order: +
                            Map-reduce partition columns: _col0 (type: varchar(4))
                            Statistics: Num rows: 1 Data size: 1478048 Basic stats: COMPLETE Column stats: NONE
                            tag: -1
                            value expressions: _col1 (type: bigint), _col2 (type: bigint)
                            auto parallelism: true
            Path -> Alias:
              hdfs://emr-header-1.cluster-285604:9000/user/hive/warehouse/fanlu.db/t_rating [r]
            Path -> Partition:
              hdfs://emr-header-1.cluster-285604:9000/user/hive/warehouse/fanlu.db/t_rating
                Partition
                  base file name: t_rating
                  input format: org.apache.hadoop.mapred.TextInputFormat
                  output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                  properties:
                    bucket_count -1
                    bucketing_version 2
                    column.name.delimiter ,
                    columns userid,movieid,rate,times
                    columns.comments
                    columns.types varchar(8):varchar(8):tinyint:varchar(32)
                    field.delim ::
                    file.inputformat org.apache.hadoop.mapred.TextInputFormat
                    file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                    location hdfs://emr-header-1.cluster-285604:9000/user/hive/warehouse/fanlu.db/t_rating
                    name fanlu.t_rating
                    numFiles 1
                    numRows 0
                    rawDataSize 0
                    serialization.ddl struct t_rating { varchar(8) userid, varchar(8) movieid, byte rate, varchar(32) times}
                    serialization.format 1
                    serialization.lib org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe
                    totalSize 24594131
                    transient_lastDdlTime 1648697212
                  serde: org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe

                    input format: org.apache.hadoop.mapred.TextInputFormat
                    output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                    properties:
                      bucket_count -1
                      bucketing_version 2
                      column.name.delimiter ,
                      columns userid,movieid,rate,times
                      columns.comments
                      columns.types varchar(8):varchar(8):tinyint:varchar(32)
                      field.delim ::
                      file.inputformat org.apache.hadoop.mapred.TextInputFormat
                      file.outputformat org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                      location hdfs://emr-header-1.cluster-285604:9000/user/hive/warehouse/fanlu.db/t_rating
                      name fanlu.t_rating
                      numFiles 1
                      numRows 0
                      rawDataSize 0
                      serialization.ddl struct t_rating { varchar(8) userid, varchar(8) movieid, byte rate, varchar(32) times}
                      serialization.format 1
                      serialization.lib org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe
                      totalSize 24594131
                      transient_lastDdlTime 1648697212
                    serde: org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe
                    name: fanlu.t_rating
                  name: fanlu.t_rating
            Truncated Path -> Alias:
              /fanlu.db/t_rating [r]
        Reducer 3
            Needs Tagging: false
            Reduce Operator Tree:
              Group By Operator
                aggregations: sum(VALUE._col0), count(VALUE._col1)
                keys: KEY._col0 (type: varchar(4))
                mode: mergepartial
                outputColumnNames: _col0, _col1, _col2
                Statistics: Num rows: 1 Data size: 1478048 Basic stats: COMPLETE Column stats: NONE
                Select Operator
                  expressions: _col0 (type: varchar(4)), (_col1 / _col2) (type: double)
                  outputColumnNames: _col0, _col1
                  Statistics: Num rows: 1 Data size: 1478048 Basic stats: COMPLETE Column stats: NONE
                  File Output Operator
                    compressed: false
                    GlobalTableId: 0
                    directory: hdfs://emr-header-1.cluster-285604:9000/tmp/hive/student3/877977e0-4d69-496c-9072-58801075004d/hive_2022-03-31_11-34-38_329_56631292980102596-1/-mr-10001/.hive-staging_hive_2022-03-31_11-34-38_329_56631292980102596-1/-ext-10002
                    NumFilesPerFileSink: 1
                    Statistics: Num rows: 1 Data size: 1478048 Basic stats: COMPLETE Column stats: NONE
                    Stats Publishing Key Prefix: hdfs://emr-header-1.cluster-285604:9000/tmp/hive/student3/877977e0-4d69-496c-9072-58801075004d/hive_2022-03-31_11-34-38_329_56631292980102596-1/-mr-10001/.hive-staging_hive_2022-03-31_11-34-38_329_56631292980102596-1/-ext-10002/
                    table:
                        input format: org.apache.hadoop.mapred.SequenceFileInputFormat
                        output format: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat
                        properties:
                          columns _col0,_col1
                          columns.types varchar(4):double
                          escape.delim \
                          hive.serialization.extend.additional.nesting.levels true
                          serialization.escape.crlf true
                          serialization.format 1
                          serialization.lib org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                        serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
                    TotalFiles: 1
                    GatherStats: false
                    MultiFileSpray: false

  Stage: Stage-0
    Fetch Operator
      limit: -1
      Processor Tree:
        ListSink

Time taken: 0.139 seconds, Fetched: 233 row(s)
```





## 题目二

找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。



### 错误解答



```sql
select m.moviename, r.movieid, avg(r.rate) as avgrate, count(*) as total
from t_rating as r
left join t_user as u on (r.userid = u.userid and u.sex='M') 
left join t_movie as m on m.movieid = r.movieid
group by m.moviename,r.movieid
having total > 50
order by avgrate desc
limit 10;

Sanjuro (1962)	4.608695652173913	69
Seven Samurai (The Magnificent S	4.560509554140127	628
Shawshank Redemption, The (1994)	4.554557700942973	2227
Godfather, The (1972)	4.524966261808367	2223
Close Shave, A (1995)	4.52054794520548	657
Usual Suspects, The (1995)	4.517106001121705	1783
Schindler's List (1993)	4.510416666666667	2304
Wrong Trousers, The (1993)	4.507936507936508	882
Sunset Blvd. (a.k.a. Sunset Boul	4.491489361702127	470
Raiders of the Lost Ark (1981)	4.477724741447892	2514
Time taken: 16.081 seconds, Fetched: 10 row(s)
              
```

得出结论与页面提示不一致，对比来看对于结果集的划分出现了差别，上述统计值应该包含了女性的评分，但是题目实际要求不包含。

```sql
hive> select count(*) from t_rating where movieid= 2905;
OK
69
```

通过查询验证了猜想。



### 排错

```sql
select u.sex,m.moviename, r.movieid, avg(r.rate) as avgrate, count(*) as total
from t_rating as r
left join t_user as u on r.userid = u.userid 
left join t_movie as m on m.movieid = r.movieid
where u.sex = 'M'
group by u.sex,m.moviename,r.movieid
having total > 50 
order by avgrate desc
limit 10;

VERTICES: 05/05  [==========================>>] 100%  ELAPSED TIME: 14.70 s
----------------------------------------------------------------------------------------------
OK
M	Sanjuro (1962)	2905	4.639344262295082	61
M	Godfather, The (1972)	858	4.583333333333333	1740
M	Seven Samurai (The Magnificent S	2019	4.576628352490421	522
M	Shawshank Redemption, The (1994)	318	4.560625	1600
M	Raiders of the Lost Ark (1981)	1198	4.520597322348094	1942
M	Usual Suspects, The (1995)	50	4.518248175182482	1370
M	Star Wars: Episode IV - A New Ho	260	4.495307167235495	2344
M	Schindler's List (1993)	527	4.49141503848431	1689
M	Paths of Glory (1957)	1178	4.485148514851486	202
M	Wrong Trousers, The (1993)	1148	4.478260869565218	644
Time taken: 16.073 seconds, Fetched: 10 row(s)
```



忽然意识到上面的问题出在使用了left join 而不是inner join，其实修改后不用改其他逻辑即可

```sql
select u.sex, m.moviename, avg(r.rate) as avgrate, count(*) as total
from t_rating as r
inner join t_user as u on (r.userid = u.userid and u.sex='M') 
left join t_movie as m on m.movieid = r.movieid
group by u.sex,m.moviename,r.movieid
having total > 50
order by avgrate desc
limit 10;

VERTICES: 05/05  [==========================>>] 100%  ELAPSED TIME: 13.44 s
----------------------------------------------------------------------------------------------
OK
M	Sanjuro (1962)	4.639344262295082	61
M	Godfather, The (1972)	4.583333333333333	1740
M	Seven Samurai (The Magnificent S	4.576628352490421	522
M	Shawshank Redemption, The (1994)	4.560625	1600
M	Raiders of the Lost Ark (1981)	4.520597322348094	1942
M	Usual Suspects, The (1995)	4.518248175182482	1370
M	Star Wars: Episode IV - A New Ho	4.495307167235495	2344
M	Schindler's List (1993)	4.49141503848431	1689
M	Paths of Glory (1957)	4.485148514851486	202
M	Wrong Trousers, The (1993)	4.478260869565218	644
Time taken: 16.075 seconds, Fetched: 10 row(s)
              
```



参考了hive join时条件过滤的规则 https://www.cnblogs.com/zsql/p/14183904.html



## 题目三

找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）。

### 解答1



```sql
select u.userid, count(*) as totalrate
from t_user as u
left join t_rating as r 
on u.userid = r.userid
where u.sex = 'F'
group by u.userid
order by totalrate desc
limit 1;

VERTICES: 05/05  [==========================>>] 100%  ELAPSED TIME: 12.07 s
----------------------------------------------------------------------------------------------
OK
1150	1302
```

得到这位女士的id 1150，下面在sql中直接使用。

```sql
select m.moviename, avg(r.rate) as avgrate
from t_movie as m
left join t_rating as r on m.movieid = r.movieid
where m.movieid in (select movieid from t_rating where userid = 1150 order by rate desc limit 10)
group by m.moviename
order by avgrate desc;

VERTICES: 07/07  [==========================>>] 100%  ELAPSED TIME: 14.97 s
----------------------------------------------------------------------------------------------
OK
Rear Window (1954)	4.476190476190476
Star Wars: Episode IV - A New Ho	4.453694416583082
Waiting for Guffman (1996)	4.147186147186147
Badlands (1973)	4.078838174273859
Roger & Me (1989)	4.0739348370927315
City of Lost Children, The (1995	4.062034739454094
Sound of Music, The (1965)	3.931972789115646
Fast, Cheap & Out of Control (19	3.8518518518518516
Big Lebowski, The (1998)	3.7383773928896993
House of Yes, The (1997)	3.4742268041237114
Time taken: 16.163 seconds, Fetched: 10 row(s)
```



又与提示的答案不同，发现问题是对题意的理解可能存在误差，

影评次数最多的女士所给出最高分的 10 部电影 这句话的断句存在问题。。。 如果是一个人 可能给出不止10个最高分。

```sql
select m.moviename, r.rate from t_rating as r, t_movie as m where r.movieid = m.movieid and userid = 1150 order by rate desc limit 20;

OK
Duck Soup (1933)	5
Trust (1990)	5
His Girl Friday (1940)	5
Roger & Me (1989)	5
Being John Malkovich (1999)	5
Holiday Inn (1942)	5
Jungle Book, The (1967)	5
Crying Game, The (1992)	5
It Happened One Night (1934)	5
Annie Hall (1977)	5
2001: A Space Odyssey (1968)	5
Fast, Cheap & Out of Control (19	5
Shop Around the Corner, The (194	5
African Queen, The (1951)	5
House of Yes, The (1997)	5
Night on Earth (1991)	5
Close Shave, A (1995)	5
Dr. Strangelove or: How I Learne	5
Rear Window (1954)	5
Some Like It Hot (1959)	5

```



所以并不特指一个人的10部，而是女性评分5分次数最多的10部电影？重新解答

### 解答2

```sql
select m.moviename, avg(r.rate) as avgrate
from t_movie as m
left join t_rating as r on m.movieid = r.movieid
where m.movieid in 
(select movieid
from t_rating as a
inner join t_user as b on a.userid = b.userid
where b.sex = 'F' and a.rate = '5'
group by movieid
order by count(*) desc
limit 10)
group by m.moviename
order by avgrate desc;

VERTICES: 09/09  [==========================>>] 100%  ELAPSED TIME: 13.40 s
----------------------------------------------------------------------------------------------
OK
Shawshank Redemption, The (1994)	4.554557700942973
Schindler's List (1993)	4.510416666666667
Raiders of the Lost Ark (1981)	4.477724741447892
Star Wars: Episode IV - A New Ho	4.453694416583082
Sixth Sense, The (1999)	4.406262708418057
Silence of the Lambs, The (1991)	4.3518231186966645
American Beauty (1999)	4.3173862310385065
Princess Bride, The (1987)	4.3037100949094045
Fargo (1996)	4.254675686430561
Shakespeare in Love (1998)	4.127479949345715
Time taken: 16.161 seconds, Fetched: 10 row(s)
```

似乎还是与结果存在差异。待助教解答。