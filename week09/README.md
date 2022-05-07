

# spark cli 练习



```scala
[student3@emr-header-1 fanlu]$ which spark-shell
/usr/lib/spark-current/bin/spark-shell
[student3@emr-header-1 fanlu]$ spark-shell --master local

scala> val df1 = (0 to 100).toDF
df1: org.apache.spark.sql.DataFrame = [value: int]

scala> df1.count
res0: Long = 101
scala> df1.filter(r => r%2 ==0).toDF
<console>:24: error: value % is not a member of org.apache.spark.sql.Row
       df1.filter(r => r%2 ==0).toDF
                        ^
scala> val ds1 = (0 to 100).toDS
ds1: org.apache.spark.sql.Dataset[Int] = [value: int]

scala> ds1.count
res7: Long = 101
scala> ds1.filter(r => r % 2 == 0).count
res8: Long = 51


scala> val ds2 = ds1.filter(r => r % 2 == 0)
ds2: org.apache.spark.sql.Dataset[Int] = [value: int]

scala> ds2.explain(true)
== Parsed Logical Plan ==
'TypedFilter $line25.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$Lambda$4008/1550984990@1f0b18ed, int, [StructField(value,IntegerType,false)], unresolveddeserializer(assertnotnull(upcast(getcolumnbyordinal(0, IntegerType), IntegerType, - root class: "scala.Int")))
+- LocalRelation [value#12]

== Analyzed Logical Plan ==
value: int
TypedFilter $line25.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$Lambda$4008/1550984990@1f0b18ed, int, [StructField(value,IntegerType,false)], assertnotnull(cast(value#12 as int))
+- LocalRelation [value#12]

== Optimized Logical Plan ==
TypedFilter $line25.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$Lambda$4008/1550984990@1f0b18ed, int, [StructField(value,IntegerType,false)], value#12: int
+- LocalRelation [value#12]

== Physical Plan ==
*(1) Filter $line25.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$Lambda$4008/1550984990@1f0b18ed.apply$mcZI$sp
+- *(1) LocalTableScan [value#12]
```

