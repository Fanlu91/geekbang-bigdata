

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



# 作业

## 作业一：为 Spark SQL 添加一条自定义命令

SHOW VERSION；
显示当前 Spark 版本和 Java 版本。
## 作业二：构建 SQL 满足如下要求

通过 set spark.sql.planChangeLog.level=WARN，查看：

构建一条 SQL，同时 apply 下面三条优化规则：
CombineFilters
CollapseProject
BooleanSimplification
构建一条 SQL，同时 apply 下面五条优化规则：
ConstantFolding
PushDownPredicates
ReplaceDistinctWithAggregate
ReplaceExceptWithAntiJoin
FoldablePropagation



## 作业三：实现自定义优化规则（静默规则）

1. 第一步：实现自定义规则 (静默规则，通过 set spark.sql.planChangeLog.level=WARN，确认执行到就行)

```scala
case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
 def apply(plan: LogicalPlan): LogicalPlan = plan transform { .... }
}
```

2. 第二步：创建自己的 Extension 并注入
3. 第三步：通过 spark.sql.extensions 提交
   `bin/spark-sql --jars my.jar --conf spark.sql.extensions=com.jikeshijian.MySparkSessionExtension`
