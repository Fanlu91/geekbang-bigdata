package week07;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
                .appName("InvertedIndex")
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
