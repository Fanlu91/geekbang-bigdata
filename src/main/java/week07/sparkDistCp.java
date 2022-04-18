package week07;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class sparkDistCp {
    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: sparkDistCp <hdfs://xxx/source> <hdfs://xxx/target> [ -i Ignore failures ] [ -m max concurrency ]");
            System.exit(1);
        }

        String source = args[0];
        String target = args[1];
        boolean ignoreFailure = false;
        int maxCon = 0;
        for (int i = 2; i < args.length; i++) {
            if (args[i].equals("-i"))
                ignoreFailure = true;
            if (args[i].equals("-m") && args.length - 1 > i)
                maxCon = Integer.parseInt(args[i + 1]);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("sparkDistCp")
//                .config("spark.master", "local")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
    }
}
