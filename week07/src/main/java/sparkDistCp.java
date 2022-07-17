import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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
        int maxCon = 3;
        for (int i = 2; i < args.length; i++) {
            if (args[i].equals("-i"))
                ignoreFailure = true;
            if (args[i].equals("-m") && args.length - 1 > i)
                maxCon = Integer.parseInt(args[i + 1]);
        }

        SparkContext context = new SparkContext(new SparkConf().setAppName("sparkDistCp").setMaster("local[*]")
                .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000").set("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName())
        );
        SparkSession session = SparkSession.builder().sparkContext(context).getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        FileSystem fileSystem = FileSystem.get(jsc.hadoopConfiguration());
        fileSystem.mkdirs(new Path("hdfs://localhost:9000/" + target + "/test"), new FsPermission("777"));

        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(new Path("hdfs://localhost:9000/" + source), true);
        // copy hdfs files to another location
        while (it.hasNext()) {
            LocatedFileStatus lfs = it.next();
            System.out.println(lfs.getPath());
            System.out.println(lfs.getPath().getParent());
            System.out.println(lfs.getPath().getName());
            System.out.println(lfs.getPath().depth());

//            FileUtil.copy(fileSystem, lfs.getPath(),
//                    fileSystem, new Path("hdfs://localhost:9000/" + target + "/" + lfs.getPath().getName()),
//                    false, false, jsc.hadoopConfiguration());
        }

        session.stop();
    }
}
