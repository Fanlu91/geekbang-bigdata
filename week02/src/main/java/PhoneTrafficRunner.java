
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

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