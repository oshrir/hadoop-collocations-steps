import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step2 {

    private static final String astrix = "*";

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable zero = new IntWritable(0);
        private Text keyToWrite = new Text("");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value format now: [decade] [w1] [w2] [bgram count]
            String line = value.toString();

            String[] split = line.split("\\s+");
            keyToWrite.set( split[0] + " " + split[1] + " " + astrix);

            context.write(keyToWrite, new IntWritable(Integer.parseInt(split[3]))); // changed
            context.write(value, zero); // changed
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        int sum;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            String[] split = key.toString().split(" ");
            if(split[2].equals(astrix)) {
                sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
            }
            else {
                context.write(key, new IntWritable(sum));
            }

        }
    }

    public static class PartitionerClass extends Partitioner<Text,IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            //This will group the keys in the reducers based on the decade
            /*String[] split = key.toString().split("\\s+");
            String firstWord = split[0] + " " + split[1];*/
            String decade = key.toString().split("\\s+")[0];
            return (decade.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "hadoop-ass2");
        job.setJarByClass(Step2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(PartitionerClass.class);
        FileInputFormat.addInputPath(job, new Path(args[1])); //TODO Check the arguments entered
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}