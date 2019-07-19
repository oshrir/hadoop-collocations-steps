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

    private static final String bucketName = "dsp_assignment2";
    private static final String astrix = "*";

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value format now: [decade] [w1] [w2] [bgram count]
            String line = value.toString();

            String[] split = line.split("\\s+");
            Text word = new Text( split[0] + " " + split[1] + " " +astrix);

            context.write(word, new IntWritable(Integer.parseInt(split[3]))); // changed
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
                for (IntWritable val : values) { //TODO can we assume the key arrives with all it's values?
                    sum += val.get();            // i think we can, but we should check it by testing
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
            //This will group the keys in the reducers based on the first word
            String[] split = key.toString().split("\\s+");
            String firstWord = split[0] + " " + split[1];
            return (firstWord.hashCode() & Integer.MAX_VALUE) % numPartitions;
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