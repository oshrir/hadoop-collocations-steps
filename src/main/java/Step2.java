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

/* Second Step - Count occurrences of [w1]:
   ========================================
    Input - [decade] [w1] [w2] [bgram count]
    1. First emit [decade] [w1] [!] as key for each given value in the mapper, with [bgram count] as value.
    2. Afterwards, emit the value of the mapper (as key) with zero as value (default value).
    The above actions are used as a technique to count the occurrences of the first word. This technique
    takes advantage of the key sorting mechanism in hadoop.

    3. For a given [w1] in a given [decade]:
        a. [sum] = the occurrences of [w1] as a first word in a bigram in [decade].
        b. Emit every [decade] [w1] [w2] [bgram count] as key with [sum] as value.

    # Implemented a Partitioner class that is responsible for distributing a given pair of [decade] [w1] to
      the same reducer. */

public class Step2 {

    private static final String punc = "!";

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable zero = new IntWritable(0);
        private Text keyToWrite = new Text("");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value format now: [decade] [w1] [w2] [bgram count]
            String line = value.toString();

            String[] split = line.split("\\s+");
            keyToWrite.set( split[0] + " " + split[1] + " " + punc);

            context.write(keyToWrite, new IntWritable(Integer.parseInt(split[3])));
            context.write(value, zero);
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        int sum;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            String[] split = key.toString().split(" ");
            if(split[2].equals(punc)) {
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
            //This will group the keys in the reducers based on the decade and the first word
            String[] split = key.toString().split("\\s+");
            int hash = split[0].hashCode() + split[1].hashCode();
            return Math.abs(hash) % numPartitions;
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
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}