import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Step4 {

    private static final String bucketName = "dsp_assignment2";
    private static final String astrix = "*";

    public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable zero = new DoubleWritable(0);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value format now: [decade] [w1] [w2] [npmi]
            String line = value.toString();

            String[] split = line.split("\\s+");
            Text word = new Text(split[0] + " " + astrix);

            DoubleWritable npmi = new DoubleWritable(Double.parseDouble(split[3]));

            context.write(word, npmi); // changed
            context.write(value, npmi); // changed
        }
    }

    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        double sum;

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            String[] split = key.toString().split("\\s+");
            if (split[1].equals(astrix)) {
                sum = 0;
                for (DoubleWritable val : values) {
                    sum += val.get();
                }
            } else {

                double wordNPMI = values.iterator().next().get();
                double wordRelNPMI = wordNPMI / sum;
                context.write(key, new DoubleWritable(wordRelNPMI));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, DoubleWritable> {

        @Override
        public int getPartition(Text key, DoubleWritable value, int numPartitions) {
            //This will group the keys in the reducers based on the first word
            String firstWord = key.toString().split("\\s+")[0];
            return (firstWord.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "hadoop-ass2");
        job.setJarByClass(Step4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
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