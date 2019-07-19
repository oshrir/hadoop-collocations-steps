import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step5 {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static DoubleWritable zero = new DoubleWritable(0);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value format now: [decade] [w1] [w2] [npmi] [relnpmi]
            String line = value.toString();

            double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi", "-1"));
            double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi",
                    "-1"));

            String[] split = line.split("\\s+");
            double npmi = Double.parseDouble(split[3]);
            double relnpmi = Double.parseDouble(split[4]);

            if (isCollocation(npmi, relnpmi, minPmi, relMinPmi)) {
                // key: [decade] [npmi]; value: [w1] [w2]
                context.write(new Text(split[0] + " " + split[3]), new Text(split[1] + " " + split[2]));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            //This will group the keys in the reducers based on the first word
            String firstWord = key.toString().split("\\s+")[0];
            return (firstWord.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("minPmi", args[3]);
        conf.set("relMinPmi", args[4]);
        Job job = new Job(conf, "hadoop-ass2");
        job.setJarByClass(Step5.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setSortComparatorClass(DecadeNPMIComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[1])); //TODO Check the arguments entered
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static boolean isCollocation(double npmi, double relnpmi, double minPmi, double relMinPmi) {
        return npmi >= minPmi | relnpmi >= relMinPmi;
    }
}