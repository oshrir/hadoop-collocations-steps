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

/* Fifth Step - Extract Collocations:
   ======================================
    Input - [decade] [w1] [w2] [npmi] [relnpmi]
    1. Check if a given bigram is a collocation.
    2. If it is, emit [decade] [w1] [w2] [npmi]

    # Used the same partitioner as in Step4.
    # Implemented a Comparator in order to sort data by npmi (descending). */

public class Step5 {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

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
                // key: [npmi] [decade]; value: [w1] [w2]
                context.write(new Text(split[3] + " " + split[0]), new Text(split[1] + " " + split[2]));
            }
        }

        private static boolean isCollocation(double npmi, double relnpmi, double minPmi, double relMinPmi) {
            return npmi >= minPmi || relnpmi >= relMinPmi;
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] keySplit = key.toString().split(" ");
            String npmi = keySplit[0];
            String decade = keySplit[1];
            String bigram = values.iterator().next().toString();

            Text newKey = new Text(decade + " " + bigram);
            context.write(newKey, new Text(npmi));
        }
    }

    public static class DescendingNPMIComparator extends WritableComparator {
        protected DescendingNPMIComparator() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            return -w1.compareTo(w2);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            //This will group the keys in the reducers based on the decade
            String decade = key.toString().split("\\s+")[1];
            return Math.abs(decade.hashCode()) % numPartitions;
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
        job.setSortComparatorClass(DescendingNPMIComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}