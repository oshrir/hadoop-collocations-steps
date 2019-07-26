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

/* Fourth Step - Calculate Relative NPMI:
   ======================================
    Input - [decade] [w1] [w2] [npmi]
    1. First emit [decade] as key each given value in the mapper with [npmi] as value.
    2. Afterwards, emit the value of the mapper (as key) with [npmi] as value.
    3. For a given [decade]:
        a. [sum] = the npmis of bigrams in [decade].
        b. Calculate [relnpmi] = [npmi] / [sum].
        b. Emit every [decade] [w1] [w2] [npmi] as key with [relnpmi].

    # Implemented a Partitioner class that is responsible for distributing a given [decade] to
      the same reducer.
    # Added a Combiner to optimize the runtime of this step. */

public class Step4 {

    private static final String punc = "!";

    public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value format now: [decade] [w1] [w2] [npmi]
            String line = value.toString();

            String[] split = line.split("\\s+");
            Text decade = new Text(split[0] + " " + punc);

            DoubleWritable npmi = new DoubleWritable(Double.parseDouble(split[3]));

            context.write(decade, npmi);
            context.write(value, npmi);
        }
    }

    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        double sum;

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            String[] split = key.toString().split("\\s+");
            if (split[1].equals(punc)) {
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
            //This will group the keys in the reducers based on the decade
            String decadeStr = key.toString().split("\\s+")[0];
            int decade = Integer.parseInt(decadeStr) / 10;
            return decade % numPartitions;
        }
    }

    public static class MyCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoubleWritable(sum));
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
        job.setCombinerClass(MyCombiner.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}