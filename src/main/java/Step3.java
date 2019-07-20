import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.net.URI;

public class Step3 {

    private static final String astrix = "*";

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable zero = new IntWritable(0);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value format now: [decade] [w1] [w2] [bgram count] [w1 count]
            String line = value.toString();

            String[] split = line.split("\\s+");
            Text word = new Text(split[0] + " " + split[2] + " " + astrix);
            Text bgram = new Text(flipArrayValues(split, 1, 2));

            context.write(word, new IntWritable(Integer.parseInt(split[3]))); // changed
            context.write(bgram, zero); // changed
        }

        private static String flipArrayValues(String[] arr, int i, int j){
            String temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;

            String output = "";
            for (int k = 0; k < arr.length; k++) {
                output += arr[k];
                if (k < arr.length - 1) {
                    output += " ";
                }
            }

            return output;
        }
    }
    public static class MyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        int sum;
        private static Long N;
        private String bucketName;

        public void setup(Context context)  throws IOException, InterruptedException {
            String input;
            bucketName = context.getConfiguration().get("bucketName");
            FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketName), context.getConfiguration());
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path(("s3://" + bucketName +"/N.txt")));//"C:\\Users\\elior\\eclipse-workspace\\DSP2\\eliortapirobucket\\N.txt"));//
            input = IOUtils.toString(fsDataInputStream, "UTF-8");
            fsDataInputStream.close();
            N = Long.valueOf(input);
        }

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

                double wordNPMI = CalculateNPMI(key.toString(), sum);
                if (wordNPMI > -1) {
                    context.write(new Text(split[0] + " " + split[2] + " " + split[1]),
                            new DoubleWritable(wordNPMI));
                }
            }

        }

        private static double CalculateNPMI(String key, int w2Count){ //TODO implement
            String[] split = key.split("\\s+");
            int bgramCount = Integer.parseInt(split[3]);
            int w1Count = Integer.parseInt(split[4]);
            double pmi = Math.log(bgramCount) + Math.log(N) - Math.log(w1Count) - Math.log(w2Count);
            double p = Math.log(N / bgramCount);
            return p == 0 ? -1 : pmi / p;
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
        conf.set("bucketName", args[3]); // TODO - make sure what index it should be
        Job job = new Job(conf, "hadoop-ass2");
        job.setJarByClass(Step3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(PartitionerClass.class);
        FileInputFormat.addInputPath(job, new Path(args[1])); //TODO Check the arguments entered
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //String bucketName = args[3]; // TODO - make sure what index it should be
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}