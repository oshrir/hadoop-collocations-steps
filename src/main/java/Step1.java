import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;

public class Step1 {
/*    public enum N{
        Count
    }*/

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static Text astrix = new Text("*");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\\s+");
            String firstWord = split[0];
            String secondWord = split[1];
            String decade = formatDecade(split[2]);
            IntWritable occurrences = new IntWritable(Integer.parseInt(split[3]));
/*            int occurrences = Integer.parseInt(split[3]);
            System.out.println(occurrences);
            //Counting number of 2-grams
            for(int i = 0; i < occurrences; i++) {
                context.getCounter(N.Count).increment(1);
                System.out.println(context.getCounter(N.Count).getValue());
            }*/

            Text toWrite = new Text( decade + " " + firstWord + " " + secondWord);
            context.write(toWrite, occurrences);
            context.write(astrix, occurrences);
        }
    }
    public static class MyReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(key.toString().equals("*")) {
                writeN(sum, context.getConfiguration());

            } else {
                context.write(key, new LongWritable(sum));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("bucketName", args[3]); // TODO - make sure what index it should be
        Job job = new Job(conf, "hadoop-ass2");
        job.setJarByClass(Step1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //job.setCombinerClass(MyReducer.class); // TODO - understand if needed
        job.setInputFormatClass(SequenceFileInputFormat.class); // TODO - change before real run
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1])); //TODO Check the arguments entered
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //String bucketName = args[3]; // TODO - make sure what index it should be
/*        boolean end = job.waitForCompletion(true);
        writeN(conf, bucketName, job);
        System.exit(end ? 0 : 1);*/
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static String formatDecade(String decade){
        return decade.length() == 4 ? decade.substring(0, 3) + "0" : decade;
    }

    public static void writeN(long n, Configuration conf/*, String bucketname, Job job*/)  throws IOException, InterruptedException {
/*        long n = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketname), conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://" + bucketname + "/N.txt"));
        PrintWriter writer  = new PrintWriter(fsDataOutputStream);
        writer.write(String.valueOf(n));
        writer.close();
        fsDataOutputStream.close();*/

        String bucketname = conf.get("bucketName");
        FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketname), conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://" + bucketname + "/N.txt"));
        PrintWriter writer  = new PrintWriter(fsDataOutputStream);
        writer.write(String.valueOf(n));
        writer.close();
        fsDataOutputStream.close();
    }
}