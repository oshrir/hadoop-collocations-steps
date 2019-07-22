import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/* First Step - Data validation & Count number of bigrams in total and by decade:
   ==============================================================================
    1. Extracts the bigram from the corpus.
    2. Data validation by filtering 1-grams and bigrams that contain stop words.
    3. Counts the occurrences of a given bigram in the corpus by decade.
    4. Count the total amount of bigrams.

    # Added a Combiner to optimize the runtime of this step. */

public class Step1 {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final Text punc = new Text("!");
        private Text keyToWrite = new Text("");
        private String line;
        private String[] split;
        private Set<String> stopWords;

        protected void setup(Context context) {
            String stopWordsList = "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at," +
                    "be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got," +
                    "had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely," +
                    "may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said," +
                    "say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too," +
                    "twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your," +
                    "!,*,.,:,[,],{,},-,—,\"\"\",\',(,),•,$,%,/,;,?,»,°,«,|,<,>,&,#,+";

            stopWords = new HashSet<>();
            stopWords.addAll(Arrays.asList(stopWordsList.split(",")));
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            line = value.toString();
            // since the bigram is separated from the other data with tab, we should first separate by \t:
            split = line.split("\t");
            if (split.length < 1) {
                return;
            }
            String[] words = split[0].split(" ");
            // valid input should be: [w1, w2, year, number, (irrelevant)]
            if (words.length < 2) {
                return;
            }
            String firstWord = words[0];
            String secondWord = words[1];
            if (stopWords.contains(firstWord) || stopWords.contains(secondWord)) {
                return;
            }
            String decade = formatDecade(split[1]);
            LongWritable occurrences = new LongWritable(Integer.parseInt(split[2]));

            keyToWrite.set(decade + " " + firstWord + " " + secondWord);
            context.write(keyToWrite, occurrences);
            context.write(punc, occurrences);
        }

        private static String formatDecade(String decade){
            return decade.substring(0, 3) + "0";
        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            if(key.toString().equals("!")) {
                writeN(sum, context.getConfiguration());
            } else {
                context.write(key, new LongWritable(sum));
            }
        }

        private static void writeN(long n, Configuration conf) throws IOException {
            String bucketname = conf.get("bucketName");
            FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketname), conf);
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://" + bucketname + "/N.txt"));
            PrintWriter writer  = new PrintWriter(fsDataOutputStream);
            writer.write(String.valueOf(n));
            writer.close();
            fsDataOutputStream.close();
        }
    }

    public static class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("bucketName", args[3]);
        Job job = new Job(conf, "hadoop-ass2");
        job.setJarByClass(Step1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyCombiner.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}