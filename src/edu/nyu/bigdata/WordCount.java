// namespace
package edu.nyu.bigdata;

//java dependencioes
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.*;

// hadoop dependencies
//  maven: org.apache.hadoop:hgadoop-client:x.y.z  (x..z = hadoop version, 3.3.0 here
import org.apache.hadoop.conf.Configuration; //driver
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// key is the offset where the line starts, and the value is next line of text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// in-built output format, need to be specified
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

//our program. This program will be 'started' by the Hadoop runtime
public class WordCount {

    // this is the driver
    public static void main(String[] args) throws Exception {

        // get a reference to a job runtime configuration for this program
        Configuration conf = new Configuration();
        // runtime dependencies, we will parse them in command line to hadoop
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // our program needs three params
        if (otherArgs.length <= 1) { //  in out
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        @SuppressWarnings("deprecation")
        // submit the job to hadoop
        Job job = new Job(conf, "jd4678 word-count");
        // specifies Mapper class, Reducer class, and input types and output types
        job.setJarByClass(WordCount.class); // tell the job class type, hadoop will send the class definition to each data node.
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        // k', v' from mapper output and reducer input
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // input to the mapper, fixes the input k, v
        // output from the reducer, fixes the output k'', v''
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // setInputClass is written in fileinput format
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // this class implements the mapper
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         *
         * @param key
         * @param value
         * @param context make you to be able to interact with hadoop, how do you output the values from the mapper, you need to use the context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value
                    .toString()
                    .replaceAll("[^A-z0-9]", " ")
                    .toLowerCase();//homework do this way

            StringTokenizer itr = new StringTokenizer(line);

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
            // word1 1, word2 1, word1 1
        }
    }

    // this class implements the reducer
    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}
