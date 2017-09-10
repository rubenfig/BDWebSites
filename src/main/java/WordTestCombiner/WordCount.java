package WordTestCombiner; /**
 * Created by ruben on 10/09/17.
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount extends Configured implements Tool {

    public class ReduceClass extends Reducer {

        protected void reduce(Text key, Iterable values,
                              Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            Iterator valuesIt = values.iterator();

            //For each key value pair, get the value and adds to the sum
            //to get the total occurances of a word
            while (valuesIt.hasNext()) {
                sum = sum + (int) valuesIt.next();
            }

            //Writes the word and total occurances as key-value pair to the context
            context.write(key, new IntWritable(sum));
        }
    }

    public class MapClass extends Mapper {

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {

            //Get the text and tokenize the word using space as separator.
            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line, " ");

            //For each token aka word, write a key value pair with
            //word and 1 as value to context
            while (st.hasMoreTokens()) {
                word.set(st.nextToken());
                context.write(word, one);
            }

        }
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        //Create a new Jar and set the driver class(this class) as the main class of jar
        Job job = new Job();
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCounter");

        //Set the input and the output path from the arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Set the map and reduce classes in the job
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);

        //Run the job and wait for its completion
        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        if (job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }
}