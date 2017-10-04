package demo;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by ruben on 10/09/17.
 */
public class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        context.write(new ImmutableBytesWritable(tokenizer.nextToken().getBytes()), new Text(tokenizer.nextToken()));
    }
}
