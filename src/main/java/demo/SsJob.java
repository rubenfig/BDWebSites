/**
 * Copyright 2012 Jee Vang
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Secondary sort job.
 * @author Jee Vang
 *
 */
public class SsJob extends Configured implements Tool {

    /**
     * Main method. You should specify -Dmapred.input.dir and -Dmapred.output.dir.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new SsJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "sort de timestamp y usuarios");

        job.setJarByClass(SsJob.class);
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(CompositeKeyComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        job.setMapOutputKeyClass(VisitKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(SsMapper.class);
        job.setReducerClass(SsReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out = new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        job.waitForCompletion(true);


        Job job2 = new Job(conf, "promedio");
        job2.setJarByClass(SsJob.class);

        job2.setMapperClass(PromedioMapper.class);
        job2.setReducerClass(PromedioReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        Path out2 = new Path(args[1] + "/temp");
        TextInputFormat.addInputPath(job2, new Path(args[1] + "/part-r-00000"));
        TextOutputFormat.setOutputPath(job2, out2);

        job2.waitForCompletion(true);

        conf.set("mapred.textoutputformat.separator", ",");

        Job job3 = new Job(conf, "listado final");
        job3.setJarByClass(SsJob.class);

        job3.setMapperClass(Map.class);
        job3.setReducerClass(FinalReducer.class);
        job3.setSortComparatorClass(ViewsDescComparator.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        Path out3 = new Path(args[1] + "/real-output");
        TextInputFormat.addInputPath(job3, new Path(out2 + "/part-r-00000"));
        TextOutputFormat.setOutputPath(job3, out3);
        out.getFileSystem(conf).delete(out3);
        job3.waitForCompletion(true);
        out.getFileSystem(conf).delete(out2);
        return 0;
    }

}
