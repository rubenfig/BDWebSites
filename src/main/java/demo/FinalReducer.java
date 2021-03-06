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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Secondary sort reducer.
 *
 * @author Jee Vang
 */
public class FinalReducer extends
        TableReducer<ImmutableBytesWritable, Text,
                ImmutableBytesWritable> {
    Integer count = 1;

    public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> valuesIt = values.iterator();

        //For each key value pair, get the value and adds to the sum
        //to get the total occurances of a word
        while (valuesIt.hasNext()) {
            Text l = valuesIt.next();
            // Put to HBase
            Put put = new Put(key.get());
            put.add(Bytes.toBytes("data"), Bytes.toBytes("promedio"),
                    Bytes.toBytes(l.toString()));
            context.write(key, put);
            count++;
        }

        //Writes the word and total occurances as key-value pair to the context


    }

}
