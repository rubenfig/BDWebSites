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
package registros;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Secondary sort reducer.
 *
 * @author Jee Vang
 */
public class SsReducer extends Reducer<KeyCompuesta, IntWritable, Text, Text> {

    public void reduce(KeyCompuesta key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Text k = new Text(key.toString());
        Integer count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }
        context.write(k, new Text(count.toString()));

    }
}
