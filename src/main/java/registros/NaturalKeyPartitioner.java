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
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions key based on "natural" key of {@link KeyCompuesta} (which
 * is the diagnostico).
 *
 * @author Jee Vang
 */
public class NaturalKeyPartitioner extends Partitioner<KeyCompuesta, IntWritable> {

    @Override
    public int getPartition(KeyCompuesta key, IntWritable val, int numPartitions) {
        int hash = key.getFecha().hashCode();
        int partition = hash % numPartitions;
        return partition;
    }

}
