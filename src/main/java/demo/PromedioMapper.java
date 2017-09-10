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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Secondary sort mapper.
 *
 * @author Jee Vang
 */
public class PromedioMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Log _log = LogFactory.getLog(PromedioMapper.class);
    private Long previous = null;
    private String pUsuario = null;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString());
        String usuario = st.nextToken();
        Long timestamp = Long.parseLong(st.nextToken());
        String url = st.nextToken();
        if (previous == null && pUsuario == null) {
            previous = timestamp;
            pUsuario = usuario;
        } else if (usuario.compareTo(pUsuario) != 0) {
            previous = timestamp;
            pUsuario = usuario;
        } else {
            context.write(new Text(url), new LongWritable(previous - timestamp));
        }

    }
}
