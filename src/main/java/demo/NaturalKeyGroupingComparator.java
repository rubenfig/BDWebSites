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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Groups values based on symbol of {@link VisitKey} (the natural key).
 * @author Jee Vang
 *
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

    /**
     * Constructor.
     */
    protected NaturalKeyGroupingComparator() {
        super(VisitKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        VisitKey k1 = (VisitKey) w1;
        VisitKey k2 = (VisitKey) w2;

        return k1.getSymbol().compareTo(k2.getSymbol());
    }
}
