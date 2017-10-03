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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Compares the composite key, {@link KeyCompuesta}.
 * We sort by diagnostico ascendingly and fecha
 * descendingly.
 *
 * @author Jee Vang
 */
public class CompositeKeyComparator extends WritableComparator {

    /**
     * Constructor.
     */
    protected CompositeKeyComparator() {
        super(KeyCompuesta.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        KeyCompuesta k1 = (KeyCompuesta) w1;
        KeyCompuesta k2 = (KeyCompuesta) w2;

        int result = k1.getDiagnostico().compareTo(k2.getDiagnostico());
        if (0 == result) {
            result = k1.getFecha().compareTo(k2.getFecha());
            if (0 == result) {
                result = k1.getDepartamento().compareTo(k2.getDepartamento());
            }
        }
        return result;
    }
}
