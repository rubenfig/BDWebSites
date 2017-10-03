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
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stock key. This key is a composite key. The "natural"
 * key is the diagnostico. The secondary sort will be performed
 * against the fecha.
 *
 * @author Jee Vang
 */
public class KeyCompuesta implements WritableComparable<KeyCompuesta> {

    private String diagnostico;
    private String fecha;
    private String departamento;

    /**
     * Constructor.
     */
    public KeyCompuesta() {
    }

    /**
     * Constructor.
     *
     * @param diagnostico Stock diagnostico. i.e. APPL
     * @param fecha       Fecha. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
     */
    public KeyCompuesta(String diagnostico, String fecha, String departamento) {
        this.diagnostico = diagnostico;
        this.fecha = fecha;
        this.departamento = departamento;
    }

    @Override
    public String toString() {
        return (new StringBuilder())
                .append(diagnostico)
                .append(',')
                .append(fecha)
                .append(',')
                .append(departamento)
                .toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        diagnostico = WritableUtils.readString(in);
        fecha = WritableUtils.readString(in);
        departamento = WritableUtils.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, diagnostico);
        WritableUtils.writeString(out, fecha);
        WritableUtils.writeString(out, departamento);
    }

    @Override
    public int compareTo(KeyCompuesta o) {
        int result = diagnostico.compareTo(o.diagnostico);
        if (0 == result) {
            result = fecha.compareTo(o.fecha);
            if (0 == result) {
                result = departamento.compareTo(o.departamento);

            }
        }
        return result;
    }

    /**
     * Gets the diagnostico.
     *
     * @return Diagnostico.
     */
    public String getDiagnostico() {
        return diagnostico;
    }

    public void setDiagnostico(String diagnostico) {
        this.diagnostico = diagnostico;
    }

    /**
     * Gets the fecha.
     *
     * @return Fecha. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
     */
    public String getFecha() {
        return fecha;
    }

    public void setFecha(String fecha) {
        this.fecha = fecha;
    }

    /**
     * Gets the departamento.
     *
     * @return Departamento. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
     */
    public String getDepartamento() {
        return fecha;
    }

    public void setDepartamento(String fecha) {
        this.fecha = fecha;
    }

}
