/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.statistics;

import java.io.IOException;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class MostCommonValuesSketch {

    private final ItemsSketch<String> sketch;

    public MostCommonValuesSketch() {
        this.sketch = new ItemsSketch<>(256);
    }

    public MostCommonValuesSketch(StreamInput in) throws IOException {
        byte[] bytes = in.readByteArray();
        this.sketch = ItemsSketch.getInstance(Memory.wrap(bytes), new ArrayOfStringsSerDe());
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(this.sketch.toByteArray(new ArrayOfStringsSerDe()));
    }

    public MostCommonValuesSketch merge(MostCommonValuesSketch other) {
        this.sketch.merge(other.sketch);
        return this;
    }

    public void update(Object value) {
        this.sketch.update(value.toString());
    }

    public MostCommonValues toMostCommonValues() {
        var rows = this.sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
        int count = rows.length;
        double[] freqs = new double[count];
        Object[] values = new Object[count];
        int i = 0;
        for (var row : rows) {
            freqs[i] = row.getEstimate();
            values[i] = row.getItem();
            i++;
        }
        return new MostCommonValues(values, freqs);
    }

}
