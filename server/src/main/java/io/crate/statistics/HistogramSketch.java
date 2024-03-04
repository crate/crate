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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class HistogramSketch {

    private final ItemsSketch<String> sketch;

    public HistogramSketch() {
        this.sketch = ItemsSketch.getInstance(String.class, Comparator.naturalOrder());
    }

    public HistogramSketch(StreamInput in) throws IOException {
        byte[] bytes = in.readByteArray();
        this.sketch = ItemsSketch.getInstance(
            String.class, Memory.wrap(bytes), Comparator.naturalOrder(), new ArrayOfStringsSerDe()
        );
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(this.sketch.toByteArray(new ArrayOfStringsSerDe()));
    }

    private HistogramSketch(ItemsSketch<String> sketch) {
        this.sketch = sketch;
    }

    public void update(Object value) {
        this.sketch.update(value.toString());
    }

    public HistogramSketch merge(HistogramSketch other) {
        ItemsUnion<String> union = ItemsUnion.getInstance(this.sketch);
        union.union(other.sketch);
        return new HistogramSketch(union.getResult());
    }

    public List<String> toHistogram() {
        int numBins = (int) Math.max(100, sketch.getN());
        double inc = (double) numBins / 100;
        List<String> values = new ArrayList<>(numBins);
        for (int i = 0; i < numBins; i++) {
            values.add(sketch.getQuantile(inc, QuantileSearchCriteria.EXCLUSIVE));
        }
        return values;
    }

}
