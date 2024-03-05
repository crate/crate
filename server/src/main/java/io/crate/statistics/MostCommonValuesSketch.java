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

import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;

public class MostCommonValuesSketch<T> {

    private final ItemsSketch<T> sketch;
    private final SketchStreamer<T> streamer;

    public MostCommonValuesSketch(Streamer<T> streamer) {
        this.streamer = new SketchStreamer<>(streamer);
        this.sketch = new ItemsSketch<>(256);
    }

    public MostCommonValuesSketch(Streamer<T> streamer, StreamInput in) throws IOException {
        this.streamer = new SketchStreamer<>(streamer);
        byte[] bytes = in.readByteArray();
        this.sketch = ItemsSketch.getInstance(Memory.wrap(bytes), this.streamer);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(this.sketch.toByteArray(streamer));
    }

    @SuppressWarnings("unchecked")
    public MostCommonValuesSketch<T> merge(MostCommonValuesSketch<?> other) {
        var otherSketch = (ItemsSketch<T>) other.sketch;
        this.sketch.merge(otherSketch);
        return this;
    }

    public void update(T value) {
        this.sketch.update(value);
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
