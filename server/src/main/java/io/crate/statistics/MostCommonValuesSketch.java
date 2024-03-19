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
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;

public class MostCommonValuesSketch<T> {

    private static final int MAX_VALUES = 100;

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

    public MostCommonValues<T> toMostCommonValues(long valueCount, double approxDistinct) {
        return toMostCommonValues(valueCount, approxDistinct, Function.identity());
    }

    public <R> MostCommonValues<R> toMostCommonValues(long valueCount, double approxDistinct, Function<T, R> converter) {
        var rows = this.sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
        int count = Math.min(MAX_VALUES, rows.length);
        long[] counts = new long[count];
        List<R> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            counts[i] = rows[i].getLowerBound();
            values.add(converter.apply(rows[i].getItem()));
        }
        if (approxDistinct < MAX_VALUES) {
            return new MostCommonValues<>(values, toFreqs(counts, valueCount));
        }
        int cutoff = cutoff(this.sketch.getStreamLength(), approxDistinct, counts);
        values = values.subList(0, cutoff);
        counts = Arrays.copyOf(counts, cutoff);
        return new MostCommonValues<>(values, toFreqs(counts, valueCount));
    }

    private static double[] toFreqs(long[] counts, long valueCount) {
        double[] freqs = new double[counts.length];
        for (int i = 0; i < freqs.length; i++) {
            freqs[i] = (double) counts[i] / valueCount;
        }
        return freqs;
    }

    private static int cutoff(long valueCount, double approxDistinct, long[] counts) {
        int cutoff = counts.length;

        long sumFreq = 0;
        // sum of all counts except for the least frequent value; this sum always
        // excludes the count of the value that is being considered for rejection
        for (int i = 0; i < counts.length - 1; i++) {
            sumFreq += counts[i];
        }

        while (cutoff > 0) {
            double selectivity = 1.0 - ((double) sumFreq / valueCount);
            selectivity = Math.max(0.0, Math.min(1.0, selectivity));
            double otherDistinct = approxDistinct - cutoff + 1;
            if (otherDistinct > 1) {
                selectivity /= otherDistinct;
            }

            selectivity *= valueCount + 0.5;

            if (counts[cutoff - 1] > selectivity) {
                // this value and all those above are sufficiently high-frequency to keep
                break;
            } else {
                // discard this value and move to the next least common
                cutoff--;
                if (cutoff == 0) {
                    break;
                }
                sumFreq -= counts[cutoff - 1];
            }
        }

        return cutoff;
    }

}
