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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.types.DataType;

/**
 * A streamable representation of an update-able histogram sketch
 * @param <T>
 */
public class HistogramSketch<T> {

    private final ItemsSketch<T> sketch;
    private final SketchStreamer<T> streamer;

    public HistogramSketch(Class<T> clazz, DataType<T> dataType) {
        this.sketch = ItemsSketch.getInstance(clazz, dataType);
        this.streamer = new SketchStreamer<>(dataType.streamer());
    }

    public HistogramSketch(Class<T> clazz, DataType<T> dataType, StreamInput in) throws IOException {
        byte[] bytes = in.readByteArray();
        this.streamer = new SketchStreamer<>(dataType.streamer());
        this.sketch = ItemsSketch.getInstance(clazz, Memory.wrap(bytes), dataType, streamer);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(this.sketch.toByteArray(streamer));
    }

    private HistogramSketch(SketchStreamer<T> streamer, ItemsSketch<T> sketch) {
        this.streamer = streamer;
        this.sketch = sketch;
    }

    public void update(T value) {
        this.sketch.update(value);
    }

    @SuppressWarnings("unchecked")
    public HistogramSketch<T> merge(HistogramSketch<?> other) {
        ItemsUnion<T> union = ItemsUnion.getInstance(this.sketch);
        union.union((ItemsSketch<T>) other.sketch);
        return new HistogramSketch<>(streamer, union.getResult());
    }

    public List<T> toHistogram(int maxBins, List<T> valuesToExclude) {
        if (sketch.isEmpty()) {
            return List.of();
        }
        Set<T> exclusions = new HashSet<>(valuesToExclude);
        List<T> histogram = new ArrayList<>();

        long count = sketch.getN();
        int numBins = (int) Math.min(maxBins, count); // 100 bins at most
        if (numBins <= 1) {
            return List.of();
        }
        long inc = (count - 1) / (numBins - 1);
        long rank = 0;
        while (rank < count) {
            T value = sketch.getQuantile((double) rank / count, QuantileSearchCriteria.EXCLUSIVE);
            if (exclusions.contains(value) == false) {
                histogram.add(value);
            }
            rank += inc;
        }

        return histogram;
    }

}
