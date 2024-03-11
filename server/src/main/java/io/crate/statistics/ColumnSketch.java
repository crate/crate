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
import java.util.Objects;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.types.DataType;

public class ColumnSketch<T> {

    private final DataType<T> dataType;

    private final long sampleCount;
    private final long nullCount;
    private final long totalBytes;

    private final Sketch distinctValues;
    private final MostCommonValuesSketch<T> mostCommonValues;

    private final HistogramSketch<T> histogram;


    public ColumnSketch(DataType<T> dataType,
                        long sampleCount,
                        long nullCount,
                        long totalBytes,
                        Sketch distinctValues,
                        MostCommonValuesSketch<T> mostCommonValues,
                        HistogramSketch<T> histogram) {
        this.dataType = dataType;
        this.sampleCount = sampleCount;
        this.nullCount = nullCount;
        this.totalBytes = totalBytes;
        this.distinctValues = distinctValues;
        this.mostCommonValues = mostCommonValues;
        this.histogram = histogram;
    }

    public ColumnSketch(Class<T> clazz, DataType<T> dataType, StreamInput in) throws IOException {
        this.dataType = dataType;
        this.sampleCount = in.readLong();
        this.nullCount = in.readLong();
        this.totalBytes = in.readLong();

        byte[] distinctSketchBytes = in.readByteArray();
        this.distinctValues = Sketches.wrapSketch(Memory.wrap(distinctSketchBytes));

        this.mostCommonValues = new MostCommonValuesSketch<>(dataType.streamer(), in);
        this.histogram = new HistogramSketch<>(clazz, dataType, in);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(this.sampleCount);
        out.writeLong(this.nullCount);
        out.writeLong(this.totalBytes);
        out.writeByteArray(this.distinctValues.toByteArray());
        this.mostCommonValues.writeTo(out);
        this.histogram.writeTo(out);
    }

    @SuppressWarnings("unchecked")
    public ColumnSketch<T> merge(ColumnSketch<?> other) {

        if (Objects.equals(this.dataType, other.dataType) == false) {
            throw new IllegalArgumentException("Columns must be of the same data type");
        }

        ColumnSketch<T> typedOther = (ColumnSketch<T>) other;

        Union union = SetOperation.builder().buildUnion();
        union.union(distinctValues);
        union.union(other.distinctValues);
        Sketch mergedDistinct = union.getResult();

        return new ColumnSketch<>(
            this.dataType,
            sampleCount + other.sampleCount,
            nullCount + other.nullCount,
            totalBytes + other.totalBytes,
            mergedDistinct,
            mostCommonValues.merge(typedOther.mostCommonValues),
            histogram.merge(typedOther.histogram)
        );
    }

    public ColumnStats<T> toColumnStats() {
        double nullFraction = nullFraction();
        double avgSizeInBytes = (double) totalBytes / ((double) sampleCount - nullCount);
        double approxDistinct = this.distinctValues.getEstimate();
        MostCommonValues<T> mcv = this.mostCommonValues.toMostCommonValues(sampleCount, approxDistinct);
        return new ColumnStats<>(
            nullFraction,
            avgSizeInBytes,
            approxDistinct,
            dataType,
            mcv,
            this.histogram.toHistogram(100, mcv.values())
        );
    }

    private double nullFraction() {
        if (nullCount == 0 || sampleCount == 0) {
            return 0;
        }
        return (double) nullCount / (double) sampleCount;
    }
}
