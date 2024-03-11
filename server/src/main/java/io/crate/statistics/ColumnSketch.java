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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.types.DataType;

/**
 * An intermediate summary of a column's data.  Can be streamed and merged with
 * other instances of the same type, or used to produce a final {@link ColumnStats}
 * summary
 * @param <T>   the column's Java data type
 */
public abstract class ColumnSketch<T> {

    protected final DataType<T> dataType;

    protected final long sampleCount;
    protected final long nullCount;
    protected final long totalBytes;

    protected final Sketch distinctValues;

    /**
     * Constructs a new sketch for a column
     */
    protected ColumnSketch(DataType<T> dataType,
                        long sampleCount,
                        long nullCount,
                        long totalBytes,
                        Sketch distinctValues) {
        this.dataType = dataType;
        this.sampleCount = sampleCount;
        this.nullCount = nullCount;
        this.totalBytes = totalBytes;
        this.distinctValues = distinctValues;
    }

    /**
     * Deserializes a sketch from a stream
     */
    public ColumnSketch(DataType<T> dataType, StreamInput in) throws IOException {
        this.dataType = dataType;
        this.sampleCount = in.readLong();
        this.nullCount = in.readLong();
        this.totalBytes = in.readLong();

        byte[] distinctSketchBytes = in.readByteArray();
        this.distinctValues = Sketches.wrapSketch(Memory.wrap(distinctSketchBytes));
    }

    /**
     * Serializes this sketch to a stream
     */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(this.sampleCount);
        out.writeLong(this.nullCount);
        out.writeLong(this.totalBytes);
        out.writeByteArray(this.distinctValues.toByteArray());
        writeSketches(out);
    }

    protected abstract void writeSketches(StreamOutput out) throws IOException;

    /**
     * Merges this sketch with another of the same type and returns the result.  This
     * sketch may be mutated as a result of the operation and should not be re-used.
     */
    public abstract ColumnSketch<T> merge(ColumnSketch<?> other);

    /**
     * Produces ColumnStats from this sketch
     */
    public abstract ColumnStats<T> toColumnStats();

    double nullFraction() {
        if (nullCount == 0 || sampleCount == 0) {
            return 0;
        }
        return (double) nullCount / (double) sampleCount;
    }

    Sketch mergeDistinct(Sketch other) {
        Union union = SetOperation.builder().buildUnion();
        union.union(distinctValues);
        union.union(other);
        return union.getResult();
    }

    /**
     * Implementation for single valued data types
     */
    public static class SingleValued<T> extends ColumnSketch<T> {

        private final MostCommonValuesSketch<T> mostCommonValues;

        private final HistogramSketch<T> histogram;

        public SingleValued(DataType<T> dataType,
                            long sampleCount,
                            long nullCount,
                            long totalBytes,
                            Sketch distinctValues,
                            MostCommonValuesSketch<T> mostCommonValues,
                            HistogramSketch<T> histogram) {
            super(dataType, sampleCount, nullCount, totalBytes, distinctValues);
            this.mostCommonValues = mostCommonValues;
            this.histogram = histogram;
        }

        public SingleValued(Class<T> clazz, DataType<T> dataType, StreamInput in) throws IOException {
            super(dataType, in);
            this.mostCommonValues = new MostCommonValuesSketch<>(dataType.streamer(), in);
            this.histogram = new HistogramSketch<>(clazz, dataType, in);
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

        @SuppressWarnings("unchecked")
        public SingleValued<T> merge(ColumnSketch<?> other) {
            if (Objects.equals(this.dataType, other.dataType) == false) {
                throw new IllegalArgumentException("Columns must be of the same data type");
            }

            SingleValued<T> typedOther = (SingleValued<T>) other;

            return new SingleValued<>(
                this.dataType,
                sampleCount + other.sampleCount,
                nullCount + other.nullCount,
                totalBytes + other.totalBytes,
                mergeDistinct(other.distinctValues),
                mostCommonValues.merge(typedOther.mostCommonValues),
                histogram.merge(typedOther.histogram)
            );
        }

        protected void writeSketches(StreamOutput out) throws IOException {
            this.mostCommonValues.writeTo(out);
            this.histogram.writeTo(out);
        }

    }

    /**
     * Implementation for array data types.  Note that this does not support histograms
     */
    // TODO - rework this so that we operate on the individual members
    public static class Composite<C> extends ColumnSketch<C> {

        private final MostCommonValuesSketch<BytesRef> mostCommonValues;

        public Composite(DataType<C> dataType,
                            long sampleCount,
                            long nullCount,
                            long totalBytes,
                            Sketch distinctValues,
                            MostCommonValuesSketch<BytesRef> mostCommonValues) {
            super(dataType, sampleCount, nullCount, totalBytes, distinctValues);
            this.mostCommonValues = mostCommonValues;
        }

        public Composite(DataType<C> dataType, StreamInput in) throws IOException {
            super(dataType, in);
            this.mostCommonValues = new MostCommonValuesSketch<>(ColumnSketchBuilder.BYTE_ARRAY_STREAMER, in);
        }

        @Override
        protected void writeSketches(StreamOutput out) throws IOException {
            this.mostCommonValues.writeTo(out);
        }

        @Override
        @SuppressWarnings("unchecked")
        public ColumnSketch<C> merge(ColumnSketch<?> other) {
            if (Objects.equals(this.dataType, other.dataType) == false) {
                throw new IllegalArgumentException("Columns must be of the same data type");
            }

            Composite<C> typedOther = (Composite<C>) other;

            return new Composite<>(
                this.dataType,
                sampleCount + other.sampleCount,
                nullCount + other.nullCount,
                totalBytes + other.totalBytes,
                mergeDistinct(other.distinctValues),
                mostCommonValues.merge(typedOther.mostCommonValues)
            );
        }

        @Override
        public ColumnStats<C> toColumnStats() {
            double nullFraction = nullFraction();
            double avgSizeInBytes = (double) totalBytes / ((double) sampleCount - nullCount);
            double approxDistinct = this.distinctValues.getEstimate();
            MostCommonValues<C> mcv = this.mostCommonValues.toMostCommonValues(sampleCount, approxDistinct, this::fromByteStream);
            return new ColumnStats<>(
                nullFraction,
                avgSizeInBytes,
                approxDistinct,
                dataType,
                mcv,
                List.of()
            );
        }

        private C fromByteStream(BytesRef bytes) {
            try {
                return dataType.streamer().readValueFrom(StreamInput.wrap(bytes.bytes, bytes.offset, bytes.length));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
