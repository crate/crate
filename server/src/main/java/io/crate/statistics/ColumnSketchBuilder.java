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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.types.DataType;

/**
 * Constructs a {@link ColumnStats} from a set of samples
 */
public abstract class ColumnSketchBuilder<T> {

    protected final DataType<T> dataType;

    protected long sampleCount;
    protected long nullCount;
    protected long totalBytes;
    protected DistinctValuesSketch distinctSketch;

    /// Exact per-value counts of the sample, used to extrapolate the sample's distinct
    /// count in the full table via the Haas-Stokes estimator (same approach as PostgreSQL).
    /// The sample is bounded by [Reservoir#NUM_SAMPLES],
    /// so tracking exact counts (rather than approximating) is cheap.
    protected final Map<String, Long> distinctCounts;

    /// Set when [#distinctCounts] does not (or may not) cover the whole sample, because some
    /// contributor (this instance itself, or one merged into it) was deserialized from a node
    /// older than [Version#V_6_5_0], which never sent per-value counts. Once set, it stays set
    /// through any further merges/serialization, and [#estimateDistinct] falls back to
    /// [#distinctSketch] instead of extrapolating from a silently-incomplete [#distinctCounts].
    protected boolean distinctCountsIncomplete;

    /**
     * Creates a new ColumnSketchBuilder for the given DataType
     */
    public ColumnSketchBuilder(DataType<T> dataType) {
        this.dataType = dataType;
        this.distinctSketch = DistinctValuesSketch.newSketch();
        this.distinctCounts = new HashMap<>();
    }

    public ColumnSketchBuilder(DataType<T> dataType, StreamInput in) throws IOException {
        this.dataType = dataType;
        this.sampleCount = in.readLong();
        this.nullCount = in.readLong();
        this.totalBytes = in.readLong();
        this.distinctSketch = DistinctValuesSketch.fromStream(in);
        if (in.getVersion().onOrAfter(Version.V_6_5_0)) {
            this.distinctCounts = in.readMap(StreamInput::readString, StreamInput::readVLong);
            this.distinctCountsIncomplete = in.readBoolean();
        } else {
            this.distinctCounts = new HashMap<>();
            this.distinctCountsIncomplete = true;
        }
    }

    public final void writeTo(StreamOutput out) throws IOException {
        out.writeLong(sampleCount);
        out.writeLong(nullCount);
        out.writeLong(totalBytes);
        out.writeByteArray(distinctSketch.getSketch().toByteArray());
        if (out.getVersion().onOrAfter(Version.V_6_5_0)) {
            out.writeMap(distinctCounts, StreamOutput::writeString, StreamOutput::writeVLong);
            out.writeBoolean(distinctCountsIncomplete);
        }
        writeSketches(out);
    }

    protected abstract void writeSketches(StreamOutput out) throws IOException;

    /**
     * Add a sample to the sketch
     */
    public void add(T value) {
        sampleCount++;
        if (value == null) {
            nullCount++;
        } else {
            totalBytes += dataType.valueBytes(value);
            String key = value.toString();
            distinctSketch.update(key);
            distinctCounts.merge(key, 1L, Long::sum);
            updateSketches(value);
        }
    }

    protected abstract void updateSketches(T value);

    /**
     * Add a collection of samples to the sketch
     */
    public final void addAll(Collection<T> values) {
        for (var v : values) {
            add(v);
        }
    }

    public abstract ColumnSketchBuilder<T> merge(ColumnSketchBuilder<?> other);

    /**
     * Produce a streamable and merge-able representation of the sketch.
     *
     * @param totalDocs the total (non-sampled) row count of the table, used to extrapolate
     *                   the sample's distinct count to the full population
     */
    public abstract ColumnStats<T> toStats(long totalDocs);

    double nullFraction() {
        if (nullCount == 0 || sampleCount == 0) {
            return 0;
        }
        return (double) nullCount / (double) sampleCount;
    }

    /// Extrapolates the sample's distinct-value count to the full table using the
    /// Haas-Stokes estimator:
    /// ```
    /// n*d / (n - f1 + f1*n/N)
    /// ```
    /// where:
    /// * `n` is the (non-null) sample size
    /// * `d` the distinct count in the sample
    /// * `f1` the count of values seen exactly once in the sample
    /// * `N` the total row count.
    ///
    /// Also see PostgreSQL's [compute_distinct_stats](https://github.com/postgres/postgres/blob/9f4bd91a1960f0263c497a9a1ddf121cbcefbb98/src/backend/commands/analyze.c#L2121).
    ///
    /// Falls back to [#distinctSketch]'s own (non-extrapolated) estimate when
    /// [#distinctCountsIncomplete] is set, since [#distinctCounts] can't be trusted in that case.
    final double estimateDistinct(long totalDocs) {
        if (distinctCountsIncomplete) {
            return distinctSketch.getSketch().getEstimate();
        }
        long n = sampleCount - nullCount;
        long d = distinctCounts.size();
        if (n <= 0) {
            return d;
        }
        long f1 = 0;
        for (long count : distinctCounts.values()) {
            if (count == 1L) {
                f1++;
            }
        }
        double N = Math.max(totalDocs, n);
        double denom = (n - f1) + f1 * (double) n / N;
        double estimate = denom > 0 ? ((double) n * d) / denom : d;
        return Math.round(Math.min(estimate, N));
    }

    /**
     * An implementation for single-valued data types
     */
    // TODO - create a specialized implementation for numeric data
    public static class SingleValued<T> extends ColumnSketchBuilder<T> {

        private MostCommonValuesSketch<T> mostCommonValuesSketch;
        private HistogramSketch<T> histogramSketch;

        public SingleValued(Class<T> clazz, DataType<T> dataType) {
            super(dataType);
            this.mostCommonValuesSketch = new MostCommonValuesSketch<>(dataType.streamer());
            this.histogramSketch = new HistogramSketch<>(clazz, dataType);
        }

        public SingleValued(Class<T> clazz, DataType<T> dataType, StreamInput in) throws IOException {
            super(dataType, in);
            this.mostCommonValuesSketch = new MostCommonValuesSketch<>(dataType.streamer(), in);
            this.histogramSketch = new HistogramSketch<>(clazz, dataType, in);
        }

        @Override
        protected void writeSketches(StreamOutput out) throws IOException {
            mostCommonValuesSketch.writeTo(out);
            histogramSketch.writeTo(out);
        }

        @Override
        protected void updateSketches(T value) {
            mostCommonValuesSketch.update(value);
            histogramSketch.update(value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public ColumnSketchBuilder<T> merge(ColumnSketchBuilder<?> other) {
            if (Objects.equals(this.dataType, other.dataType) == false) {
                throw new IllegalArgumentException("Columns must be of the same data type");
            }

            final ColumnSketchBuilder.SingleValued<T> typedOther = (ColumnSketchBuilder.SingleValued<T>) other;
            this.sampleCount += other.sampleCount;
            this.nullCount += other.nullCount;
            this.totalBytes += other.totalBytes;
            this.distinctSketch = this.distinctSketch.merge(other.distinctSketch);
            this.distinctCountsIncomplete = this.distinctCountsIncomplete || typedOther.distinctCountsIncomplete;
            typedOther.distinctCounts.forEach((key, count) -> this.distinctCounts.merge(key, count, Long::sum));
            this.mostCommonValuesSketch = this.mostCommonValuesSketch.merge(typedOther.mostCommonValuesSketch);
            this.histogramSketch = this.histogramSketch.merge(typedOther.histogramSketch);

            return this;
        }

        @Override
        public ColumnStats<T> toStats(long totalDocs) {
            double nullFraction = nullFraction();
            double avgSizeInBytes = (double) totalBytes / ((double) sampleCount - nullCount);
            double approxDistinct = estimateDistinct(totalDocs);
            MostCommonValues<T> mcv = this.mostCommonValuesSketch.toMostCommonValues(sampleCount, approxDistinct);
            return new ColumnStats<>(
                nullFraction,
                avgSizeInBytes,
                approxDistinct,
                dataType,
                mcv,
                this.histogramSketch.toHistogram(100, mcv.values())
            );
        }

    }

    /**
     * An implementation for array data types
     */
    // TODO - rework so that stats are produced for the individual items in the arrays
    public static class Composite<T> extends ColumnSketchBuilder<T> {

        private MostCommonValuesSketch<BytesRef> mostCommonValuesSketch;
        private final BytesStreamOutput scratch = new BytesStreamOutput();

        public Composite(DataType<T> dataType) {
            super(dataType);
            this.mostCommonValuesSketch = new MostCommonValuesSketch<>(BYTE_ARRAY_STREAMER);
        }

        public Composite(DataType<T> dataType, StreamInput in) throws IOException {
            super(dataType, in);
            this.mostCommonValuesSketch = new MostCommonValuesSketch<>(BYTE_ARRAY_STREAMER, in);
        }

        @Override
        protected void writeSketches(StreamOutput out) throws IOException {
            this.mostCommonValuesSketch.writeTo(out);
        }

        @Override
        protected void updateSketches(T value) {
            try {
                scratch.reset();
                dataType.streamer().writeValueTo(scratch, value);
                // TODO update DataSketches to take byte, offset, length
                BytesRef bv = scratch.copyBytes().toBytesRef();
                mostCommonValuesSketch.update(bv);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public ColumnSketchBuilder<T> merge(ColumnSketchBuilder<?> other) {
            if (Objects.equals(this.dataType, other.dataType) == false) {
                throw new IllegalArgumentException("Columns must be of the same data type");
            }

            final ColumnSketchBuilder.Composite<T> typedOther = (ColumnSketchBuilder.Composite<T>) other;
            this.sampleCount += other.sampleCount;
            this.nullCount += other.nullCount;
            this.totalBytes += other.totalBytes;
            this.distinctSketch = this.distinctSketch.merge(other.distinctSketch);
            this.distinctCountsIncomplete = this.distinctCountsIncomplete || typedOther.distinctCountsIncomplete;
            typedOther.distinctCounts.forEach((key, count) -> this.distinctCounts.merge(key, count, Long::sum));
            this.mostCommonValuesSketch = this.mostCommonValuesSketch.merge(typedOther.mostCommonValuesSketch);

            return this;
        }

        @Override
        public ColumnStats<T> toStats(long totalDocs) {
            double nullFraction = nullFraction();
            double avgSizeInBytes = (double) totalBytes / ((double) sampleCount - nullCount);
            double approxDistinct = estimateDistinct(totalDocs);
            MostCommonValues<T> mcv =
                this.mostCommonValuesSketch.toMostCommonValues(sampleCount, approxDistinct, this::fromByteStream);
            return new ColumnStats<>(
                nullFraction,
                avgSizeInBytes,
                approxDistinct,
                dataType,
                mcv,
                List.of()
            );
        }

        private T fromByteStream(BytesRef bytes) {
            try {
                return dataType.streamer().readValueFrom(StreamInput.wrap(bytes.bytes, bytes.offset, bytes.length));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static final Streamer<BytesRef> BYTE_ARRAY_STREAMER = new Streamer<>() {
        @Override
        public BytesRef readValueFrom(StreamInput in) throws IOException {
            byte[] b = in.readByteArray();
            return new BytesRef(b);
        }

        @Override
        public void writeValueTo(StreamOutput out, BytesRef v) throws IOException {
            out.writeVInt(v.length);
            out.writeBytes(v.bytes, v.offset, v.length);
        }
    };

}
