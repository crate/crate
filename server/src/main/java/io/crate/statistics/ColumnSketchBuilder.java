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
import java.util.List;
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
    /// Deprecated, see [#20172](https://github.com/crate/crate/pull/20172) for details.
    @Deprecated(since = "6.5.1")
    protected DistinctValuesSketch distinctSketch;

    /// Used to count the number of distinct values generally
    /// and the number of distinct values appearing exactly once in a sample.
    protected final DistinctValueSet distinctCounts;

    /// Set when [#distinctCounts] does not (or may not) cover the whole sample,
    /// because this instance was:
    /// * deserialized from a node older than 6.4.5
    /// * an instance coming from a node older than 6.4.5 was merged into this instance.
    ///
    /// When true, we can't use [#distinctCounts] to provide a better estimate of the
    /// number of distinct values, so [#estimateDistinct] falls back to [#distinctSketch]
    /// (instead of extrapolating from incomplete [#distinctCounts]).
    protected boolean distinctCountsIncomplete;

    /**
     * Creates a new ColumnSketchBuilder for the given DataType
     */
    public ColumnSketchBuilder(DataType<T> dataType) {
        this.dataType = dataType;
        this.distinctSketch = DistinctValuesSketch.newSketch();
        // Pre-sized to the reservoir's upper bound so that we have no re-sizing (and re-hashing)
        // when counting distinct values on a single node.
        this.distinctCounts = new DistinctValueSet();
    }

    public ColumnSketchBuilder(DataType<T> dataType, StreamInput in) throws IOException {
        this.dataType = dataType;
        this.sampleCount = in.readLong();
        this.nullCount = in.readLong();
        this.totalBytes = in.readLong();
        this.distinctSketch = DistinctValuesSketch.fromStream(in);
        if (in.getVersion().after(Version.V_6_5_0)) {
            this.distinctCounts = DistinctValueSet.fromStream(in);
            this.distinctCountsIncomplete = in.readBoolean();
        } else {
            this.distinctCounts = new DistinctValueSet();
            this.distinctCountsIncomplete = true;
        }
    }

    public final void writeTo(StreamOutput out) throws IOException {
        out.writeLong(sampleCount);
        out.writeLong(nullCount);
        out.writeLong(totalBytes);
        out.writeByteArray(distinctSketch.getSketch().toByteArray());
        if (out.getVersion().after(Version.V_6_5_0)) {
            distinctCounts.writeTo(out);
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
            distinctCounts.update(key);
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
    /// Haas-Stokes estimator.
    /// Falls back to [#distinctSketch]'s own (non-extrapolated) estimate when
    /// [#distinctCountsIncomplete] is set, since [#distinctCounts] can't be trusted in that case.
    final double estimateDistinct(long totalDocs) {
        if (distinctCountsIncomplete) {
            return distinctSketch.getSketch().getEstimate();
        }

        return estimateWithHaasStokes(totalDocs);
    }

    private double estimateWithHaasStokes(long totalRows) {
        double distinctSamples = distinctCounts.size();
        double numValuesWithDuplicates = distinctCounts.size() - distinctCounts.singletons();
        double samplesWithoutNulls = sampleCount - nullCount;

        if (numValuesWithDuplicates == 0) {
            // All sample values are unique. We assume it is a unique column, but we need to discount for null values.
            return (1.0 - nullFraction()) * totalRows;
        }
        if (numValuesWithDuplicates == distinctSamples) {
            // Every value appeared more than once. Assume the column has just these values.
            // This should address cases like boolean or enum columns, where there is a small fixed set of possible values.
            return distinctSamples;
        }

        // Estimate the number of distinct values using the estimator
        // proposed by Haas and Stokes in IBM Research Report RJ 10025:
        // ```
        // n*d / (n - f1 + f1*n/N)
        // ```
        // where:
        // * `n` is the (non-null) sample size
        // * `d` the distinct count in the sample
        // * `f1` the count of values seen exactly once in the sample
        // * `N` the total count of non-null values
        //
        // This is their Duj1 estimator; the other estimators they
        // recommend are considerably more complex, and are numerically
        // very unstable when n is much smaller than N.
        //
        // Also see PostgreSQL's [compute_distinct_stats](https://github.com/postgres/postgres/blob/9f4bd91a1960f0263c497a9a1ddf121cbcefbb98/src/backend/commands/analyze.c#L2121).
        double f1 = distinctSamples - numValuesWithDuplicates;
        double d = f1 + numValuesWithDuplicates;
        //noinspection UnnecessaryLocalVariable
        double n = samplesWithoutNulls;
        double N = totalRows * (1.0 - nullFraction());
        double approxDistinct;
        if (N > 0) {
            approxDistinct = (n * d) / ((n - f1) + f1 * n / N);
        } else {
            approxDistinct = 0;
        }
        /* Clamp to sane range in case of round-off error */
        if (approxDistinct < d) {
            approxDistinct = d;
        }
        if (approxDistinct > N) {
            approxDistinct = N;
        }
        return Math.floor(approxDistinct + 0.5);
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
            this.distinctCounts.merge(typedOther.distinctCounts);
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
            this.distinctCounts.merge(typedOther.distinctCounts);
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
