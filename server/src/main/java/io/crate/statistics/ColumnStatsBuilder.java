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
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.data.breaker.RamAccounting;
import io.crate.types.DataType;

public class ColumnStatsBuilder<T> {

    private final DataType<T> dataType;
    private final List<T> samples = new ArrayList<>();

    private long sampleCount;
    private int nullCount;
    private long totalBytes;

    public ColumnStatsBuilder(DataType<T> dataType) {
        this.dataType = dataType;
    }

    public ColumnStatsBuilder(DataType<T> dataType, StreamInput in) throws IOException {
        this.dataType = dataType;
        this.sampleCount = in.readLong();
        this.nullCount = in.readInt();
        this.totalBytes = in.readLong();
        int valueCount = in.readInt();
        final Streamer<T> streamer = dataType.streamer();
        for (int i = 0; i < valueCount; i++) {
            samples.add(streamer.readValueFrom(in));
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(this.sampleCount);
        out.writeInt(this.nullCount);
        out.writeLong(this.totalBytes);
        out.writeInt(this.samples.size());
        final Streamer<T> streamer = dataType.streamer();
        for (int i = 0; i < samples.size(); i++) {
            streamer.writeValueTo(out, samples.get(i));
        }
    }

    public void add(T value, RamAccounting ramAccounting) {
        sampleCount++;
        if (value == null) {
            nullCount++;
        } else {
            totalBytes += dataType.valueBytes(value) + Long.BYTES;
            ramAccounting.addBytes(totalBytes);
            samples.add(value);
        }
    }

    public ColumnStats<T> toColumnStats() {
        samples.sort(dataType);
        return ColumnStats.fromSortedValues(samples, dataType, nullCount, sampleCount);
    }

    /**
     * Merges two builders together.  If the combined sample count of both builders is greater
     * than {@code maxSampleCount} then a sampled subset of both builders are used.  This
     * method may mutate this builder.
     */
    @SuppressWarnings("unchecked")
    public ColumnStatsBuilder<T> merge(Random random, int maxSampleCount, ColumnStatsBuilder<?> other) {
        if (Objects.equals(this.dataType, other.dataType) == false) {
            throw new IllegalArgumentException(
                "Cannot merge column stats from different datatypes [" + this.dataType + ", " + other.dataType + "]");
        }
        ColumnStatsBuilder<T> typedOther = (ColumnStatsBuilder<T>) other;
        if (this.sampleCount == 0) {
            return typedOther;
        }
        if (other.sampleCount == 0) {
            return this;
        }
        if (this.sampleCount + other.sampleCount < maxSampleCount) {
            for (T value : typedOther.samples) {
                this.add(value, RamAccounting.NO_ACCOUNTING);
            }
            this.sampleCount += other.nullCount;
            this.nullCount += other.nullCount;
            return this;
        }
        ColumnStatsBuilder<T> mergedStats = new ColumnStatsBuilder<>(this.dataType);
        Sampler<T> sampler = new Sampler<>(random, this.samples, typedOther.samples);
        for (int i = 0; i < maxSampleCount; i++) {
            mergedStats.add(sampler.next(), RamAccounting.NO_ACCOUNTING);
        }
        mergedStats.nullCount = this.nullCount + other.nullCount;
        mergedStats.sampleCount = this.sampleCount + other.sampleCount;
        return mergedStats;
    }

    // https://ballsandbins.wordpress.com/2014/04/13/distributedparallel-reservoir-sampling/
    private static class Sampler<T> {
        final Random random;
        final List<T> left;
        final List<T> right;
        final double pivot;

        private Sampler(Random random, List<T> left, List<T> right) {
            this.random = random;
            this.left = left;
            this.right = right;
            this.pivot = (double) right.size() / (right.size() + left.size());
        }

        T next() {
            double j = random.nextDouble();
            return j <= pivot ?
                left.get(random.nextInt(left.size())) :
                right.get(random.nextInt(right.size()));
        }
    }

}
