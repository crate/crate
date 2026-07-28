/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.aggregation.statistics;

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public class Variance implements Writeable, Comparable<Variance> {

    public static final int FIXED_SIZE = 3 * 64; // 2 * double vars + 1 long var

    public static int fixedSize() {
        return FIXED_SIZE;
    }

    private double mean;
    private double m2;
    private long count;

    public Variance() {
        mean = 0.0;
        m2 = 0.0;
        count = 0;
    }

    public Variance(StreamInput in) throws IOException {
        mean = in.readDouble();
        m2 = in.readDouble();
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(mean);
        out.writeDouble(m2);
        out.writeVLong(count);
    }

    protected long count() {
        return count;
    }

    /**
     * Welford's online algorithm. Unlike the naive {@code sum(x^2) - sum(x)^2/n} formula it does
     * not square the raw values, so it avoids the catastrophic cancellation that produced a
     * negative variance (and therefore {@code NULL} standard deviations) for large-magnitude
     * inputs such as a constant {@code DATE}/{@code TIMESTAMP}. See crate/crate#19760.
     */
    public Variance increment(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
        return this;
    }

    public void decrement(double value) {
        if (count == 1) {
            count = 0;
            mean = 0.0;
            m2 = 0.0;
            return;
        }
        double meanPrev = (count * mean - value) / (count - 1);
        m2 -= (value - mean) * (value - meanPrev);
        mean = meanPrev;
        count--;
    }

    public double result() {
        if (count == 0) {
            return Double.NaN;
        }
        // m2 is non-negative by construction; clamp guards against tiny negative residues that
        // can appear after a long sequence of decrements in sliding window frames.
        return Math.max(m2, 0.0) / count;
    }

    public void merge(Variance other) {
        if (other.count == 0) {
            return;
        }
        if (count == 0) {
            mean = other.mean;
            m2 = other.m2;
            count = other.count;
            return;
        }
        long newCount = count + other.count;
        double delta = other.mean - mean;
        m2 = m2 + other.m2 + delta * delta * count * other.count / newCount;
        mean = mean + delta * other.count / newCount;
        count = newCount;
    }

    @Override
    public int compareTo(Variance o) {
        return Double.compare(result(), o.result());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Variance variance = (Variance) o;
        return Objects.equals(variance.result(), result());
    }

    @Override
    public int hashCode() {
        return Objects.hash(result());
    }
}
