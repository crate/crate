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

    /**
     * The streamed partial state is always two IEEE-754 doubles plus a count, but the meaning of
     * the two doubles depends on {@link #legacy}:
     * <ul>
     *   <li>legacy  (pre-6.5 wire format): {@code d1} = Σx² (sum of squares), {@code d2} = Σx (sum)</li>
     *   <li>Welford (6.5+ wire format):    {@code d1} = running mean,         {@code d2} = M2 = Σ(x-mean)²</li>
     * </ul>
     * Welford's online algorithm never squares the raw values, so it avoids the catastrophic
     * cancellation that produced a negative variance (and NULL standard deviations) for
     * large-magnitude inputs. See crate/crate#19760.
     * <p>
     * The naive layout is kept as a rolling-upgrade fallback: as long as any node in the cluster is
     * older than 6.5.0 the partial state is streamed in the legacy layout so that pre-6.5 nodes
     * (which only understand the naive layout) keep interoperating. The mode is chosen cluster-wide
     * at plan time from the min node version and mirrored by {@code newState}, so both accumulators
     * are never mixed within a single query. See PR #19885.
     */
    private final boolean legacy;
    private double d1;
    private double d2;
    private long count;

    public Variance() {
        this(false);
    }

    public Variance(boolean legacy) {
        this.legacy = legacy;
        this.d1 = 0.0;
        this.d2 = 0.0;
        this.count = 0;
    }

    public Variance(StreamInput in, boolean legacy) throws IOException {
        this.legacy = legacy;
        this.d1 = in.readDouble();
        this.d2 = in.readDouble();
        this.count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(d1);
        out.writeDouble(d2);
        out.writeVLong(count);
    }

    protected long count() {
        return count;
    }

    public boolean isLegacy() {
        return legacy;
    }

    public Variance increment(double value) {
        if (legacy) {
            d1 += value * value;
            d2 += value;
            count++;
        } else {
            count++;
            double delta = value - d1;
            d1 += delta / count;
            d2 += delta * (value - d1);
        }
        return this;
    }

    public void decrement(double value) {
        if (legacy) {
            d1 -= value * value;
            d2 -= value;
            count--;
            return;
        }
        if (count == 1) {
            count = 0;
            d1 = 0.0;
            d2 = 0.0;
            return;
        }
        double meanPrev = (count * d1 - value) / (count - 1);
        d2 -= (value - d1) * (value - meanPrev);
        d1 = meanPrev;
        count--;
    }

    public double result() {
        if (count == 0) {
            return Double.NaN;
        }
        if (legacy) {
            // The naive sum-of-squares formula can produce a small negative result for
            // large-magnitude inputs with little spread (e.g. a constant DATE/TIMESTAMP) due to
            // floating point cancellation. A variance is never negative, so clamp it; otherwise
            // sqrt() in the stddev variants would yield NaN (returned to the user as NULL).
            double variance = (d1 - ((d2 * d2) / count)) / count;
            return variance < 0 ? 0 : variance;
        }
        // d2 (M2) is non-negative by construction under forward accumulation; the clamp guards
        // against tiny negative residues from decrement() in removable window frames.
        return Math.max(d2, 0.0) / count;
    }

    public void merge(Variance other) {
        assert legacy == other.legacy
            : "Cannot merge a legacy and a Welford variance state; the mode must be chosen "
              + "consistently cluster-wide at plan time";
        if (legacy) {
            d1 += other.d1;
            d2 += other.d2;
            count += other.count;
            return;
        }
        if (other.count == 0) {
            return;
        }
        if (count == 0) {
            d1 = other.d1;
            d2 = other.d2;
            count = other.count;
            return;
        }
        long newCount = count + other.count;
        double delta = other.d1 - d1;
        d2 = d2 + other.d2 + delta * delta * count * other.count / newCount;
        d1 = d1 + delta * other.count / newCount;
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
