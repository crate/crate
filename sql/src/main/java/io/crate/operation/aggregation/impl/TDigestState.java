/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.aggregation.impl;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

class TDigestState extends AVLTreeDigest {

    private final double compression;
    private Double[] fractions;

    TDigestState(double compression, Double[] fractions) {
        super(compression);
        this.compression = compression;
        this.fractions = fractions;
    }

    @Override
    public double compression() {
        return compression;
    }

    Double[] fractions() {
        return fractions;
    }

    void fractions(Double[] fractions) {
        this.fractions = fractions;
    }

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression);

        int n = state.fractions().length;
        out.writeVInt(n);
        for (int i = 0; i < n; i++) {
            Double fraction = state.fractions()[i];
            out.writeBoolean(fraction != null);
            if (fraction != null) {
                out.writeDouble(fraction);
            }
        }

        out.writeVInt(state.centroidCount());
        for (Centroid centroid : state.centroids()) {
            out.writeDouble(centroid.mean());
            out.writeVLong(centroid.count());
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        double compression = in.readDouble();

        int n = in.readVInt();
        Double[] fractions = new Double[n];
        for (int i = 0; i < n; i++) {
            fractions[i] = in.readBoolean() ? in.readDouble(): null;
        }
        TDigestState state = new TDigestState(compression, fractions);
        n = in.readVInt();
        for (int i = 0; i < n; i++) {
            state.add(in.readDouble(), in.readVInt());
        }
        return state;
    }

    @Override
    public void add(TDigest other) {
        super.add(other);
        TDigestState tDigestState = (TDigestState) other;
        fractions = tDigestState.fractions();
    }
}
