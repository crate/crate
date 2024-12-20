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

package io.crate.execution.engine.aggregation.impl;

import java.io.IOException;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;

class TDigestState extends AVLTreeDigest {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TDigestState.class);

    public static final double DEFAULT_COMPRESSION = 100.0;

    private double[] fractions;

    TDigestState(double compression, double[] fractions) {
        super(compression);
        this.fractions = fractions;
    }

    static TDigestState createEmptyState() {
        return new TDigestState(DEFAULT_COMPRESSION, new double[]{});
    }

    boolean isEmpty() {
        return fractions.length == 0;
    }

    double[] fractions() {
        return fractions;
    }

    void fractions(double[] fractions) {
        this.fractions = fractions;
    }

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression());
        out.writeDoubleArray(state.fractions);
        out.writeVInt(state.centroidCount());
        for (Centroid centroid : state.centroids()) {
            out.writeDouble(centroid.mean());
            out.writeVLong(centroid.count());
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        double compression = in.readDouble();
        double[] fractions = in.readDoubleArray();
        TDigestState state = new TDigestState(compression, fractions);
        int n = in.readVInt();
        for (int i = 0; i < n; i++) {
            state.add(in.readDouble(), in.readVInt());
        }
        return state;
    }
}
