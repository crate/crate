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
import java.nio.ByteBuffer;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;

class TDigestState extends MergingDigest {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TDigestState.class);

    public static final double DEFAULT_COMPRESSION = 200.0;

    private double[] fractions;

    private int lastByteSize = 0;

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

    int addGetSizeDelta(double value) {
        add(value);
        int newByteSize = byteSize();
        int delta = newByteSize - lastByteSize;
        lastByteSize = newByteSize;
        return delta;
    }


    public static void write(TDigestState state, StreamOutput out) throws IOException {
        if (out.getVersion().onOrBefore(Version.V_6_1_1)) {
            out.writeDouble(state.compression());
            out.writeDoubleArray(state.fractions);
            out.writeVInt(state.centroidCount());
            for (Centroid centroid : state.centroids()) {
                out.writeDouble(centroid.mean());
                out.writeVLong(centroid.count());
            }
        } else {
            out.writeDoubleArray(state.fractions);

            byte[] bytes = new byte[state.byteSize()];
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            state.asBytes(buf);

            out.writeVInt(bytes.length);
            out.writeBytes(bytes, 0, bytes.length);
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        if (in.getVersion().onOrBefore(Version.V_6_1_1)) {
            double compression = in.readDouble();
            double[] fractions = in.readDoubleArray();
            TDigestState state = new TDigestState(compression, fractions);
            int n = in.readVInt();
            for (int i = 0; i < n; i++) {
                state.add(in.readDouble(), in.readVInt());
            }
            return state;
        } else {
            double[] factions = in.readDoubleArray();

            int len = in.readVInt();
            byte[] bytes = new byte[len];
            in.readBytes(bytes, 0, len);

            // MergingDigest.fromBytes(...) restores the digest exactly, including its centroids.
            // Calling tDigestState.add(mergingDigest) re-merges it, which may change the centroids.
            // This is fine because t-digest instances with different centroids are equivalent
            // as long as their percentile estimates remain close.
            MergingDigest mergingDigest = MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
            TDigestState tDigestState = new TDigestState(mergingDigest.compression(), factions);
            tDigestState.add(mergingDigest);

            return tDigestState;
        }
    }
}
