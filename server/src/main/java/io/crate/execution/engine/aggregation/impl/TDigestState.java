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

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;

class TDigestState extends MergingDigest {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TDigestState.class);

    public static final double DEFAULT_COMPRESSION = 200.0;

    private double[] fractions;

    private long mergingDigestState = 0;

    TDigestState(double compression, double[] fractions) {
        // mergingDigestState relies on the fact that only single argument ctor of the parent class is used.
        // If we change it, we need to update its implementation.
        super(compression);
        mergingDigestState = mergingDigestState(compression, -1, -1);
        this.fractions = fractions;
    }

    /**
     * Accounts for arrays of the parent class MergingDigest.
     * As stated in its javadocs, no allocation is required after initialization,
     * so we can mirror sizes of arrays allocated for the given compression in the ctor.
     * Note, that data and tempData are always null, as we don't use recordAllData().
     */
    private static long mergingDigestState(double compression, int bufferSize, int size) {
        if (compression < 10) {
            compression = 10;
        }

        double sizeFudge = 0;
        // Reference code has if (useWeightLimit) check that is always true here.
        sizeFudge = 10;
        if (compression < 30) {
            sizeFudge += 20;
        }

        size = (int) Math.max(2 * compression + sizeFudge, size);

        // Reference code has if (bufferSize == -1) check that is always true here.
        bufferSize = 5 * size;

        if (bufferSize <= 2 * size) {
            bufferSize = 2 * size;
        }

        // Reference code resets scale to 1 if (!useTwoLevelCompression), but can't happen here.
        double scale = Math.max(1, bufferSize / size - 1);

        double publicCompression = compression;
        compression = Math.sqrt(scale) * publicCompression;

        // changing the compression could cause buffers to be too small, readjust if so
        if (size < compression + sizeFudge) {
            size = (int) Math.ceil(compression + sizeFudge);
        }

        if (bufferSize <= 2 * size) {
            bufferSize = 2 * size;
        }

        /**
        weight = new double[size];
        mean = new double[size];

        tempWeight = new double[bufferSize];
        tempMean = new double[bufferSize];
        order = new int[bufferSize];
        **/

        long weightAndMeanSize = 2 * (NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * size);
        long tempArraysSize = 2 * (NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * bufferSize);
        long orderSize = NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * bufferSize;
        return alignObjectSize(weightAndMeanSize + tempArraysSize + orderSize);
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
        Collection<Centroid> centroids = state.centroids();
        out.writeVInt(state.centroidCount());
        for (Centroid centroid : centroids) {
            out.writeDouble(centroid.mean());
            out.writeVInt(centroid.count());
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

    /**
     * Must be accounted only once per state.
     */
    public long initialSize() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(fractions) + mergingDigestState;
    }
}
