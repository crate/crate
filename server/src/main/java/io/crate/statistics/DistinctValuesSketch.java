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
import java.nio.charset.StandardCharsets;

import org.apache.datasketches.common.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;

/**
 * A streamable count of the hashes of all sampled values
 */
public final class DistinctValuesSketch {

    private static final int INITIAL_CAPACITY = 16;
    private static final double LOAD_FACTOR = 15.0 / 16.0;

    private final LongIntHashMap countsByHash;

    private DistinctValuesSketch(LongIntHashMap countsByHash) {
        this.countsByHash = countsByHash;
    }

    /**
     * Creates a new empty sketch
     */
    public static DistinctValuesSketch newSketch() {
        return new DistinctValuesSketch(new LongIntHashMap(INITIAL_CAPACITY, LOAD_FACTOR));
    }

    /**
     * Add a new value to the sketch
     */
    public void update(String v) {
        if (v.isEmpty()) {
            return;
        }
        countsByHash.addTo(MurmurHash3.hash(v.getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0], 1);
    }

    /**
     * Merge another sketch into this one
     */
    public DistinctValuesSketch merge(DistinctValuesSketch other) {
        for (LongIntCursor cursor : other.countsByHash) {
            countsByHash.addTo(cursor.key, cursor.value);
        }
        return this;
    }

    /**
     * Number of distinct values
     */
    public double getEstimate() {
        return countsByHash.size();
    }

    /**
     * Number of distinct values that were added exactly once
     */
    public double singletons() {
        int singletons = 0;
        for (LongIntCursor cursor : countsByHash) {
            if (cursor.value == 1) {
                singletons++;
            }
        }
        return singletons;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(countsByHash.size());
        for (LongIntCursor cursor : countsByHash) {
            out.writeLong(cursor.key);
            out.writeVInt(cursor.value);
        }
    }

    /**
     * Reads a sketch from a StreamInput
     */
    public static DistinctValuesSketch fromStream(StreamInput in) throws IOException {
        int size = in.readVInt();
        LongIntHashMap countsByHash = new LongIntHashMap(size, LOAD_FACTOR);
        for (int i = 0; i < size; i++) {
            countsByHash.put(in.readLong(), in.readVInt());
        }
        return new DistinctValuesSketch(countsByHash);
    }
}
