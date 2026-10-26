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

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;

/**
 * A streamable set of the hashes of all sampled values.
 * <p>
 * The lowest bit of each hash is used as a flag that is set once the value has been seen more than once,
 * so the set provides the exact number of distinct values and of values seen exactly once in the sample.
 */
public final class DistinctValuesSketch {

    private static final double LOAD_FACTOR = 15.0 / 16.0;
    private static final long DUPLICATE_FLAG = 1L;
    private static final long KEY_MASK = ~DUPLICATE_FLAG;

    private final LongHashSet hashes;

    private DistinctValuesSketch(LongHashSet hashes) {
        this.hashes = hashes;
    }

    /**
     * Creates a new empty sketch
     */
    public static DistinctValuesSketch newSketch() {
        return new DistinctValuesSketch(new LongHashSet(Reservoir.NUM_SAMPLES, LOAD_FACTOR));
    }

    /**
     * Add a new value to the sketch
     */
    public void update(String v) {
        if (v.isEmpty()) {
            return;
        }
        long key = MurmurHash3.hash(v.getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0] & KEY_MASK;
        add(key);
    }

    /**
     * Merge another sketch into this one
     */
    public DistinctValuesSketch merge(DistinctValuesSketch other) {
        for (LongCursor cursor : other.hashes) {
            add(cursor.value);
        }
        return this;
    }

    /**
     * Number of distinct values
     */
    public double getEstimate() {
        return hashes.size();
    }

    /**
     * Number of distinct values that were added exactly once
     */
    public double singletons() {
        int singletons = 0;
        for (LongCursor cursor : hashes) {
            if ((cursor.value & DUPLICATE_FLAG) == 0) {
                singletons++;
            }
        }
        return singletons;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(hashes.size());
        for (LongCursor cursor : hashes) {
            out.writeLong(cursor.value);
        }
    }

    /**
     * Reads a sketch from a StreamInput
     */
    public static DistinctValuesSketch fromStream(StreamInput in) throws IOException {
        int size = in.readVInt();
        LongHashSet hashes = new LongHashSet(size, LOAD_FACTOR);
        for (int i = 0; i < size; i++) {
            hashes.add(in.readLong());
        }
        return new DistinctValuesSketch(hashes);
    }

    /**
     * Adds an entry, or flags its key as duplicate if the key is already present.
     * A new key keeps the entry's flag, which is set if the key was already seen more than once in another sketch.
     */
    private void add(long entry) {
        long key = entry & KEY_MASK;
        long duplicate = key | DUPLICATE_FLAG;
        if (hashes.contains(duplicate)) {
            return;
        }
        if (hashes.remove(key)) {
            hashes.add(duplicate);
        } else {
            hashes.add(entry);
        }
    }
}
