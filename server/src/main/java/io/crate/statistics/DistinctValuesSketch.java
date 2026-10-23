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
import org.apache.datasketches.thetacommon.HashOperations;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/**
 * A streamable set of the hashes of all sampled values.
 * <p>
 * The lowest bit of each hash is used as a flag that is set once the value has been seen more than once,
 * so the set provides the exact number of distinct values and of values seen exactly once in the sample.
 */
public final class DistinctValuesSketch {

    private static final int LG_MIN_CAPACITY = 5;
    private static final double RESIZE_THRESHOLD = 15.0 / 16.0;
    private static final long DUPLICATE_FLAG = 1L;
    private static final long KEY_MASK = ~DUPLICATE_FLAG;

    private long[] table;
    private int lgCapacity;
    private int count;

    private DistinctValuesSketch(int lgCapacity) {
        this.table = new long[1 << lgCapacity];
        this.lgCapacity = lgCapacity;
    }

    /**
     * Creates a new empty sketch
     */
    public static DistinctValuesSketch newSketch() {
        return new DistinctValuesSketch(LG_MIN_CAPACITY);
    }

    /**
     * Add a new value to the sketch
     */
    public void update(String v) {
        if (v.isEmpty()) {
            return;
        }
        long key = (MurmurHash3.hash(v.getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0] >>> 1) & KEY_MASK;
        add(key, 0L);
    }

    /**
     * Merge another sketch into this one
     */
    public DistinctValuesSketch merge(DistinctValuesSketch other) {
        for (long entry : other.table) {
            if (entry != 0) {
                add(entry & KEY_MASK, entry & DUPLICATE_FLAG);
            }
        }
        return this;
    }

    /**
     * Number of distinct values
     */
    public double getEstimate() {
        return count;
    }

    /**
     * Number of distinct values that were added exactly once
     */
    public double singletons() {
        int singletons = 0;
        for (long entry : table) {
            if (entry != 0 && (entry & DUPLICATE_FLAG) == 0) {
                singletons++;
            }
        }
        return singletons;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(count);
        for (long entry : table) {
            if (entry != 0) {
                out.writeLong(entry);
            }
        }
    }

    /**
     * Reads a sketch from a StreamInput
     */
    public static DistinctValuesSketch fromStream(StreamInput in) throws IOException {
        int count = in.readVInt();
        int lgCapacity = LG_MIN_CAPACITY;
        while (count > RESIZE_THRESHOLD * (1 << lgCapacity)) {
            lgCapacity++;
        }
        DistinctValuesSketch sketch = new DistinctValuesSketch(lgCapacity);
        for (int i = 0; i < count; i++) {
            sketch.insert(in.readLong());
        }
        return sketch;
    }

    /**
     * Inserts the key, or flags it as duplicate if it is already present.
     * A new key keeps the given flag, which is set if the key was already seen more than once in another sketch.
     */
    private void add(long key, long flag) {
        if (key == 0) {
            return;
        }
        int mask = table.length - 1;
        long hash = key >>> 1;
        int index = (int) hash & mask;
        int stride = 2 * (int) ((hash >>> lgCapacity) & HashOperations.STRIDE_MASK) + 1;
        while (true) {
            long entry = table[index];
            if (entry == 0) {
                table[index] = key | flag;
                count++;
                if (count > RESIZE_THRESHOLD * table.length) {
                    resize(lgCapacity + 1);
                }
                return;
            }
            if ((entry & KEY_MASK) == key) {
                table[index] = entry | DUPLICATE_FLAG;
                return;
            }
            index = (index + stride) & mask;
        }
    }

    private void resize(int newLgCapacity) {
        long[] oldTable = table;
        table = new long[1 << newLgCapacity];
        lgCapacity = newLgCapacity;
        count = 0;
        for (long entry : oldTable) {
            if (entry != 0) {
                insert(entry);
            }
        }
    }

    /**
     * Inserts an entry whose key is known to be absent, without resizing
     */
    private void insert(long entry) {
        int mask = table.length - 1;
        long hash = (entry & KEY_MASK) >>> 1;
        int index = (int) hash & mask;
        int stride = 2 * (int) ((hash >>> lgCapacity) & HashOperations.STRIDE_MASK) + 1;
        while (table[index] != 0) {
            index = (index + stride) & mask;
        }
        table[index] = entry;
        count++;
    }
}
