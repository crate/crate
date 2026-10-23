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

import org.apache.datasketches.common.QuickSelect;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.thetacommon.HashOperations;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/**
 * A streamable representation of a distinct values sketch.
 * <p>
 * Works like a Theta QuickSelect sketch: it retains the hashes below theta and estimates the number of distinct
 * values as {@code retained / theta}. The lowest bit of each retained hash is used as a flag that is set once the
 * value has been seen more than once, so the sketch can also estimate the number of values seen exactly once.
 * Distinct hashes differ in their higher bits, so the flag doesn't change their order.
 */
public final class DistinctValuesSketch {

    private static final int NOMINAL_ENTRIES = 4096;
    private static final int LG_MIN_CAPACITY = 5;
    private static final int LG_MAX_CAPACITY = 13;
    private static final double RESIZE_THRESHOLD = 0.5;
    private static final double REBUILD_THRESHOLD = 15.0 / 16.0;
    private static final long DUPLICATE_FLAG = 1L;
    private static final long KEY_MASK = ~DUPLICATE_FLAG;

    private long[] table;
    private int lgCapacity;
    private int count;
    private long theta;

    private DistinctValuesSketch(int lgCapacity, long theta) {
        this.table = new long[1 << lgCapacity];
        this.lgCapacity = lgCapacity;
        this.theta = theta;
    }

    /**
     * Creates a new empty sketch
     */
    public static DistinctValuesSketch newSketch() {
        return new DistinctValuesSketch(LG_MIN_CAPACITY, Long.MAX_VALUE);
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
        if (other.theta < theta) {
            theta = other.theta;
            rebuild(lgCapacity);
        }
        for (long entry : other.table) {
            if (entry != 0) {
                add(entry & KEY_MASK, entry & DUPLICATE_FLAG);
            }
        }
        return this;
    }

    /**
     * Estimated number of distinct values
     */
    public double getEstimate() {
        return count / thetaFraction();
    }

    /**
     * Estimated number of distinct values that were added exactly once
     */
    public double singletons() {
        int retainedSingletons = 0;
        for (long entry : table) {
            if (entry != 0 && (entry & DUPLICATE_FLAG) == 0) {
                retainedSingletons++;
            }
        }
        return retainedSingletons / thetaFraction();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(theta);
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
        long theta = in.readLong();
        int count = in.readVInt();
        int lgCapacity = LG_MIN_CAPACITY;
        while (lgCapacity < LG_MAX_CAPACITY && count > RESIZE_THRESHOLD * (1 << lgCapacity)) {
            lgCapacity++;
        }
        DistinctValuesSketch sketch = new DistinctValuesSketch(lgCapacity, theta);
        for (int i = 0; i < count; i++) {
            sketch.insert(in.readLong());
        }
        return sketch;
    }

    private double thetaFraction() {
        return (double) theta / Long.MAX_VALUE;
    }

    /**
     * Inserts the key, or flags it as duplicate if it is already present.
     * A new key keeps the given flag, which is set if the key was already seen more than once in another sketch.
     */
    private void add(long key, long flag) {
        if (key == 0 || key >= theta) {
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
                if (lgCapacity < LG_MAX_CAPACITY) {
                    if (count > RESIZE_THRESHOLD * table.length) {
                        rebuild(lgCapacity + 1);
                    }
                } else if (count > REBUILD_THRESHOLD * table.length) {
                    theta = QuickSelect.selectExcludingZeros(table, count, NOMINAL_ENTRIES + 1) & KEY_MASK;
                    rebuild(lgCapacity);
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

    /**
     * Re-inserts all entries below theta into a new table
     */
    private void rebuild(int newLgCapacity) {
        long[] oldTable = table;
        table = new long[1 << newLgCapacity];
        lgCapacity = newLgCapacity;
        count = 0;
        for (long entry : oldTable) {
            if (entry != 0 && (entry & KEY_MASK) < theta) {
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
