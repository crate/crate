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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.carrotsearch.hppc.BitMixer;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;

// TODO
public final class DistinctValueSet {

    // For hashes that represent duplicate values, the last bit is set to 1.
    private static final long DUPLICATE_FLAG = 1L;
    // For hashes that represent singletons, the last bit is set to 0.
    private static final long KEY_MASK = ~DUPLICATE_FLAG;

    // We balance between:
    // * size of the LongHashSet (increases memory footprint),
    // * number of retained hashes (increases accuracy), and
    // * load factor (increases lookup time).
    // The values below keep the performance and memory footprint
    // at similar level as the DistinctValuesSketch that was used previously.
    private static final double LOAD_FACTOR = 15.0 / 16.0;
    private static final int MAX_RETAINED = (int) (8192 * LOAD_FACTOR);
    // A hash can have between 0 and 64 leading zeros.
    private static final int NUM_LEVELS = Long.SIZE + 1;
    // value ^ HASH_SEED: this is done to make sure 0 is properly removed from a "level".
    // 0 would be level 64, i.e. it would never be dropped from the hash.
    // Whatever level we finish at, 0 would be in it. At that level,
    // the number of distinct values would be estimated as 2^level * num_values_at_level,
    // which would likely be far from the actual NDV.
    private static final long HASH_SEED = 0x9E3779B97F4A7C15L;

    private final LongHashSet hashes;
    private int level;
    // The number of added rows by the level of their hash, including the rows of dropped hashes.
    private final long[] rowsByLevel;

    DistinctValueSet() {
        this(new LongHashSet(MAX_RETAINED, LOAD_FACTOR), new long[NUM_LEVELS], 0);
    }

    private DistinctValueSet(LongHashSet hashes, long[] rowsByLevel, int level) {
        this.hashes = hashes;
        this.rowsByLevel = rowsByLevel;
        this.level = level;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(level);
        for (long rows : rowsByLevel) {
            out.writeVLong(rows);
        }
        out.writeVInt(hashes.size());
        for (LongCursor cursor : hashes) {
            out.writeLong(cursor.value);
        }
    }

    public static DistinctValueSet fromStream(StreamInput in) throws IOException {
        int level = in.readVInt();
        long[] rowsByLevel = new long[NUM_LEVELS];
        for (int i = 0; i < NUM_LEVELS; i++) {
            rowsByLevel[i] = in.readVLong();
        }
        int size = in.readVInt();
        LongHashSet hashes = new LongHashSet(MAX_RETAINED, LOAD_FACTOR);
        for (int i = 0; i < size; i++) {
            hashes.add(in.readLong());
        }
        return new DistinctValueSet(hashes, rowsByLevel, level);
    }

    public void add(Object val) {
        long v;
        switch (val) {
            case Long l -> v = l;
            case Integer i -> v = i;
            case Short s -> v = s;
            case Byte b -> v = b;
            case Double d -> v = Double.doubleToLongBits(d);
            case Float f -> v = Float.floatToIntBits(f);
            default -> {
                add(val.toString().getBytes(StandardCharsets.UTF_8));
                return;
            }
        }
        // LongHashSet uses BitMixer as well, however, benchmarks didn't show a noticeable
        // impact on performance.
        addHashedValue(BitMixer.mix64(v ^ HASH_SEED));
    }

    private void add(byte[] bytes) {
        long key = org.elasticsearch.common.hash.MurmurHash3.hash64(bytes, 0, bytes.length);
        addHashedValue(key);
    }

    private void addHashedValue(long hash) {
        long key = hash & KEY_MASK;
        int keyLevel = levelOf(key);
        rowsByLevel[keyLevel]++;
        if (keyLevel >= level) {
            addKey(key);
        }
    }

    private static int levelOf(long entry) {
        return Long.numberOfLeadingZeros(entry & KEY_MASK);
    }

    private void addKey(long key) {
        long duplicate = key | DUPLICATE_FLAG;
        if (hashes.contains(duplicate)) {
            return;
        }
        if (hashes.remove(key)) {
            hashes.add(duplicate);
        } else {
            hashes.add(key);
            shrinkIfFull();
        }
    }

    private void shrinkIfFull() {
        while (hashes.size() >= MAX_RETAINED) {
            raiseLevel(level + 1);
        }
    }

    private void raiseLevel(int newLevel) {
        level = newLevel;
        hashes.removeAll(entry -> levelOf(entry) < newLevel);
    }

    public DistinctValueSet merge(DistinctValueSet other) {
        for (int i = 0; i < NUM_LEVELS; i++) {
            rowsByLevel[i] += other.rowsByLevel[i];
        }
        if (other.level > level) {
            raiseLevel(other.level);
        }
        for (LongCursor cursor : other.hashes) {
            // `level` may have been raised by an earlier entry.
            if (levelOf(cursor.value) >= level) {
                addMergeEntry(cursor.value);
            }
        }
        return this;
    }

    private void addMergeEntry(long entry) {
        // If `entry` was a duplicate in the original set,
        // then it will also be duplicate in the union of that set and this set.
        // Because of this, a duplicate is added as-is.
        boolean isDuplicate = (entry & DUPLICATE_FLAG) != 0;
        if (isDuplicate) {
            long nonDupe = entry & KEY_MASK;
            hashes.remove(nonDupe);
            hashes.add(entry);
            shrinkIfFull();
        } else {
            addKey(entry);
        }
    }

    // Estimated number of distinct values.
    public double distinctValues() {
        return Math.scalb((double) hashes.size(), level);
    }

    // Estimated number of distinct values that appear exactly once.
    public double singletons() {
        return Math.scalb((double) keptSingletons(), level);
    }

    // Estimated number of rows whose value appears more than once (`n - f1` in Haas-Stokes).
    public double nonSingletonRows() {
        long keptRows = 0;
        for (int i = level; i < NUM_LEVELS; i++) {
            keptRows += rowsByLevel[i];
        }
        return Math.scalb((double) (keptRows - keptSingletons()), level);
    }

    private int keptSingletons() {
        int singletons = 0;
        for (LongCursor cursor : hashes) {
            if ((cursor.value & DUPLICATE_FLAG) == 0) {
                singletons++;
            }
        }
        return singletons;
    }
}
