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

/**
 * A streamable, bounded sample of the hashes of all sampled values.
 * <p>
 * Only hashes with at least {@code level} leading zeros are kept, so each distinct value is kept
 * with probability {@code 2^-level}, together with all its occurrences.
 * Once {@link #MAX_RETAINED} hashes are kept, the level is raised, which drops about half of them.
 * <p>
 * The lowest bit of each hash is used as a duplication flag.
 */
public final class DistinctValueSet {

    private static final long DUPLICATE_FLAG = 1L;
    private static final long KEY_MASK = ~DUPLICATE_FLAG;

    // HPPC resizes at a load factor of 0.75, so the hash table stays at 8192 slots.
    private static final int MAX_RETAINED = 6144;

    // A hash has 0 to 64 leading zeros.
    private static final int NUM_LEVELS = Long.SIZE + 1;

    private final LongHashSet hashes;

    // The number of added rows by the level of their hash, including the rows of dropped hashes.
    private final long[] rowsByLevel;

    private int level;

    DistinctValueSet() {
        this(new LongHashSet(), new long[NUM_LEVELS], 0);
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
        LongHashSet hashes = new LongHashSet(size);
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
        add(BitMixer.mix64(v));
    }

    private void add(byte[] bytes) {
        long key = org.elasticsearch.common.hash.MurmurHash3.hash64(bytes, 0, bytes.length);
        add(key);
    }

    private void add(long hash) {
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
        return Math.scalb((double) (keptRows() - keptSingletons()), level);
    }

    // Estimated number of rows whose value already appeared in an earlier row (`n - d`).
    public double repeatedRows() {
        return Math.scalb((double) (keptRows() - hashes.size()), level);
    }

    // The rows of all kept hashes. Hashes are dropped by level, so a row at a level of at least `level` belongs to a kept hash.
    private long keptRows() {
        long keptRows = 0;
        for (int i = level; i < NUM_LEVELS; i++) {
            keptRows += rowsByLevel[i];
        }
        return keptRows;
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
