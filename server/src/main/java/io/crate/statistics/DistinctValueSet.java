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
 * To estimate the number of distinct values, Wegman's Adaptive Sampling algorithm
 * is used. For more information about, check Flajolet's analysis of it:
 * <a href="https://algo.inria.fr/flajolet/Publications/Flajolet90.pdf">On Adaptive Sampling</a>.
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

    // The hash table is allocated with 8192 slots up front and is never resized,
    // because it is shrunk before it reaches the resize threshold of 8192 * LOAD_FACTOR.
    private static final double LOAD_FACTOR = 15.0 / 16.0;
    private static final int MAX_RETAINED = 7680;

    private static final long HASH_SEED = 0x9E3779B97F4A7C15L;

    private final LongPreHashedSet hashes;

    private int level;

    DistinctValueSet() {
        this(new LongPreHashedSet(), 0);
    }

    private DistinctValueSet(LongPreHashedSet hashes, int level) {
        this.hashes = hashes;
        this.level = level;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(level);
        out.writeVInt(hashes.size());
        for (LongCursor cursor : hashes) {
            out.writeLong(cursor.value);
        }
    }

    public static DistinctValueSet fromStream(StreamInput in) throws IOException {
        int level = in.readVInt();
        int size = in.readVInt();
        LongPreHashedSet hashes = new LongPreHashedSet();
        for (int i = 0; i < size; i++) {
            hashes.add(in.readLong());
        }
        return new DistinctValueSet(hashes, level);
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
        add(BitMixer.mix64(v ^ HASH_SEED));
    }

    private void add(byte[] bytes) {
        long key = org.elasticsearch.common.hash.MurmurHash3.hash64(bytes, 0, bytes.length);
        add(key);
    }

    private void add(long hash) {
        long key = hash & KEY_MASK;
        if (levelOf(key) >= level) {
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
        assert this.hashes.keys.length == other.hashes.keys.length;

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

    private int keptSingletons() {
        int singletons = 0;
        for (LongCursor cursor : hashes) {
            if ((cursor.value & DUPLICATE_FLAG) == 0) {
                singletons++;
            }
        }
        return singletons;
    }

    // todo somehow, it might have caused a small regression with memory allocation for text columns.
    private static final class LongPreHashedSet extends LongHashSet {
        public LongPreHashedSet() {
            super(MAX_RETAINED, LOAD_FACTOR);
        }

        @Override
        protected int hashKey(long key) {
            return (int) key;
        }
    }
}
