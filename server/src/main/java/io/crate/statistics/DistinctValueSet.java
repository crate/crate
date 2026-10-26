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

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;

/**
 * A streamable set of the hashes of all sampled values.
 * <p>
 * The lowest bit of each hash is used as a flag that is set once the value has been seen more than once,
 * so the set provides the exact number of distinct values and of values seen exactly once in the sample.
 */
public final class DistinctValueSet {

    private static final long DUPLICATE_FLAG = 1L;
    private static final long KEY_MASK = ~DUPLICATE_FLAG;

    private final LongHashSet hashes;

    DistinctValueSet() {
        this(new LongHashSet());
    }

    private DistinctValueSet(LongHashSet hashes) {
        this.hashes = hashes;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(hashes.size());
        for (LongCursor cursor : hashes) {
            out.writeLong(cursor.value);
        }
    }

    public static DistinctValueSet fromStream(StreamInput in) throws IOException {
        int size = in.readVInt();
        LongHashSet hashes = new LongHashSet(size);
        for (int i = 0; i < size; i++) {
            hashes.add(in.readLong());
        }
        return new DistinctValueSet(hashes);
    }

    public void update(String v) {
        update(v.getBytes(StandardCharsets.UTF_8));
    }

    public void update(byte[] bytes) {
        long key = org.elasticsearch.common.hash.MurmurHash3.hash64(bytes, 0, bytes.length) & KEY_MASK;
        add(key);
    }

    public DistinctValueSet merge(DistinctValueSet other) {
        // todo this may over-allocate, see if we can estimate the size.
        hashes.ensureCapacity(hashes.size() + other.hashes.size());
        for (LongCursor cursor : other.hashes) {
            add(cursor.value);
        }
        return this;
    }

    // Number of distinct values.
    public int size() {
        return hashes.size();
    }

    // Number of distinct values that appear exactly once.
    public int singletons() {
        int singletons = 0;
        for (LongCursor cursor : hashes) {
            if ((cursor.value & DUPLICATE_FLAG) == 0) {
                singletons++;
            }
        }
        return singletons;
    }

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
