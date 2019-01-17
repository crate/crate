/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

/**
 * A reusable class to encode {@code field -&gt; memory size} mappings
 */
public final class FieldMemoryStats implements Writeable, Iterable<ObjectLongCursor<String>>{

    private final ObjectLongHashMap<String> stats;

    /**
     * Creates a new FieldMemoryStats instance
     */
    public FieldMemoryStats(ObjectLongHashMap<String> stats) {
        this.stats = Objects.requireNonNull(stats, "status must be non-null");
        assert !stats.containsKey(null);
    }

    /**
     * Creates a new FieldMemoryStats instance from a stream
     */
    public FieldMemoryStats(StreamInput input) throws IOException {
        int size = input.readVInt();
        stats = new ObjectLongHashMap<>(size);
        for (int i = 0; i < size; i++) {
            stats.put(input.readString(), input.readVLong());
        }
    }

    /**
     * Adds / merges the given field memory stats into this stats instance
     */
    public void add(FieldMemoryStats fieldMemoryStats) {
        for (ObjectLongCursor<String> entry : fieldMemoryStats.stats) {
            stats.addTo(entry.key, entry.value);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(stats.size());
        for (ObjectLongCursor<String> entry : stats) {
            out.writeString(entry.key);
            out.writeVLong(entry.value);
        }
    }

    /**
     * Generates x-content into the given builder for each of the fields in this stats instance
     * @param builder the builder to generated on
     * @param key the top level key for this stats object
     * @param rawKey the raw byte key for each of the fields byte sizes
     * @param readableKey the readable key for each of the fields byte sizes
     */
    public void toXContent(XContentBuilder builder, String key, String rawKey, String readableKey) throws IOException {
        builder.startObject(key);
        for (ObjectLongCursor<String> entry : stats) {
            builder.startObject(entry.key);
            builder.humanReadableField(rawKey, readableKey, new ByteSizeValue(entry.value));
            builder.endObject();
        }
        builder.endObject();
    }

    /**
     * Creates a deep copy of this stats instance
     */
    public FieldMemoryStats copy() {
        return new FieldMemoryStats(stats.clone());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMemoryStats that = (FieldMemoryStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    @Override
    public Iterator<ObjectLongCursor<String>> iterator() {
        return stats.iterator();
    }

    /**
     * Returns the fields value in bytes or <code>0</code> if it's not present in the stats
     */
    public long get(String field) {
        return stats.get(field);
    }

    /**
     * Returns <code>true</code> iff the given field is in the stats
     */
    public boolean containsField(String field) {
        return stats.containsKey(field);
    }
}
