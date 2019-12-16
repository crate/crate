/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.breaker;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOf;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

public final class MapSizeEstimator extends SizeEstimator<Map<?, ?>> {

    public static final MapSizeEstimator INSTANCE = new MapSizeEstimator();
    private static final int MAX_DEPTH = 1;
    private static final int STRING_SIZE = (int) shallowSizeOfInstance(String.class);

    @Override
    public long estimateSize(@Nullable Map<?, ?> value) {
        if (value == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        }
        return sizeOfMap(value, 0, UNKNOWN_DEFAULT_RAM_BYTES_USED);
    }

    private static long sizeOfMap(Map<?, ?> map, int depth, long defSize) {
        if (map == null) {
            return 0;
        }
        long size = shallowSizeOf(map);
        if (depth > MAX_DEPTH) {
            return size;
        }
        long sizeOfEntry = -1;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (sizeOfEntry == -1) {
                sizeOfEntry = shallowSizeOf(entry);
            }
            size += sizeOfEntry;
            size += sizeOfObject(entry.getKey(), depth, defSize);
            size += sizeOfObject(entry.getValue(), depth, defSize);
        }
        return alignObjectSize(size);
    }

    private static long sizeOfObject(Object o, int depth, long defSize) {
        if (o == null) {
            return 0;
        }
        long size;
        if (o instanceof Accountable) {
            size = ((Accountable)o).ramBytesUsed();
        } else if (o instanceof String) {
            size = sizeOf((String)o);
        } else if (o instanceof boolean[]) {
            size = RamUsageEstimator.sizeOf((boolean[])o);
        } else if (o instanceof byte[]) {
            size = RamUsageEstimator.sizeOf((byte[])o);
        } else if (o instanceof char[]) {
            size = RamUsageEstimator.sizeOf((char[])o);
        } else if (o instanceof double[]) {
            size = RamUsageEstimator.sizeOf((double[])o);
        } else if (o instanceof float[]) {
            size = RamUsageEstimator.sizeOf((float[])o);
        } else if (o instanceof int[]) {
            size = RamUsageEstimator.sizeOf((int[])o);
        } else if (o instanceof Long) {
            size = RamUsageEstimator.sizeOf((Long)o);
        } else if (o instanceof long[]) {
            size = RamUsageEstimator.sizeOf((long[])o);
        } else if (o instanceof short[]) {
            size = RamUsageEstimator.sizeOf((short[])o);
        } else if (o instanceof String[]) {
            size = sizeOf((String[]) o);
        } else if (o instanceof Map) {
            size = sizeOfMap((Map) o, ++depth, defSize);
        } else if (o instanceof Collection) {
            size = sizeOfCollection((Collection)o, ++depth, defSize);
        } else {
            if (defSize > 0) {
                size = defSize;
            } else {
                size = shallowSizeOf(o);
            }
        }
        return size;
    }

    private static long sizeOf(String[] arr) {
        long size = shallowSizeOf(arr);
        for (String s : arr) {
            if (s == null) {
                continue;
            }
            size += sizeOf(s);
        }
        return size;
    }

    public static long sizeOf(String s) {
        if (s == null) {
            return 0;
        }
        // may not be true in Java 9+ and CompactStrings - but we have no way to determine this

        // char[] + hashCode
        long size = STRING_SIZE + (long)NUM_BYTES_ARRAY_HEADER + (long)Character.BYTES * s.length();
        return alignObjectSize(size);
    }

    private static long sizeOfCollection(Collection<?> collection, int depth, long defSize) {
        if (collection == null) {
            return 0;
        }
        long size = shallowSizeOf(collection);
        if (depth > MAX_DEPTH) {
            return size;
        }
        // assume array-backed collection and add per-object references
        size += NUM_BYTES_ARRAY_HEADER + collection.size() * NUM_BYTES_OBJECT_REF;
        for (Object o : collection) {
            size += sizeOfObject(o, depth, defSize);
        }
        return alignObjectSize(size);
    }
}
