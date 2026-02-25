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

package org.elasticsearch.common.collect;

import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.internal.hppc.IntObjectHashMap.IntObjectCursor;
import org.apache.lucene.internal.hppc.ObjectCursor;


/**
 * An immutable map implementation based on open hash map.
 * <p>
 * Can be constructed using a {@link #builder()}, or using {@link #builder(org.elasticsearch.common.collect.ImmutableOpenIntMap)} (which is an optimized
 * option to copy over existing content and modify it).
 */
public final class ImmutableOpenIntMap<VType> implements Iterable<IntObjectCursor<VType>> {

    private final IntObjectHashMap<VType> map;

    private ImmutableOpenIntMap(IntObjectHashMap<VType> map) {
        this.map = map;
    }

    /**
     * @return Returns the value associated with the given key or the default value
     * for the key type, if the key is not associated with any value.
     * <p>
     * <b>Important note:</b> For primitive type values, the value returned for a non-existing
     * key may not be the default value of the primitive type (it may be any value previously
     * assigned to that slot).
     */
    public VType get(int key) {
        return map.get(key);
    }

    /**
     * Returns <code>true</code> if this container has an association to a value for
     * the given key.
     */
    public boolean containsKey(int key) {
        return map.containsKey(key);
    }

    /**
     * @return Returns the current size (number of assigned keys) in the container.
     */
    public int size() {
        return map.size();
    }

    /**
     * @return Return <code>true</code> if this hash map contains no assigned keys.
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Returns a cursor over the entries (key-value pairs) in this map. The iterator is
     * implemented as a cursor and it returns <b>the same cursor instance</b> on every
     * call to {@link java.util.Iterator#next()}. To read the current key and value use the cursor's
     * public fields. An example is shown below.
     * <pre>
     * for (IntShortCursor c : intShortMap)
     * {
     *     System.out.println(&quot;index=&quot; + c.index
     *       + &quot; key=&quot; + c.key
     *       + &quot; value=&quot; + c.value);
     * }
     * </pre>
     * <p>
     * The <code>index</code> field inside the cursor gives the internal index inside
     * the container's implementation. The interpretation of this index depends on
     * to the container.
     */
    @Override
    public Iterator<IntObjectCursor<VType>> iterator() {
        return map.iterator();
    }

    /**
     * Returns a specialized view of the keys of this associated container.
     * The view additionally implements {@link com.carrotsearch.hppc.ObjectLookupContainer}.
     * @return
     */
    public IntObjectHashMap<VType>.KeysContainer keys() {
        return map.keys();
    }

    /**
     * Returns a direct iterator over the keys.
     */
    public Iterator<Integer> keysIt() {
        final Iterator<IntCursor> iterator = map.keys().iterator();
        return new Iterator<Integer>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Integer next() {
                return iterator.next().value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * @return
     * @return Returns a container with all values stored in this map.
     */
    public IntObjectHashMap<VType>.ValuesContainer values() {
        return map.values();
    }

    /**
     * Returns a direct iterator over the keys.
     */
    public Iterator<VType> valuesIt() {
        final Iterator<ObjectCursor<VType>> iterator = map.values().iterator();
        return new Iterator<VType>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public VType next() {
                return iterator.next().value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public String toString() {
        return map.toString();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImmutableOpenIntMap that = (ImmutableOpenIntMap) o;

        if (!map.equals(that.map)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final ImmutableOpenIntMap EMPTY = new ImmutableOpenIntMap(new IntObjectHashMap());

    @SuppressWarnings("unchecked")
    public static <VType> ImmutableOpenIntMap<VType> of() {
        return EMPTY;
    }

    public static <VType> Builder<VType> builder() {
        return new Builder<>();
    }

    public static <VType> Builder<VType> builder(int size) {
        return new Builder<>(size);
    }

    public static <VType> Builder<VType> builder(ImmutableOpenIntMap<VType> map) {
        return new Builder<>(map);
    }

    public static class Builder<VType> {

        private IntObjectHashMap<VType> map;

        @SuppressWarnings("unchecked")
        public Builder() {
            this(EMPTY);
        }

        public Builder(int size) {
            this.map = new IntObjectHashMap<>(size);
        }

        public Builder(ImmutableOpenIntMap<VType> map) {
            this.map = map.map.clone();
        }

        /**
         * Builds a new instance of the
         */
        public ImmutableOpenIntMap<VType> build() {
            IntObjectHashMap<VType> map = this.map;
            this.map = null; // nullify the map, so any operation post build will fail! (hackish, but safest)
            return new ImmutableOpenIntMap<>(map);
        }

        /**
         * Puts all the entries in the map to the builder.
         */
        public Builder<VType> putAll(Map<Integer, VType> map) {
            for (Map.Entry<Integer, VType> entry : map.entrySet()) {
                this.map.put(entry.getKey(), entry.getValue());
            }
            return this;
        }

        /**
         * A put operation that can be used in the fluent pattern.
         */
        public Builder<VType> fPut(int key, VType value) {
            map.put(key, value);
            return this;
        }

        public VType put(int key, VType value) {
            return map.put(key, value);
        }

        public VType get(int key) {
            return map.get(key);
        }

        public VType getOrDefault(int kType, VType vType) {
            return map.getOrDefault(kType, vType);
        }

        /**
         * Remove that can be used in the fluent pattern.
         */
        public Builder<VType> fRemove(int key) {
            map.remove(key);
            return this;
        }

        public VType remove(int key) {
            return map.remove(key);
        }

        public Iterator<IntObjectCursor<VType>> iterator() {
            return map.iterator();
        }

        public boolean containsKey(int key) {
            return map.containsKey(key);
        }

        public int size() {
            return map.size();
        }

        public boolean isEmpty() {
            return map.isEmpty();
        }

        public void clear() {
            map.clear();
        }

        public IntObjectHashMap<VType>.KeysContainer keys() {
            return map.keys();
        }

        public void putAll(ImmutableOpenIntMap<VType> other) {
            map.putAll(other);
        }
    }
}
