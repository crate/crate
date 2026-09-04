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

package io.crate.collections;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A hash map split into a fixed number of independent buckets, each a plain {@link java.util.HashMap}.
 * The bucket is picked from the high bits of the (spread) key hash code, mirroring ClickHouse's
 * TwoLevelHashTable. Splitting into buckets allows e.g. bucket-parallel merging of two instances.
 */
public class TwoLevelHashMap<K, V> implements Iterable<Map.Entry<K, V>> {

    private static final int DEFAULT_BITS_FOR_BUCKET = 8;

    private final int bitsForBucket;
    private final int numBuckets;
    private final HashMap<K, V>[] buckets;

    public TwoLevelHashMap() {
        this(DEFAULT_BITS_FOR_BUCKET);
    }

    @SuppressWarnings("unchecked")
    public TwoLevelHashMap(int bitsForBucket) {
        this.bitsForBucket = bitsForBucket;
        this.numBuckets = 1 << bitsForBucket;
        this.buckets = new HashMap[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = new HashMap<>();
        }
    }

    /** Fibonacci hashing: spreads entropy into the high bits regardless of the input distribution. */
    private static final int MIX_CONSTANT = 0x9E3779B1;

    /** Exposed so callers can route entries into {@link #bucket(int)} directly, bypassing {@link #put}/{@link #get}. */
    public int bucketIndex(Object key) {
        int mixed = key.hashCode() * MIX_CONSTANT;
        return mixed >>> (32 - bitsForBucket);
    }

    public V put(K key, V value) {
        return buckets[bucketIndex(key)].put(key, value);
    }

    public V get(Object key) {
        return buckets[bucketIndex(key)].get(key);
    }

    public V remove(Object key) {
        return buckets[bucketIndex(key)].remove(key);
    }

    public boolean containsKey(Object key) {
        return buckets[bucketIndex(key)].containsKey(key);
    }

    public int size() {
        int size = 0;
        for (var bucket : buckets) {
            size += bucket.size();
        }
        return size;
    }

    public boolean isEmpty() {
        for (var bucket : buckets) {
            if (!bucket.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public void clear() {
        for (var bucket : buckets) {
            bucket.clear();
        }
    }

    public int numBuckets() {
        return numBuckets;
    }

    public Map<K, V> bucket(int index) {
        return buckets[index];
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return new Iterator<>() {
            int bucketIdx = 0;
            Iterator<Map.Entry<K, V>> current = buckets[0].entrySet().iterator();

            private void advance() {
                while (!current.hasNext() && bucketIdx < numBuckets - 1) {
                    bucketIdx++;
                    current = buckets[bucketIdx].entrySet().iterator();
                }
            }

            @Override
            public boolean hasNext() {
                advance();
                return current.hasNext();
            }

            @Override
            public Map.Entry<K, V> next() {
                advance();
                if (!current.hasNext()) {
                    throw new NoSuchElementException();
                }
                return current.next();
            }
        };
    }
}
