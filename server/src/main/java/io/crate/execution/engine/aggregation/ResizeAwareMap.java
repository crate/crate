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

package io.crate.execution.engine.aggregation;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import io.crate.common.annotations.VisibleForTesting;

/**
 * Wrapper for maps containing large number of elements.
 *
 * Resize of such map can create significant number of (retained) elements,
 * memory accounting must utilize expectedCapacityIncrease to account for that.
 */
public class ResizeAwareMap<K, V> implements Map<K, V> {

    // Copy of java.util.HashMap.MAXIMUM_CAPACITY
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private final Map<K, V> delegate;
    private final float loadFactor;
    private final boolean openAddressing;
    private final long singleItemBytes;

    private int currentCapacity;

    /**
     * @param openAddressing represents if it's a Netty map (true) or JDK map (false)
     *
     * @param singleItemBytes represents bytes per additional capacity slot on resize.
     * JDK maps only store an object reference per slot, regardless of the key type.
     * Netty maps additionally keep a primitive key array.
     */
    public ResizeAwareMap(Map<K, V> delegate,
                          int initialCapacity,
                          float loadFactor,
                          boolean openAddressing,
                          long singleItemBytes) {
        this.delegate = delegate;
        this.currentCapacity = tableSizeFor(initialCapacity);
        this.loadFactor = loadFactor;
        this.openAddressing = openAddressing;
        this.singleItemBytes = singleItemBytes;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return delegate.get(key);
    }

    @Override
    public V put(K key, V value) {
        V result = delegate.put(key, value);
        growIfNeeded();
        return result;
    }

    @Override
    public V remove(Object key) {
        return delegate.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("putAll is not supported on ResizeAwareMap");
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Set<K> keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection<V> values() {
        return delegate.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return delegate.entrySet();
    }

    /**
     * @return internal capacity increase
     * after the next put() which will add a new unique key.
     */
    @VisibleForTesting
    int expectedCapacityIncrease() {
        int capacity = currentCapacity;
        while (capacity < MAXIMUM_CAPACITY && delegate.size() + 1 > resizeThreshold(capacity)) {
            capacity = capacity << 1;
        }
        return capacity - currentCapacity;
    }

    public long expectedCapacityIncreaseBytes() {
        return singleItemBytes * expectedCapacityIncrease();
    }

    public int currentCapacity() {
        return currentCapacity;
    }

    /**
     * Mirrors what delegate map is doing to keep track of the current capacity.
     * Both JDK and Netty maps grow in power of 2.
     * JDK map has an explicit limit.
     * Netty map's growSize() doesn't check 1<<30 and rehash with negative capacity throws.
     */
    private void growIfNeeded() {
        // This method is always called **after** put(),
        // so delegate.size() already accounts for potential duplicate key(s).
        while (currentCapacity < MAXIMUM_CAPACITY && delegate.size() > resizeThreshold(currentCapacity)) {
            currentCapacity <<= 1;
        }
    }

    private int resizeThreshold(int capacity) {
        int threshold = (int) (capacity * loadFactor);
        // Netty maps need 1 slot to be free, hence resize happens earlier.
        // See calcMaxSize() in Netty maps.
        return openAddressing ? Math.min(capacity - 1, threshold) : threshold;
    }

    /**
     * Copied from java.util.HashMap#tableSizeFor
     */
    private static int tableSizeFor(int cap) {
        int n = -1 >>> Integer.numberOfLeadingZeros(cap - 1);
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }
}
