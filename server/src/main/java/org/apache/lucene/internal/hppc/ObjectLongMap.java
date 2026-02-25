/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package org.apache.lucene.internal.hppc;

import java.util.Objects;

public class ObjectLongMap<K> {

    private K[] keys;
    private long[] values;
    private double loadFactor;
    private int resizeAt;
    private int mask;

    private boolean hasEmptyKey;


    /**
     * The number of stored keys (assigned key slots), excluding the special
     * "empty" key, if any (use {@link #size()} instead).
     *
     * @see #size()
     */
    private int assigned;

    /**
     * We perturb hash values with a container-unique
     * seed to avoid problems with nearly-sorted-by-hash
     * values on iterations.
     *
     * @see #hashKey
     * @see "http://issues.carrot2.org/browse/HPPC-80"
     * @see "http://issues.carrot2.org/browse/HPPC-103"
     */
    private int keyMixer;

    public ObjectLongMap(int expectedElements) {
        this(expectedElements, HashContainers.DEFAULT_LOAD_FACTOR);
    }

    public ObjectLongMap(int expectedElements, double loadFactor) {
        HashContainers.checkLoadFactor(loadFactor, HashContainers.MIN_LOAD_FACTOR, HashContainers.MAX_LOAD_FACTOR);
        this.loadFactor = loadFactor;
        ensureCapacity(expectedElements);
    }

    public long put(K key, long value) {
        assert assigned < mask + 1;

        final int mask = this.mask;
        if (((key) == null)) {
            hasEmptyKey = true;
            long previousValue = values[mask + 1];
            values[mask + 1] = value;
            return previousValue;
        } else {
            final K[] keys = this.keys;
            int slot = hashKey(key) & mask;

            K existing;
            while (!((existing = keys[slot]) == null)) {
                if (Objects.equals(existing, key)) {
                    final long previousValue = values[slot];
                    values[slot] = value;
                    return previousValue;
                }
                slot = (slot + 1) & mask;
            }

            if (assigned == resizeAt) {
                allocateThenInsertThenRehash(slot, key, value);
            } else {
                keys[slot] = key;
                values[slot] = value;
            }

            assigned++;
            return 0L;
        }
    }

    public long get(K key) {
        if (((key) == null)) {
            return hasEmptyKey ? values[mask + 1] : 0L;
        } else {
            final K[] keys = this.keys;
            final int mask = this.mask;
            int slot = hashKey(key) & mask;

            K existing;
            while (!((existing = keys[slot]) == null)) {
                if (((existing) == (key))) {
                    return values[slot];
                }
                slot = (slot + 1) & mask;
            }

            return 0L;
        }
    }

    public boolean isEmpty() {
        return assigned == 0;
    }

    public int size() {
        return assigned;
    }

    /**
    * This method is invoked when there is a new key/ value pair to be inserted into
    * the buffers but there is not enough empty slots to do so.
    *
    * New buffers are allocated. If this succeeds, we know we can proceed
    * with rehashing so we assign the pending element to the previous buffer
    * (possibly violating the invariant of having at least one empty slot)
    * and rehash all keys, substituting new buffers at the end.
    */
    protected void allocateThenInsertThenRehash(int slot, K pendingKey, long pendingValue) {
        assert assigned == resizeAt
            && ((keys[slot]) == null)
            && !((pendingKey) == null);

        // Try to allocate new buffers first. If we OOM, we leave in a consistent state.
        final K[] prevKeys = (K[]) this.keys;
        final long[] prevValues = this.values;
        allocateBuffers(HashContainers.nextBufferSize(mask + 1, size(), loadFactor));
        assert this.keys.length > prevKeys.length;

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the old arrays before rehashing.
        prevKeys[slot] = pendingKey;
        prevValues[slot] = pendingValue;

        // Rehash old keys, including the pending key.
        rehash(prevKeys, prevValues);
    }

    /**
     * Returns a hash code for the given key.
     *
     * <p>The default implementation mixes the hash of the key with {@link #keyMixer}
     * to differentiate hash order of keys between hash containers. Helps
     * alleviate problems resulting from linear conflict resolution in open
     * addressing.</p>
     *
     * <p>The output from this function should evenly distribute keys across the
     * entire integer range.</p>
     */
    int hashKey(K key) {
        assert key != null : "key==0 is handled as special case via empty slot marker";
        return BitMixer.mix32(key.hashCode() ^ keyMixer);
    }


    /**
    * Ensure this container can hold at least the given number of keys (entries) without resizing its
    * buffers.
    *
    * @param expectedElements The total number of keys, inclusive.
    */
    public void ensureCapacity(int expectedElements) {
        if (expectedElements > resizeAt || keys == null) {
            final K[] prevKeys = this.keys;
            final long[] prevValues = this.values;
            allocateBuffers(HashContainers.minBufferSize(expectedElements, loadFactor));
            if (prevKeys != null && !isEmpty()) {
                rehash(prevKeys, prevValues);
            }
        }
    }

    /** Rehash from old buffers to new buffers. */
    protected void rehash(K[] fromKeys, long[] fromValues) {
        assert fromKeys.length == fromValues.length &&
            HashContainers.checkPowerOfTwo(fromKeys.length - 1);

        // Rehash all stored key/value pairs into the new buffers.
        final K[] keys = this.keys;
        final long[] values = this.values;
        final int mask = this.mask;
        K existing;

        // Copy the zero element's slot, then rehash everything else.
        int from = fromKeys.length - 1;
        keys[keys.length - 1] = fromKeys[from];
        values[values.length - 1] = fromValues[from];
        while (--from >= 0) {
            if (!((existing = fromKeys[from]) == null)) {
                int slot = hashKey(existing) & mask;
                while (!((keys[slot]) == null)) {
                    slot = (slot + 1) & mask;
                }
                keys[slot] = existing;
                values[slot] = fromValues[from];
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void allocateBuffers(int arraySize) {
        assert Integer.bitCount(arraySize) == 1;

        // Ensure no change is done if we hit an OOM.
        K[] prevKeys = this.keys;
        long[] prevValues = this.values;
        try {
            int emptyElementSlot = 1;
            this.keys = (K[]) new Object[arraySize + emptyElementSlot];
            this.values = (new long[arraySize + emptyElementSlot]);
        } catch (OutOfMemoryError e) {
            this.keys = prevKeys;
            this.values = prevValues;
            throw new BufferAllocationException(
                "Not enough memory to allocate buffers for rehashing: %,d -> %,d",
                e, this.mask + 1, arraySize);
        }
        this.resizeAt = HashContainers.expandAtCount(arraySize, loadFactor);
        this.mask = arraySize - 1;
    }
}
