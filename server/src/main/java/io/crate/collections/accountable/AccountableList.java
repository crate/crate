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

package io.crate.collections.accountable;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.function.LongConsumer;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * List implementation accounting for shallow memory usage.
 * Based on {@link ArrayList} with some simplifications.
 * Doesn't have concurrent modification check.
 * Supports boundary checks since some features (lag/lead) rely on IndexOutOfBoundsException.
 */
public class AccountableList<T> extends AbstractList<T> {

    private static final int SOFT_MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;
    private static final Object[] DEFAULTCAPACITY_EMPTY_ELEMENTDATA = {};
    private static final int DEFAULT_CAPACITY = 10;

    private final LongConsumer allocateBytes;
    private Object[] elementData;
    private int size;

    public AccountableList(LongConsumer allocateBytes) {
        this.allocateBytes = allocateBytes;
        this.elementData = DEFAULTCAPACITY_EMPTY_ELEMENTDATA; // Empty now, will be accounted on the first growth.
        allocateBytes.accept(4); // internal 'size' integer;
    }

    private static class SubList<T> extends AbstractList<T> implements RandomAccess {
        private final AccountableList<T> root;
        private final int offset;
        private int size;

        public SubList(AccountableList<T> root, int fromIndex, int toIndex) {
            this.root = root;
            root.allocateBytes.accept(NUM_BYTES_OBJECT_REF + 8); // Pointer, offset and size.
            this.offset = fromIndex;
            this.size = toIndex - fromIndex;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void sort(Comparator<? super T> c) {
            // Arrays.sort might require up to n/ 2 temporary object references.
            root.allocateBytes.accept(RamUsageEstimator.alignObjectSize((long) NUM_BYTES_OBJECT_REF * size / 2));
            Arrays.sort((T[]) root.elementData, offset, offset + size, c);
        }

        @SuppressWarnings("unchecked")
        public T set(int index, T element) {
            Objects.checkIndex(index, size);
            T oldValue = (T) root.elementData[offset + index];
            root.elementData[offset + index] = element;
            return oldValue;
        }

        @SuppressWarnings("unchecked")
        public T get(int index) {
            Objects.checkIndex(index, size);
            return (T) root.elementData[offset + index];
        }

        public int size() {
            return size;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get(int index) {
        Objects.checkIndex(index, size);
        return (T) elementData[index];
    }

    @Override
    public int size() {
        return size;
    }


    /**
     * Similar to {@link ArrayList#sort(Comparator)} but without modification count checks.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void sort(Comparator<? super T> c) {
        // Arrays.sort might require up to n/ 2 temporary object references.
        allocateBytes.accept(RamUsageEstimator.alignObjectSize((long) NUM_BYTES_OBJECT_REF * size / 2));
        Arrays.sort((T[]) elementData, 0, size, c);
    }

    @Override
    public boolean add(T t) {
        if (size == elementData.length) {
            elementData = grow(size + 1);
        }
        elementData[size] = t;
        size++;
        return true;
    }

    /**
     * Copy of {@link ArrayList#addAll(Collection)}} with memory accounting.
     */
    @Override
    public boolean addAll(Collection<? extends T> c) {
        Object[] a = c.toArray();
        allocateBytes.accept(RamUsageEstimator.shallowSizeOf(a));
        int numNew = a.length;
        if (numNew == 0)
            return false;
        Object[] elementData;
        final int s;
        if (numNew > (elementData = this.elementData).length - (s = size))
            elementData = grow(s + numNew);
        System.arraycopy(a, 0, elementData, s, numNew);
        size = s + numNew;
        return true;
    }

    /**
     *
     * @param fromIndex low endpoint (inclusive) of the subList
     * @param toIndex high endpoint (exclusive) of the subList
     * @return
     */
    public List<T> subList(int fromIndex, int toIndex) {
        return new SubList<>(this, fromIndex, toIndex);
    }

    /**
     * Copy of {@link ArrayList#grow(int)}} with memory accounting.
     */
    private Object[] grow(int minCapacity) {
        int oldCapacity = elementData.length;
        if (oldCapacity > 0 || elementData != DEFAULTCAPACITY_EMPTY_ELEMENTDATA) {
            int newCapacity = newLength(oldCapacity,
                minCapacity - oldCapacity, /* minimum growth */
                oldCapacity >> 1           /* preferred growth */);
            // Same as RamUsageEstimator.shallowSizeOf(array) but without NUM_BYTES_ARRAY_HEADER as we are accounting only for expansion.
            allocateBytes.accept(RamUsageEstimator.alignObjectSize((long) NUM_BYTES_OBJECT_REF * (newCapacity - oldCapacity)));
            elementData = Arrays.copyOf(elementData, newCapacity);
        } else {
            elementData = new Object[Math.max(DEFAULT_CAPACITY, minCapacity)];
            allocateBytes.accept(RamUsageEstimator.shallowSizeOf(elementData));
        }
        return elementData;
    }

    /**
     * Copy of ArraysSupport.newLength
     */
    private static int newLength(int oldLength, int minGrowth, int prefGrowth) {
        int prefLength = oldLength + Math.max(minGrowth, prefGrowth); // might overflow
        if (0 < prefLength && prefLength <= SOFT_MAX_ARRAY_LENGTH) {
            return prefLength;
        } else {
            // put code cold in a separate method
            return hugeLength(oldLength, minGrowth);
        }
    }

    /**
     * Copy of to ArraysSupport.hugeLength
     * Despite on accounting, we still throw OOM here.
     * This is done to satisfy general VM contract that arrays with size > MAX_INT cannot be allocated even if there is enough memory.
     */
    private static int hugeLength(int oldLength, int minGrowth) {
        int minLength = oldLength + minGrowth;
        if (minLength < 0) { // overflow
            throw new OutOfMemoryError(
                "Required array length " + oldLength + " + " + minGrowth + " is too large");
        } else if (minLength <= SOFT_MAX_ARRAY_LENGTH) {
            return SOFT_MAX_ARRAY_LENGTH;
        } else {
            return minLength;
        }
    }
}
