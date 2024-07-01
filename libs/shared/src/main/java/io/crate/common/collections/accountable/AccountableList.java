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

package io.crate.common.collections.accountable;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntConsumer;

/**
 * List implementation accounting for shallow memory usage.
 * Based on {@link ArrayList} with some simplifications.
 * Doesn't have concurrent modification check.
 * Supports boundary checks since some features (lag/lead) rely on IndexOutOfBoundsException.
 * Supports only {@link #add(Object)} operation.
 */
public class AccountableList<T> extends AbstractList<T> {

    private  static final int SOFT_MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;
    private static final Object[] DEFAULTCAPACITY_EMPTY_ELEMENTDATA = {};
    private static final int DEFAULT_CAPACITY = 10;

    private final IntConsumer growthAccounting;
    private Object[] elementData;
    private int size;

    public AccountableList(IntConsumer growthAccounting) {
        this.growthAccounting = growthAccounting;
        this.elementData = DEFAULTCAPACITY_EMPTY_ELEMENTDATA;
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
     * Simplified version of {@link ArrayList#grow(int)}} operation but with minGrowth = 1.
     * We don't support "addAll" or "ensureCapacity", so minGrowth (minCapacity - oldCapacity) is fixed and equals to 1.
     */
    private Object[] grow(int minCapacity) {
        int oldCapacity = elementData.length;
        if (oldCapacity > 0 || elementData != DEFAULTCAPACITY_EMPTY_ELEMENTDATA) {
            int newCapacity = newLength(oldCapacity, oldCapacity >> 1);
            elementData = Arrays.copyOf(elementData, newCapacity);
        } else {
            elementData = new Object[Math.max(DEFAULT_CAPACITY, minCapacity)];
        }
        growthAccounting.accept(elementData.length - oldCapacity);
        return elementData;
    }

    /**
     * Copy of ArraysSupport.newLength with following preconditions:
     * minGrowth = 1. prefGrowth >= minGrowth.
     */
    public static int newLength(int oldLength, int prefGrowth) {
        int prefLength = oldLength + prefGrowth; // might overflow
        if (prefLength <= SOFT_MAX_ARRAY_LENGTH) {
            return prefLength;
        } else {
            return hugeLength(oldLength);
        }
    }

    /**
     * Copy of to ArraysSupport.hugeLength with fixed minGrowth = 1.
     * Despite on accounting, we still throw OOM here.
     * This is done to satisfy general VM contract that arrays with size > MAX_INT cannot be allocated even if there is enough memory.
     */
    private static int hugeLength(int oldLength) {
        int minLength = oldLength + 1;
        if (minLength < 0) { // overflow
            throw new OutOfMemoryError(
                "Required array length " + oldLength + " + " + 1 + " is too large");
        } else if (minLength <= SOFT_MAX_ARRAY_LENGTH) {
            return SOFT_MAX_ARRAY_LENGTH;
        } else {
            return minLength;
        }
    }
}
