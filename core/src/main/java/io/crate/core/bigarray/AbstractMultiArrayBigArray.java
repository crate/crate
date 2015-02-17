/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.core.bigarray;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Locale;

/**
 * ObjectArray that is Iterable and backed by multiple custom array implementations
 * @param <T> the type of element this array contains in several backing arrays
 * @param <ArrayType> the actual type of backing array. e.g. T[] for native arrays
 */
public abstract class AbstractMultiArrayBigArray<T, ArrayType> implements IterableBigArray<T> {

    protected final ArrayType[] backingArrays;
    protected final long offset;
    protected final long size;

    /**
     * create a bigarray instance using n numbers of backing arrays
     * with
     *
     * @param offset a starting offset that starts to count at the beginning
     *               of all the backing arrays.
     *               Using {@linkplain #get(long)} will get the element at offset + the given argument
     *               in the underlying row of backingArrays.
     * @param size how many elements of the underlying backingArrays this array references
     * @param backingArrays a variable number ( > 0 ) of arrays that back this bigarray instance
     */
    @SafeVarargs
    public AbstractMultiArrayBigArray(long offset, long size, ArrayType ... backingArrays) {
        long arraysSize = arraysSize(backingArrays);
        Preconditions.checkArgument(offset <= arraysSize, "offset exceeds backing arrays");
        this.backingArrays = backingArrays;
        this.offset = offset;
        this.size = Math.min(size, arraysSize);
    }

    public abstract long arrayLength(ArrayType array);

    protected abstract T getValue(long backingArraysIdx, long curArrayIdx);

    protected abstract T setValue(long backingArraysIdx, long curArrayIdx, T value);

    protected void assertIsInt(long l) {
        assert l == (int)l : "long value exceeds int range";
    }

    protected long arraysSize(ArrayType[] arrays) {
        long count = 0L;
        for (ArrayType array : arrays) {
            count += arrayLength(array);
        }
        return count;
    }

    protected long[] arraysIdx(ArrayType[] arrays, long index) {
        int arrayIdx = 0;
        long accumulatedSize = 0L;


        long curArrayLen;
        while (arrayIdx < arrays.length) {
            curArrayLen = arrayLength(arrays[arrayIdx]);
            if (accumulatedSize + curArrayLen > index) {
                break;
            }
            accumulatedSize += curArrayLen;
            arrayIdx++;
        }

        long curIdx = (int) (accumulatedSize > 0 ? (index-accumulatedSize) : index);
        return new long[]{arrayIdx, curIdx};
    }

    protected long[] arraysIdx(long index) {
        return arraysIdx(backingArrays, index+offset);
    }


    @Override
    public T get(long index) {
        if (index >= size || index < 0 || backingArrays.length == 0) {
            throw new ArrayIndexOutOfBoundsException(
                    String.format(Locale.ENGLISH, "index %d exceeds bounds of backing arrays", index));
        }
        long[] indices = arraysIdx(index);
        return getValue(indices[0], indices[1]);
    }

    @Override
    public T set(long index, T value) {
        if (index > size || backingArrays.length == 0) {
            throw new ArrayIndexOutOfBoundsException(
                    String.format(Locale.ENGLISH, "index %d exceeds bounds of backing arrays", index));
        }
        long[] indices = arraysIdx(index);
        return setValue(indices[0], indices[1], value);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractMultiArrayBigArray that = (AbstractMultiArrayBigArray) o;

        if (offset != that.offset) return false;
        if (size != that.size) return false;
        if (!Arrays.deepEquals(backingArrays, that.backingArrays))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.deepHashCode(backingArrays);
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (int) (size ^ (size >>> 32));
        return result;
    }
}
