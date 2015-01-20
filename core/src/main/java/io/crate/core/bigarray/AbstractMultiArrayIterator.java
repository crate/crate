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

import com.google.common.collect.UnmodifiableIterator;
import io.crate.core.collections.RewindableIterator;

import java.util.NoSuchElementException;

public abstract class AbstractMultiArrayIterator<T, ArrayType> extends UnmodifiableIterator<T> implements RewindableIterator<T> {

    protected final ArrayType[] backingArrays;
    protected final long endOffset;
    protected final int arraysLen;

    protected long backingArraysIdx;
    protected long curArrayIdx;
    protected long curOffset;

    public AbstractMultiArrayIterator(AbstractMultiArrayBigArray<T, ArrayType> bigArray,
                                      long offset,
                                      long size,
                                      ArrayType[] backingArrays) {
        this.backingArrays = backingArrays;
        this.arraysLen = backingArrays.length;
        this.endOffset = offset + size;
        this.curOffset = offset;

        long[] offsetIndices = bigArray.arraysIdx(backingArrays, offset);
        this.backingArraysIdx = offsetIndices[0];
        this.curArrayIdx = offsetIndices[1];
    }

    protected void assertIsInt(long l) {
        assert l == (int)l : "long value exceeds int range";
    }

    private boolean switchArray() {
        backingArraysIdx++;
        curArrayIdx = 0;
        if (backingArraysIdx >= arraysLen) {
            return false;
        }
        // advance through 0 length arrays
        while (getArrayLength(backingArraysIdx) == 0) {
            backingArraysIdx++;
            if (backingArraysIdx >= arraysLen) {
                return false;
            }
        }
        return true;
    }

    public abstract long getArrayLength(long idx);

    public abstract T getValue(long backingArraysIdx, long curArrayIdx);

    @Override
    public boolean hasNext() {
        if (backingArraysIdx < arraysLen && curArrayIdx >= getArrayLength(backingArraysIdx)) {
            if (!switchArray()) {
                return false;
            }
        }
        if (curOffset >= endOffset) {
            return false;
        }
        return true;
    }

    @Override
    public T next() {
        T value;
        try {
            value = getValue(backingArraysIdx, curArrayIdx);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
        curArrayIdx++;
        curOffset++;
        return value;
    }

    @Override
    public int rewind(int positions) {
        int rewinded = 0;
        long furtherRewind = curArrayIdx - positions;

        while (furtherRewind < 0 && backingArraysIdx > 0) {
            rewinded += (int)curArrayIdx;
            backingArraysIdx--;
            curArrayIdx = getArrayLength(backingArraysIdx)-1;
            furtherRewind = furtherRewind + curArrayIdx;
        }
        rewinded += curArrayIdx - furtherRewind;
        curArrayIdx = furtherRewind;
        curOffset = curOffset - positions;
        return rewinded;
    }
}
