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

import org.elasticsearch.ElasticsearchException;

import java.util.Iterator;

public class MultiNativeArrayBigArray<T> extends AbstractMultiArrayBigArray<T, T[]> {

    @SafeVarargs
    public MultiNativeArrayBigArray(long offset, long size, T[]... backingArrays) {
        super(offset, size, backingArrays);
    }

    @Override
    public long ramBytesUsed() {
        return 0; // TODO
    }

    @Override
    public void close() throws ElasticsearchException {
        // lalala!
    }

    @Override
    public long arrayLength(T[] array) {
        return array.length;
    }

    @Override
    protected T getValue(long backingArraysIdx, long curArrayIdx) {
        assertIsInt(backingArraysIdx);
        assertIsInt(curArrayIdx);
        return backingArrays[(int)backingArraysIdx][(int)curArrayIdx];
    }

    @Override
    protected T setValue(long backingArraysIdx, long curArrayIdx, T value) {
        assertIsInt(backingArraysIdx);
        assertIsInt(curArrayIdx);
        T old;
        synchronized (backingArrays) {
            old = backingArrays[(int) backingArraysIdx][(int) curArrayIdx];
            backingArrays[(int) backingArraysIdx][(int) curArrayIdx] = value;
        }
        return old;
    }

    @Override
    public Iterator<T> iterator() {
        return new MultiNativeArrayIterator<>(this, offset, size, backingArrays);
    }
}
