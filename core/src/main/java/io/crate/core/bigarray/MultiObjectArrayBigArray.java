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
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ObjectArray;

import java.util.Iterator;

public class MultiObjectArrayBigArray<T> extends AbstractMultiArrayBigArray<T, ObjectArray<T>> {

    @SafeVarargs
    public MultiObjectArrayBigArray(long offset, long size, ObjectArray<T>... backingArrays) {
        super(offset, size, backingArrays);
    }

    @Override
    public long arrayLength(ObjectArray<T> array) {
        return array.size();
    }

    @Override
    protected T getValue(long backingArraysIdx, long curArrayIdx) {
        assertIsInt(backingArraysIdx);
        return backingArrays[(int)backingArraysIdx].get(curArrayIdx);
    }

    @Override
    protected T setValue(long backingArraysIdx, long curArrayIdx, T value) {
        assertIsInt(backingArraysIdx);
        synchronized (backingArrays) {
            return backingArrays[(int) backingArraysIdx].set(curArrayIdx, value);
        } 
    }

    @Override
    public long ramBytesUsed() {
        long used = 24; // initial offset
        for (ObjectArray<T> array : backingArrays) {
            used += array.ramBytesUsed();
        }
        return used;
    }

    @Override
    public Iterator<T> iterator() {
        return new MultiBigArrayIterator<>(this, offset, size(), backingArrays);
    }

    @Override
    public void close() throws ElasticsearchException {
        Releasables.close(backingArrays);
    }
}
