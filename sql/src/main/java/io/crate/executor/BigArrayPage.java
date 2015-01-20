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

package io.crate.executor;

import com.google.common.base.Preconditions;
import io.crate.core.collections.RewindableIterator;
import org.elasticsearch.common.util.ObjectArray;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class BigArrayPage implements Page {

    private class ObjectArrayIterator<T> implements RewindableIterator<T> {

        private final ObjectArray<T> array;
        private final long endPos;
        private long position;

        ObjectArrayIterator(ObjectArray<T> array, long start, long limit) {
            this.array = array;
            this.endPos = Math.min(array.size(), start+limit);
            this.position = start;
        }

        @Override
        public int rewind(int positions) {
            int rewinded = (int)Math.min(positions, position);
            position = Math.max(0, position-positions);
            return rewinded;
        }

        @Override
        public boolean hasNext() {
            return position < endPos;
        }

        @Override
        public T next() {
            try {
                return this.array.get(position++);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported");
        }
    }

    private final ObjectArray<Object[]> page;
    private final long start;
    private final long size;

    public BigArrayPage(ObjectArray<Object[]> page) {
        this(page, 0, page.size());
    }

    public BigArrayPage(ObjectArray<Object[]> page, long start, long size) {
        Preconditions.checkArgument(start <= page.size(), "start exceeds page");
        this.page = page;
        this.start = start;
        this.size = Math.min(size, page.size() - start);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new ObjectArrayIterator<>(page, start, size);
    }
}
