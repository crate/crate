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
import com.google.common.collect.AbstractIterator;
import org.elasticsearch.common.util.ObjectArray;

import java.util.Iterator;

public class BigArrayPage implements Page {


    private final boolean isLastPage;

    private class ObjectArrayIterator<T> extends AbstractIterator<T>{

        private final ObjectArray<T> array;
        private final long limit;
        private long position;

        ObjectArrayIterator(ObjectArray<T> array, long start, long limit) {
            this.array = array;
            this.limit = start+limit;
            this.position = start;
        }

        @Override
        protected T computeNext() {
            if (position > limit-1) {
                endOfData();
                return null;
            }
            try {
                return this.array.get(position++);
            } catch (ArrayIndexOutOfBoundsException e) {
                endOfData();
                return null;
            }
        }
    }

    private final ObjectArray<Object[]> page;
    private final long start;
    private final long size;

    public BigArrayPage(ObjectArray<Object[]> pageSource, boolean isLastPage) {
        this(pageSource, 0, pageSource.size(), isLastPage);
    }

    public BigArrayPage(ObjectArray<Object[]> page, long start, long size, boolean isLastPage) {
        Preconditions.checkArgument(start <= page.size(), "start exceeds page");
        this.page = page;
        this.start = start;
        this.size = Math.min(size, page.size() - start);
        this.isLastPage = isLastPage;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean isLastPage() {
        return isLastPage;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new ObjectArrayIterator<>(page, start, size);
    }
}
