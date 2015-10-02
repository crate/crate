/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.merge;

import com.google.common.collect.Ordering;

import java.util.Collections;
import java.util.Iterator;

/**
 * A pagingIterator that sorts on consumption
 *
 * {@link #hasNext()} might call next() on a backing iterator. So if one or more of the backing iterators contains shared
 * objects these should be consumed after a next() call and before the next hasNext() or next() call or they'll change.
 */
public class SortedPagingIterator<T> implements PagingIterator<T> {

    private final SortedMergeIterator<T> mergingIterator;
    private boolean ignoreLeastExhausted = false;

    /**
     * @param ordering determining how the items are sorted
     * @param needsRepeat if true additional internal state is kept in order to be able to repeat this iterator.
     *                    If this is false a call to {@link #repeat()} might result in an excaption, at best the behaviour is undefined.
     */
    public SortedPagingIterator(Ordering<T> ordering, boolean needsRepeat) {
        if (needsRepeat) {
            mergingIterator = new RecordingSortedMergeIterator<>(Collections.<NumberedIterable<T>>emptyList(), ordering);
        } else {
            // does not support repeat !!!
            mergingIterator = new PlainSortedMergeIterator<>(Collections.<NumberedIterable<T>>emptyList(), ordering);
        }
    }

    @Override
    public void merge(Iterable<? extends NumberedIterable<T>> iterables) {
        mergingIterator.merge(iterables);
    }

    @Override
    public void finish() {
        ignoreLeastExhausted = true;
    }

    @Override
    public int exhaustedIterable() {
        return mergingIterator.exhaustedIterable();
    }

    @Override
    public Iterable<T> repeat() {
        return mergingIterator.repeat();
    }

    @Override
    public boolean hasNext() {
        return mergingIterator.hasNext() && (ignoreLeastExhausted || !mergingIterator.isLeastExhausted());
    }

    @Override
    public T next() {
        return mergingIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
