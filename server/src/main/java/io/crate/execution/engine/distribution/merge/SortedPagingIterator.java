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

package io.crate.execution.engine.distribution.merge;

import java.util.Comparator;

/**
 * A pagingIterator that sorts on consumption
 * <p>
 * {@link #hasNext()} might call next() on a backing iterator. So if one or more of the backing iterators contains shared
 * objects these should be consumed after a next() call and before the next hasNext() or next() call or they'll change.
 */
public class SortedPagingIterator<TKey, TRow> implements PagingIterator<TKey, TRow> {

    private final SortedMergeIterator<TKey, TRow> mergingIterator;
    private boolean ignoreLeastExhausted = false;

    /**
     * @param comparator    determining how the items are sorted
     * @param needsRepeat if true additional internal state is kept in order to be able to repeat this iterator.
     *                    If this is false a call to {@link #repeat()} might result in an exception, at best the behaviour is undefined.
     */
    public SortedPagingIterator(Comparator<TRow> comparator, boolean needsRepeat) {
        if (needsRepeat) {
            mergingIterator = new RecordingSortedMergeIterator<>(comparator);
        } else {
            // does not support repeat !!!
            mergingIterator = new PlainSortedMergeIterator<>(comparator);
        }
    }

    @Override
    public void merge(Iterable<? extends KeyIterable<TKey, TRow>> iterables) {
        mergingIterator.merge(iterables);
    }

    @Override
    public void finish() {
        ignoreLeastExhausted = true;
    }

    @Override
    public TKey exhaustedIterable() {
        return mergingIterator.exhaustedIterable();
    }

    @Override
    public Iterable<TRow> repeat() {
        return mergingIterator.repeat();
    }

    @Override
    public boolean hasNext() {
        return mergingIterator.hasNext() && (ignoreLeastExhausted || !mergingIterator.isLeastExhausted());
    }

    @Override
    public TRow next() {
        return mergingIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported for " +
                                                SortedPagingIterator.class.getSimpleName());
    }
}
