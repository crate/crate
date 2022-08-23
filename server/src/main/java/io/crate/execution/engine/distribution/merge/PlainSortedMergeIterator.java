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

import io.crate.common.collections.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

import static io.crate.common.collections.Iterators.peekingIterator;


/**
 * MergingIterator like it is used in guava Iterators.mergedSort
 * It has (limited) shared object support.
 * <p>
 * And it also has a merge function with which additional backing iterators can be added to enable paging
 */
final class PlainSortedMergeIterator<TKey, TRow> implements PagingIterator<TKey, TRow> {

    private final Queue<NumberedPeekingIterator<TKey, TRow>> queue;
    private NumberedPeekingIterator<TKey, TRow> lastUsedIter = null;
    private boolean leastExhausted = false;
    private boolean ignoreLeastExhausted = false;
    private TKey exhausted;

    PlainSortedMergeIterator(final Comparator<? super TRow> itemComparator) {
        Comparator<PeekingIterator<TRow>> heapComparator = (o1, o2) -> itemComparator.compare(o1.peek(), o2.peek());
        queue = new PriorityQueue<>(2, heapComparator);
    }

    private void addIterators(Iterable<? extends KeyIterable<TKey, TRow>> iterables) {
        for (KeyIterable<TKey, TRow> iterable : iterables) {
            Iterator<TRow> rowIterator = iterable.iterator();
            if (rowIterator.hasNext()) {
                queue.add(new NumberedPeekingIterator<>(iterable.key(), peekingIterator(rowIterator)));
            }
        }
    }

    @Override
    public void finish() {
        ignoreLeastExhausted = true;
    }

    @Override
    public boolean hasNext() {
        reAddLastIterator();
        return !queue.isEmpty() && (ignoreLeastExhausted || !leastExhausted);
    }

    private void reAddLastIterator() {
        if (lastUsedIter != null) {
            if (lastUsedIter.hasNext()) {
                queue.add(lastUsedIter);
            } else {
                leastExhausted = true;
                exhausted = lastUsedIter.key;
            }
            lastUsedIter = null;
        }
    }

    @Override
    public TRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException("no more rows should exist");
        }
        lastUsedIter = queue.remove();
        return lastUsedIter.next();
    }

    @Override
    public void merge(Iterable<? extends KeyIterable<TKey, TRow>> numberedIterables) {
        if (lastUsedIter != null && lastUsedIter.hasNext()) {
            queue.add(lastUsedIter);
            lastUsedIter = null;
        }
        addIterators(numberedIterables);
        leastExhausted = false;
    }

    public boolean isLeastExhausted() {
        return leastExhausted;
    }

    @Override
    public TKey exhaustedIterable() {
        return exhausted;
    }

    @Override
    public Iterable<TRow> repeat() {
        throw new UnsupportedOperationException("cannot repeat with " + getClass().getSimpleName());
    }

    private static class NumberedPeekingIterator<TKey, TRow> implements PeekingIterator<TRow> {

        private final TKey key;
        private final PeekingIterator<TRow> peekingIterator;

        NumberedPeekingIterator(TKey key, PeekingIterator<TRow> peekingIterator) {
            this.key = key;
            this.peekingIterator = peekingIterator;
        }

        @Override
        public TRow peek() {
            return peekingIterator.peek();
        }

        @Override
        public TRow next() {
            return peekingIterator.next();
        }

        @Override
        public void remove() {
            peekingIterator.remove();
        }

        @Override
        public boolean hasNext() {
            return peekingIterator.hasNext();
        }
    }
}
