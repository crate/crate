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
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

import java.util.*;

import static com.google.common.collect.Iterators.peekingIterator;

/**
 * A pagingIterator that sorts on consumption
 *
 * {@link #hasNext()} might call next() on a backing iterator. So if one or more of the backing iterators contains shared
 * objects these should be consumed after a next() call and before the next hasNext() or next() call or they'll change.
 */
public class SortedPagingIterator<T> implements PagingIterator<T> {

    private final SortedMergeIterator<T> mergingIterator;
    private boolean ignoreLeastExhausted = false;

    public SortedPagingIterator(Ordering<T> ordering) {
        mergingIterator = new SortedMergeIterator<>(Collections.<Iterator<? extends T>>emptyList(), ordering);
    }

    @Override
    public void merge(Iterable<? extends Iterator<T>> iterators) {
        mergingIterator.merge(iterators);
    }

    @Override
    public void finish() {
        ignoreLeastExhausted = true;
    }

    @Override
    public boolean hasNext() {
        return mergingIterator.hasNext() && (ignoreLeastExhausted || !mergingIterator.leastExhausted);
    }

    @Override
    public T next() {
        return mergingIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * MergingIterator like it is used in guava Iterators.mergedSort
     * It has (limited) shared object support.
     *
     * And it also has a merge function with which additional backing iterators can be added to enable paging
     */
    private static class SortedMergeIterator<T> extends UnmodifiableIterator<T> {

        final Queue<PeekingIterator<T>> queue;
        private PeekingIterator<T> lastUsedIter = null;
        private boolean leastExhausted = false;

        public SortedMergeIterator(Iterable<? extends Iterator<? extends T>> iterators, final Comparator<? super T> itemComparator) {
            Comparator<PeekingIterator<T>> heapComparator = new Comparator<PeekingIterator<T>>() {
                @Override
                public int compare(PeekingIterator<T> o1, PeekingIterator<T> o2) {
                    return itemComparator.compare(o1.peek(), o2.peek());
                }
            };
            queue = new PriorityQueue<>(2, heapComparator);

            for (Iterator<? extends T> iterator : iterators) {
                if (iterator.hasNext()) {
                    queue.add(peekingIterator(iterator));
                }
            }
        }

        @Override
        public boolean hasNext() {
            reAddLastIterator();
            return !queue.isEmpty();
        }

        private void reAddLastIterator() {
            if (lastUsedIter != null) {
                if (lastUsedIter.hasNext()) {
                    queue.add(lastUsedIter);
                } else {
                    leastExhausted = true;
                }
                lastUsedIter = null;
            }
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            lastUsedIter = queue.remove();
            return lastUsedIter.next();
        }

        void merge(Iterable<? extends Iterator<T>> iterators) {
            if (lastUsedIter != null && lastUsedIter.hasNext()) {
                queue.add(lastUsedIter);
                lastUsedIter = null;
            }
            for (Iterator<T> rowIterator : iterators) {
                if (rowIterator.hasNext()) {
                    queue.add(peekingIterator(rowIterator));
                }
            }
            leastExhausted = false;
        }
    }
}
