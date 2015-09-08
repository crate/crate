/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.merge;

import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

import java.util.*;

import static com.google.common.collect.Iterators.peekingIterator;

/**
 * MergingIterator like it is used in guava Iterators.mergedSort
 * It has (limited) shared object support.
 *
 * And it also has a merge function with which additional backing iterators can be added to enable paging
 */
class PlainSortedMergeIterator<T> extends UnmodifiableIterator<T> implements SortedMergeIterator<T> {

    final Queue<PeekingIterator<T>> queue;
    PeekingIterator<T> lastUsedIter = null;
    boolean leastExhausted = false;

    public PlainSortedMergeIterator(Iterable<? extends Iterable<T>> iterables, final Comparator<? super T> itemComparator) {
        Comparator<PeekingIterator<T>> heapComparator = new Comparator<PeekingIterator<T>>() {
            @Override
            public int compare(PeekingIterator<T> o1, PeekingIterator<T> o2) {
                return itemComparator.compare(o1.peek(), o2.peek());
            }
        };
        queue = new PriorityQueue<>(2, heapComparator);
        addIterators(iterables);
    }

    private void addIterators(Iterable<? extends Iterable<T>> iterables) {
        for (Iterable<T> rowIterable : iterables) {
            Iterator<T> rowIterator = rowIterable.iterator();
            if (rowIterator.hasNext()) {
                queue.add(peekingIterator(rowIterator));
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

    public void merge(Iterable<? extends Iterable<T>> iterables) {
        if (lastUsedIter != null && lastUsedIter.hasNext()) {
            queue.add(lastUsedIter);
            lastUsedIter = null;
        }
        addIterators(iterables);
        leastExhausted = false;
    }

    public boolean isLeastExhausted() {
        return leastExhausted;
    }

    @Override
    public Iterator<T> repeat() {
        throw new UnsupportedOperationException("cannot repeat with " + getClass().getSimpleName());
    }
}
