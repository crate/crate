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

import static io.crate.common.collections.Iterators.peekingIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.common.collections.AbstractIterator;
import io.crate.common.collections.Iterables;
import io.crate.common.collections.Lists;
import io.crate.common.collections.PeekingIterator;


/**
 * records sort order in order to repeat it later without having to sort everything again
 */
final class RecordingSortedMergeIterator<TKey, TRow> implements PagingIterator<TKey, TRow> {

    private final Queue<Indexed<TKey, PeekingIterator<TRow>>> queue;
    private Indexed<TKey, PeekingIterator<TRow>> lastUsedIter = null;
    private boolean leastExhausted = false;
    private boolean ignoreLeastExhausted = false;

    private final IntArrayList sortRecording = new IntArrayList();
    private final List<Iterable<TRow>> storedIterables = new ArrayList<>();
    private TKey exhausted;

    RecordingSortedMergeIterator(final Comparator<? super TRow> itemComparator) {
        Comparator<Indexed<?, PeekingIterator<TRow>>> heapComparator = (o1, o2) -> {
            return itemComparator.compare(o1.val.peek(), o2.val.peek());
        };
        queue = new PriorityQueue<>(2, heapComparator);
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
            if (lastUsedIter.val.hasNext()) {
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
        sortRecording.add(lastUsedIter.i); // record sorting for repeat
        return lastUsedIter.val.next();
    }

    private void addIterators(Iterable<? extends KeyIterable<TKey, TRow>> iterables) {
        for (KeyIterable<TKey, TRow> rowIterable : iterables) {
            Iterator<TRow> rowIterator = rowIterable.iterator();
            if (rowIterator.hasNext()) {
                // store index in stored list
                queue.add(new Indexed<>(storedIterables.size(), rowIterable.key(), peekingIterator(rowIterator)));
                this.storedIterables.add(rowIterable);
            }
        }
    }

    @Override
    public void merge(Iterable<? extends KeyIterable<TKey, TRow>> numberedIterables) {
        if (lastUsedIter != null && lastUsedIter.val.hasNext()) {
            queue.add(lastUsedIter);
            lastUsedIter = null;
        }
        addIterators(numberedIterables);
        leastExhausted = false;
    }

    @Override
    public TKey exhaustedIterable() {
        return exhausted;
    }

    public Iterable<TRow> repeat() {
        if (storedIterables.isEmpty()) {
            return Collections.emptyList();
        }
        return () -> new ReplayingIterator<>(sortRecording.buffer, Iterables.transform(storedIterables, Iterable::iterator));
    }

    static class ReplayingIterator<T> extends AbstractIterator<T> {
        private final int[] sorting;
        private int index = 0;
        private final List<Iterator<T>> iters;
        private final int itersSize;

        ReplayingIterator(int[] sorting, Iterable<? extends Iterator<T>> iterators) {
            this.sorting = sorting;
            this.iters = Lists.of(iterators);
            this.itersSize = this.iters.size();
        }

        @Override
        protected T computeNext() {
            if (index >= sorting.length) {
                return endOfData();
            }
            int iterIdx = sorting[index++];
            assert iterIdx < itersSize : "invalid iters index";

            Iterator<T> iter = iters.get(iterIdx);
            if (!iter.hasNext()) {
                return endOfData();
            }
            return iter.next();
        }
    }

    /**
     * a container for associating some object with an int index
     */
    static class Indexed<TKey, TVal> {
        private final int i;
        private final TVal val;
        private final TKey key;


        Indexed(int i, TKey key, TVal val) {
            this.i = i;
            this.key = key;
            this.val = val;
        }
    }
}
