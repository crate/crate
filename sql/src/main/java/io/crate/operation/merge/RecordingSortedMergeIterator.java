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

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.Function;
import com.google.common.collect.*;

import javax.annotation.Nullable;
import java.util.*;

import static com.google.common.collect.Iterators.peekingIterator;

/**
 * records sort order in order to repeat it later without having to sort everything again
 */
class RecordingSortedMergeIterator<TKey, TRow> extends UnmodifiableIterator<TRow> implements SortedMergeIterator<TKey, TRow> {

    private final Function<Iterable<TRow>, Iterator<TRow>> TO_ITERATOR = new Function<Iterable<TRow>, Iterator<TRow>>() {
        @Nullable
        @Override
        public Iterator<TRow> apply(Iterable<TRow> input) {
            return input.iterator();
        }
    };
    private final Queue<Indexed<TKey, PeekingIterator<TRow>>> queue;
    private Indexed<TKey, PeekingIterator<TRow>> lastUsedIter = null;
    private boolean leastExhausted = false;

    private final IntArrayList sortRecording = new IntArrayList();
    private final List<Iterable<TRow>> storedIterables = new ArrayList<>();
    private TKey exhausted;

    public RecordingSortedMergeIterator(Iterable<? extends KeyIterable<TKey, TRow>> iterables, final Comparator<? super TRow> itemComparator) {
        Comparator<Indexed<?, PeekingIterator<TRow>>> heapComparator = new Comparator<Indexed<?, PeekingIterator<TRow>>>() {
            @Override
            public int compare(Indexed<?, PeekingIterator<TRow>> o1, Indexed<?, PeekingIterator<TRow>> o2) {
                return itemComparator.compare(o1.val.peek(), o2.val.peek());
            }
        };
        queue = new PriorityQueue<>(2, heapComparator);

        addIterators(iterables);
    }

    @Override
    public boolean hasNext() {
        reAddLastIterator();
        return !queue.isEmpty();
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
            throw new NoSuchElementException();
        }
        lastUsedIter = queue.remove();
        sortRecording.add(lastUsedIter.i); // record sorting for repeat
        return lastUsedIter.val.next();
    }

    void addIterators(Iterable<? extends KeyIterable<TKey, TRow>> iterables) {
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
    public boolean isLeastExhausted() {
        return leastExhausted;
    }

    @Override
    public TKey exhaustedIterable() {
        return exhausted;
    }

    public Iterable<TRow> repeat() {
        // TODO: make defensive copies?
        return new Iterable<TRow>() {
            @Override
            public Iterator<TRow> iterator() {
                return new ReplayingIterator<>(sortRecording.buffer, Iterables.transform(storedIterables, TO_ITERATOR));
            }
        };
    }

    static class ReplayingIterator<T> extends AbstractIterator<T> {
        private final int[] sorting;
        private int index = 0;
        private final List<Iterator<T>> iters;
        private final int itersSize;

        ReplayingIterator(int[] sorting, Iterable<? extends Iterator<T>> iterators) {
            this.sorting = sorting;
            this.iters = ImmutableList.<Iterator<T>>builder().addAll(iterators).build();
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


        public Indexed(int i, TKey key, TVal val) {
            this.i = i;
            this.key = key;
            this.val = val;
        }
    }
}
