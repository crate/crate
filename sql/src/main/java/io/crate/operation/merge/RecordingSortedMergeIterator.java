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
class RecordingSortedMergeIterator<T> extends UnmodifiableIterator<T> implements SortedMergeIterator<T> {

    private final Function<Iterable<T>, Iterator<T>> TO_ITERATOR = new Function<Iterable<T>, Iterator<T>>() {
        @Nullable
        @Override
        public Iterator<T> apply(Iterable<T> input) {
            return input.iterator();
        }
    };
    private final Queue<Indexed<PeekingIterator<T>>> queue;
    private Indexed<PeekingIterator<T>> lastUsedIter = null;
    private boolean leastExhausted = false;

    private final IntArrayList sortRecording = new IntArrayList();
    private final List<Iterable<T>> storedIterables = new ArrayList<>();

    public RecordingSortedMergeIterator(Iterable<? extends Iterable<T>> iterables, final Comparator<? super T> itemComparator) {
        Comparator<Indexed<PeekingIterator<T>>> heapComparator = new Comparator<Indexed<PeekingIterator<T>>>() {
            @Override
            public int compare(Indexed<PeekingIterator<T>> o1, Indexed<PeekingIterator<T>> o2) {
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
        sortRecording.add(lastUsedIter.i); // record sorting for repeat
        return lastUsedIter.val.next();
    }

    void addIterators(Iterable<? extends Iterable<T>> iterables) {
        for (Iterable<T> rowIterable : iterables) {
            Iterator<T> rowIterator = rowIterable.iterator();
            if (rowIterator.hasNext()) {
                // store index in stored list
                queue.add(new Indexed<>(storedIterables.size(), peekingIterator(rowIterator)));
                this.storedIterables.add(rowIterable);
            }
        }
    }

    public void merge(Iterable<? extends Iterable<T>> iterables) {
        if (lastUsedIter != null && lastUsedIter.val.hasNext()) {
            queue.add(lastUsedIter);
            lastUsedIter = null;
        }
        addIterators(iterables);
        leastExhausted = false;
    }

    @Override
    public boolean isLeastExhausted() {
        return leastExhausted;
    }

    public Iterator<T> repeat() {
        // TODO: make defensive copies?
        return new ReplayingIterator<>(sortRecording.buffer, Iterables.transform(storedIterables, TO_ITERATOR));
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
    static class Indexed<T> {
        private final int i;
        private final T val;

        public Indexed(int i, T val) {
            this.i = i;
            this.val = val;
        }
    }
}
