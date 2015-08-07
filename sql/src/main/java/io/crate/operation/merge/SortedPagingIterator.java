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
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;

public class SortedPagingIterator implements PagingIterator<Row> {

    private final RowMergingIterator mergingIterator;
    private boolean ignoreLeastExhausted = false;

    public SortedPagingIterator(Ordering<Row> ordering) {
        mergingIterator = new RowMergingIterator(Collections.<Iterator<? extends Row>>emptyList(), ordering);
    }

    @Override
    public void merge(Iterable<? extends Iterator<Row>> iterators) {
        mergingIterator.merge(iterators);
    }

    @Override
    public void finish() {
        ignoreLeastExhausted = true;
    }

    @Override
    public boolean hasNext() {
        return (ignoreLeastExhausted || !mergingIterator.leastExhausted)
                && mergingIterator.hasNext();
    }

    @Override
    public Row next() {
        return mergingIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private static class PeekingRowIterator implements PeekingIterator<Row> {

        private final Iterator<? extends Row> iterator;
        private boolean hasPeeked;
        private Row peekedElement;

        public PeekingRowIterator(Iterator<? extends Row> iterator) {
            this.iterator = iterator;
        }

        @Override
        public Row peek() {
            if (!hasPeeked) {
                peekedElement = new RowN(iterator.next().materialize());
                hasPeeked = true;
            }
            return peekedElement;
        }

        @Override
        public boolean hasNext() {
            return hasPeeked || iterator.hasNext();
        }

        @Override
        public Row next() {
            if (!hasPeeked) {
                return iterator.next();
            }
            Row result = peekedElement;
            hasPeeked = false;
            peekedElement = null;
            return result;
        }

        @Override
        public void remove() {
            checkState(!hasPeeked, "Can't remove after you've peeked at next");
            iterator.remove();
        }
    }

    private static PeekingRowIterator peekingIterator(Iterator<? extends Row> iterator) {
        if (iterator instanceof PeekingRowIterator) {
            return (PeekingRowIterator) iterator;
        }
        return new PeekingRowIterator(iterator);
    }

    /**
     * MergingIterator like it is used in guava Iterators.mergedSort
     * but uses a custom PeekingRowIterator which materializes the rows on peek()
     */
    private static class RowMergingIterator extends UnmodifiableIterator<Row> {

        final Queue<PeekingRowIterator> queue;
        private boolean leastExhausted = false;

        public RowMergingIterator(Iterable<? extends Iterator<? extends Row>> iterators, final Comparator<? super Row> itemComparator) {
            Comparator<PeekingRowIterator> heapComparator = new Comparator<PeekingRowIterator>() {
                @Override
                public int compare(PeekingRowIterator o1, PeekingRowIterator o2) {
                    return itemComparator.compare(o1.peek(), o2.peek());
                }
            };
            queue = new PriorityQueue<>(2, heapComparator);

            for (Iterator<? extends Row> iterator : iterators) {
                if (iterator.hasNext()) {
                    queue.add(peekingIterator(iterator));
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public Row next() {
            PeekingRowIterator nextIter = queue.remove();
            Row next = nextIter.next();
            if (nextIter.hasNext()) {
                queue.add(nextIter);
            } else {
                leastExhausted = true;
            }
            return next;
        }

        void merge(Iterable<? extends Iterator<Row>> iterators) {
            for (Iterator<Row> rowIterator : iterators) {
                if (rowIterator.hasNext()) {
                    queue.add(peekingIterator(rowIterator));
                }
            }
            leastExhausted = false;
        }
    }
}
