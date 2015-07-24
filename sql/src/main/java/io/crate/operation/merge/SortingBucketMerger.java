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

package io.crate.operation.merge;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.MultiFutureCallback;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.*;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.naming.directory.BasicAttribute;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;

/**
 * BucketMerger implementation that expects sorted rows in the
 * incoming buckets and merges them to a single sorted stream of {@linkplain Row}s.
 */
public class SortingBucketMerger implements PageDownstream, StoppableRowUpstream {

    private final static ESLogger LOGGER = Loggers.getLogger(SortingBucketMerger.class);

    private final RowDownstreamHandle downstream;
    private final Ordering<Row> ordering;
    private final int numBuckets;
    private final Optional<Executor> executor;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    private final AtomicBoolean paused = new AtomicBoolean(false);

    private RowMergingIterator prevRowMergingIterator = null;
    private boolean pendingPause;
    private RowMergingIterator pausedIterator;
    private PageConsumeListener pausedListener;


    public SortingBucketMerger(RowDownstream rowDownstream,
                               int numBuckets,
                               int[] orderByPositions,
                               boolean[] reverseFlags,
                               Boolean[] nullsFirst,
                               Optional<Executor> executor) {
        Preconditions.checkArgument(numBuckets > 0, "must at least get 1 bucket per merge call");
        this.numBuckets = numBuckets;
        this.executor = executor;
        List<Comparator<Row>> comparators = new ArrayList<>(orderByPositions.length);
        for (int i = 0; i < orderByPositions.length; i++) {
            OrderingByPosition<Row> rowOrdering = OrderingByPosition.rowOrdering(orderByPositions[i], reverseFlags[i], nullsFirst[i]);
            comparators.add(rowOrdering.reverse());
        }
        ordering = Ordering.compound(comparators);
        downstream = rowDownstream.registerUpstream(this);
    }


    @Override
    public void pause() {
        pendingPause = true;
    }

    @Override
    public void resume() {
        if (paused.compareAndSet(true, false)) {
            LOGGER.trace("resume sending rows to: {}", downstream);

            if (pausedIterator.leastExhausted()) {
                prevRowMergingIterator = pausedIterator;
                pausedListener.needMore();
            } else {
                processBuckets(pausedIterator, pausedListener);
            }
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received resume but wasn't paused {}", downstream);
        }
    }

    /**
     * This is basically a sort-merge but with more than 2 sides.
     * Results must be pre-sorted for this to work
     *
     * p1
     * b1: [ A , A, B, C ]
     * b2: [ B, B ]
     *
     * p2
     * b1: [ C, C, D ]
     * b2: [ B, B, D ]
     *
     * output:
     *  [ A, A, B, B, B, B, C, C, C, D, D ]
     *
     */
    @Override
    public void nextPage(BucketPage page, final PageConsumeListener listener) {
        assert page.buckets().size() == numBuckets :
                "number of buckets received in merge call must match the number given in the constructor";

        FutureCallback<List<Bucket>> finalCallback = new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(List<Bucket> buckets) {
                RowMergingIterator mergingIterator = getMergingIterator(getBucketIterators(buckets));
                processBuckets(mergingIterator, listener);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                fail(t);
                listener.finish();
            }
        };

        Executor executor = this.executor.or(MoreExecutors.directExecutor());
        MultiFutureCallback<Bucket> multiFutureCallback = new MultiFutureCallback<>(page.buckets().size(), finalCallback);
        for (ListenableFuture<Bucket> bucketFuture : page.buckets()) {
            Futures.addCallback(bucketFuture, multiFutureCallback, executor);
        }
    }

    private void processBuckets(RowMergingIterator mergingIterator, PageConsumeListener listener) {
        while (mergingIterator.hasNext()) {
            if (finished.get()) {
                listener.finish();
                return;
            }
            Row row = mergingIterator.next();
            boolean wantMore = downstream.setNextRow(row);

            if (pendingPause) {
                pausedIterator = mergingIterator;
                pausedListener = listener;
                paused.set(true);
                pendingPause = false;
                return;
            }

            if (!wantMore) {
                listener.finish();;
                return;
            }
            if (mergingIterator.leastExhausted()) {
                // this means the first bucket in the page is empty, need to request the next page
                // before consuming the remaining buckets
                prevRowMergingIterator = mergingIterator;
                break;
            }
        }

        listener.needMore();
    }

    private RowMergingIterator getMergingIterator(List<Iterator<Row>> bucketIts) {
        RowMergingIterator mergingIterator;
        if (prevRowMergingIterator != null) {
            mergingIterator = prevRowMergingIterator.merge(bucketIts);
        } else {
            mergingIterator = new RowMergingIterator(bucketIts, ordering);
        }
        return mergingIterator;
    }

    private List<Iterator<Row>> getBucketIterators(List<Bucket> buckets) {
        List<Iterator<Row>> bucketIts = new ArrayList<>(buckets.size());
        for (Bucket bucket : buckets) {
            bucketIts.add(bucket.iterator());
        }
        return bucketIts;
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

        public boolean leastExhausted() {
            return leastExhausted;
        }

        public RowMergingIterator merge(List<Iterator<Row>> iterators) {
            for (Iterator<Row> rowIterator : iterators) {
                if (rowIterator.hasNext()) {
                    queue.add(peekingIterator(rowIterator));
                }
            }
            leastExhausted = false;
            return this;
        }
    }

    @Override
    public void finish() {
        if(finished.compareAndSet(false, true)) {
            consumeRemaining();
            downstream.finish();
        }
    }

    private void consumeRemaining() {
        if (prevRowMergingIterator == null) {
            return;
        }
        while (prevRowMergingIterator.hasNext()) {
            boolean wantMore = downstream.setNextRow(prevRowMergingIterator.next());
            if (!wantMore) {
                break;
            }
        }
    }

    @Override
    public void fail(Throwable e) {
        if (!finished.getAndSet(true)) {
            downstream.fail(e);
        }
    }
}
