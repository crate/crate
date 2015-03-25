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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.projectors.NoOpProjector;
import io.crate.operation.projectors.sorting.OrderingByPosition;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BucketMerger implementation that expects sorted rows in the
 * incoming buckets and merges them to a single sorted stream of {@linkplain Row}s.
 */
public class SortingBucketMerger implements BucketMerger {

    private RowDownstreamHandle downstream;
    private final Ordering<Row> ordering;
    private final int numBuckets;
    private final AtomicBoolean wantMore;
    private final IntOpenHashSet exhaustedIterators;
    private final IntArrayList bucketsWithRowEqualToLeast;
    private final ArrayList<Row> previousRows;
    private final Optional<Executor> executor;
    private Iterator<Row>[] remainingBucketIts = null;

    public SortingBucketMerger(int numBuckets,
                               int[] orderByPositions,
                               boolean[] reverseFlags,
                               Boolean[] nullsFirst,
                               Optional<Executor> executor) {
        Preconditions.checkArgument(numBuckets > 0, "must at least get 1 bucket per merge call");
        this.numBuckets = numBuckets;
        this.executor = executor;
        List<Comparator<Row>> comparators = new ArrayList<>(orderByPositions.length);
        for (int i = 0; i < orderByPositions.length; i++) {
            comparators.add(OrderingByPosition.rowOrdering(orderByPositions[i], reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
        wantMore = new AtomicBoolean(true);

        //noinspection unchecked
        remainingBucketIts = new Iterator[numBuckets];

        previousRows = new ArrayList<>(numBuckets);
        bucketsWithRowEqualToLeast = new IntArrayList(numBuckets);
        exhaustedIterators = new IntOpenHashSet(numBuckets, 1);

        // use noOp as default to avoid null checks
        downstream = NoOpProjector.INSTANCE;
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
     * see private emitBuckets(...) for more details on how this works
     */
    @Override
    public void nextPage(BucketPage page, final PageConsumeListener listener) {
        assert page.buckets().size() == numBuckets :
                "number of buckets received in merge call must match the number given in the constructor";
        final AtomicBoolean listenerNotified = new AtomicBoolean(false);
        Futures.addCallback(Futures.allAsList(page.buckets()), new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(List<Bucket> buckets) {
                try {
                    if (numBuckets == 1) {
                        emitSingleBucket(buckets.get(0));
                    } else {
                        ArrayList<Iterator<Row>> bucketIts = new ArrayList<>(numBuckets);
                        for (int i = 0; i < buckets.size(); i++) {
                            Iterator<Row> remainingBucketIt = remainingBucketIts[i];
                            if (remainingBucketIt == null) {
                                bucketIts.add(buckets.get(i).iterator());
                            } else {
                                bucketIts.add(Iterators.concat(remainingBucketIt, buckets.get(i).iterator()));
                            }
                        }
                        emitBuckets(bucketIts);
                    }
                } catch (Throwable t) {
                    downstream.fail(t);
                } finally {
                    notifyListener();
                }
            }

            private void notifyListener() {
                if (!listenerNotified.getAndSet(true)) {
                    if (wantMore.get()) {
                        listener.needMore();
                    } else {
                        listener.finish();
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                wantMore.set(false);
                fail(t);
                notifyListener();
            }
        }, executor.isPresent() ? executor.get() : MoreExecutors.directExecutor());
    }

    /**
     * multi bucket sort-merge based on iterators
     *
     * Page1  (first merge call)
     *     B1      B2       B3
     *
     *    ->B     ->A     ->A
     *      B       C       A
     *                      B
     *
     * first iteration across all buckets:
     *
     *      leastRow:                   A (from b2)
     *      equal (or also leastRow):   b3
     *
     *      these will be emitted
     *
     * second iteration:
     *
     *    ->B       A       A
     *      B     ->C     ->A
     *                      B
     *
     *      leastRow:   A (from b3)
     *      equal: none
     *
     * third iteration:
     *
     *    ->B       A       A
     *      B     ->C       A
     *                    ->B
     *
     *      leastRow:   B (from b1)
     *      equal:      B (from b2)
     *
     * fourth iteration:
     *
     *      B       A       A
     *    ->B     ->C       A
     *                      B
     *
     *      leastRow:   B (from b1)
     *
     * after the fourth iteration the iterator that had the leastRow (B1) will be exhausted
     * which causes B2 (Row C) to be put into the remainingIterators which will be used if a new merge call is made
     */
    private void emitBuckets(ArrayList<Iterator<Row>> bucketIts) {
        exhaustedIterators.clear();
        bucketsWithRowEqualToLeast.clear();
        previousRows.clear();

        for (Iterator<Row> bucketIt : bucketIts) {
            if (bucketIt.hasNext()) {
                previousRows.add(bucketIt.next());
            } else {
                previousRows.add(null);
            }
        }

        int bi = 0;
        Row leastRow = null;
        int leastBi = -1;
        while (exhaustedIterators.size() < numBuckets) {
            Row row = previousRows.get(bi);
            if (row == null) {
                exhaustedIterators.add(bi);
            }

            if (leastRow == null) {
                leastRow = row;
                leastBi = bi;
            } else if (row != null) {
                int compare = ordering.compare(leastRow, row);
                if (compare < 0) {
                    leastBi = bi;
                    leastRow = row;
                } else if (compare == 0) {
                    bucketsWithRowEqualToLeast.add(bi);
                }
            }

            if (bi == (numBuckets-1)) {
                // looked at all buckets..
                boolean leastBucketItExhausted = false;

                if (leastRow == null) {
                    leastBucketItExhausted = true;
                } else {
                    if (!emit(leastRow)) {
                        Arrays.fill(remainingBucketIts, null);
                        return;
                    }

                    // send for all other buckets that are equal to least
                    for (IntCursor equalBucketIdx : bucketsWithRowEqualToLeast) {

                        if (!emit(previousRows.get(equalBucketIdx.value))) {
                            Arrays.fill(remainingBucketIts, null);
                            return;
                        }

                        Iterator<Row> equalBucketIt = bucketIts.get(equalBucketIdx.value);
                        if (equalBucketIt.hasNext()) {
                            previousRows.set(equalBucketIdx.value, equalBucketIt.next());
                        } else {
                            remainingBucketIts[equalBucketIdx.value] = null;
                            previousRows.set(equalBucketIdx.value, null);
                            exhaustedIterators.add(bi);
                        }
                    }
                    Iterator<Row> bucketItWithLeastRow = bucketIts.get(leastBi);
                    if (bucketItWithLeastRow.hasNext()) {
                        previousRows.set(leastBi, bucketItWithLeastRow.next());
                    } else {
                        previousRows.set(leastBi, null);
                        remainingBucketIts[leastBi] = null;
                        leastBucketItExhausted = true;
                    }


                    leastRow = null;
                    bucketsWithRowEqualToLeast.clear();
                }
                if (leastBucketItExhausted) {
                    // need next page to continue...
                    for (int i = 0; i < numBuckets; i++) {
                        Iterator<Row> bucketIt = bucketIts.get(i);
                        Row previousRow = previousRows.get(i);
                        if (previousRow != null) {
                            Iterator<Row> iterator = ImmutableList.of(previousRow).iterator();
                            if (bucketIt.hasNext()) {
                                iterator = Iterators.concat(iterator, bucketIt);
                            }
                            remainingBucketIts[i] = iterator;
                        } else if (bucketIt.hasNext()) {
                            remainingBucketIts[i] = bucketIt;
                        }
                    }
                    return;
                }
            }
            bi = (bi+1) % numBuckets;
        }
    }

    private void emitSingleBucket(Bucket bucket) {
        for (Row row : bucket) {
            if (!emit(row)) {
                wantMore.set(false);
                return;
            }
        }
    }

    private boolean emit(Row row) {
        if (!downstream.setNextRow(row)) {
            wantMore.set(false);
            return false;
        }
        return true;
    }

    @Override
    public void finish() {
        while (hasReminaingBucketIts()) {
            ArrayList<Iterator<Row>> bucketIts = new ArrayList<>(remainingBucketIts.length);
            for (Iterator<Row> bucketIt : remainingBucketIts) {
                if (bucketIt == null) {
                    bucketIts.add(Collections.<Row>emptyIterator());
                } else {
                    bucketIts.add(bucketIt);
                }
            }
            emitBuckets(bucketIts);
        }
        downstream.finish();
    }

    @SuppressWarnings("unchecked")
    private boolean hasReminaingBucketIts() {
        return Iterators.any(Iterators.forArray(remainingBucketIts), Predicates.notNull());
    }

    @Override
    public void fail(Throwable e) {
        downstream.fail(e);
    }

    @Override
    public void downstream(RowDownstream downstream) {
        assert downstream != null : "downstream must not be null";
        this.downstream = downstream.registerUpstream(this);
    }
}
