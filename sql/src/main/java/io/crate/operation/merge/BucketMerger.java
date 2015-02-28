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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.NoOpProjector;
import io.crate.operation.projectors.Projector;
import io.crate.operation.projectors.sorting.OrderingByPosition;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class BucketMerger implements ProjectorUpstream {

    private Projector downstream;
    private final Ordering<Row> ordering;
    private final int numBuckets;
    private final int offset;
    private final int limit;
    private final AtomicInteger rowsEmitted;
    private final AtomicInteger rowsSkipped;
    private final IntOpenHashSet exhaustedIterators;
    private final IntArrayList bucketsWithRowEqualToLeast;
    private final ArrayList<Row> previousRows;
    private Iterator<Row>[] remainingBucketIts = null;

    public BucketMerger(int numBuckets,
                        int offset,
                        int limit,
                        int[] orderByPositions,
                        boolean[] reverseFlags,
                        Boolean[] nullsFirst) {
        Preconditions.checkArgument(numBuckets > 0, "must at least get 1 bucket per merge call");
        this.numBuckets = numBuckets;
        this.offset = offset;
        this.limit = limit;
        List<Comparator<Row>> comparators = new ArrayList<>(orderByPositions.length);
        for (int i = 0; i < orderByPositions.length; i++) {
            comparators.add(new OrderingByPosition(i, reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
        rowsEmitted = new AtomicInteger(0);
        rowsSkipped = new AtomicInteger(0);

        //noinspection unchecked
        remainingBucketIts = new Iterator[numBuckets];

        previousRows = new ArrayList<>(numBuckets);
        bucketsWithRowEqualToLeast = new IntArrayList(numBuckets);
        exhaustedIterators = new IntOpenHashSet(numBuckets, 1);

        // use noOp as default to avoid null checks
        downstream = new NoOpProjector();
    }

    @Override
    public void downstream(Projector downstream) {
        assert downstream != null : "downstream must not be null";
        downstream.registerUpstream(this);
        this.downstream = downstream;
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
    public void merge(List<ListenableFuture<Bucket>> buckets) {
        assert buckets.size() == numBuckets :
                "number of buckets received in merge call must match the number given in the constructor";

        try {
            if (numBuckets == 1) {
                emitSingleBucket(buckets.get(0).get());
            } else {
                ArrayList<Iterator<Row>> bucketIts = new ArrayList<>(numBuckets);
                for (int i = 0; i < buckets.size(); i++) {
                    Iterator<Row> remainingBucketIt = remainingBucketIts[i];
                    if (remainingBucketIt == null) {
                        bucketIts.add(buckets.get(i).get().iterator());
                    } else {
                        bucketIts.add(Iterators.concat(remainingBucketIt, new FutureBackedRowIterator(buckets.get(i))));
                    }
                }
                emitBuckets(bucketIts);
            }
        } catch (Throwable t) {
            downstream.upstreamFailed(t);
        }
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
                bi = bi < numBuckets - 1 ? bi + 1 : 0;
            } else {
                int compare = ordering.compare(leastRow, row);
                if (compare < 0) {
                    leastBi = bi;
                    leastRow = row;
                } else if (compare == 0) {
                    bucketsWithRowEqualToLeast.add(bi);
                }

                bi++;
                if (bi == numBuckets) {
                    // looked at all buckets..
                    boolean leastBucketItExhausted = false;

                    emit(leastRow);

                    // send for all other buckets that are equal to least
                    for (IntCursor equalBucketIdx : bucketsWithRowEqualToLeast) {
                        emit(previousRows.get(equalBucketIdx.value));
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
                    bi = 0;

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
            }
        }
    }

    private ArrayList<Iterator<Row>> getIterators(List<Bucket> buckets) {
        ArrayList<Iterator<Row>> bucketIts = new ArrayList<>(numBuckets);

        for (int i = 0; i < buckets.size(); i++) {
            Iterator<Row> remainingBucketIt = remainingBucketIts[i];
            if (remainingBucketIt == null) {
                bucketIts.add(buckets.get(i).iterator());
            } else {
                bucketIts.add(Iterators.concat(remainingBucketIt, buckets.get(i).iterator()));
            }
        }
        return bucketIts;
    }

    private void emitSingleBucket(Bucket bucket) {
        for (Row row : bucket) {
            if (!emit(row)) {
                return;
            }
        }
    }

    private boolean emit(Row row) {
        if (rowsSkipped.getAndIncrement() < offset) {
            return true;
        }
        //noinspection SimplifiableIfStatement
        if (rowsEmitted.getAndIncrement() < limit) {
            return downstream.setNextRow(rowToArray(row));
        }
        return false;
    }

    public void finish() {
        ArrayList<Iterator<Row>> bucketIts = new ArrayList<>(remainingBucketIts.length);
        for (Iterator<Row> bucketIt : remainingBucketIts) {
            if (bucketIt == null) {
                bucketIts.add(Collections.<Row>emptyIterator());
            } else {
                bucketIts.add(bucketIt);
            }
        }
        emitBuckets(bucketIts);
        downstream.upstreamFinished();
    }

    // TODO: remove once Projector is row based
    private Object[] rowToArray(Row row) {
        Object[] rowArray = new Object[row.size()];
        for (int j = 0; j < row.size(); j++) {
            rowArray[j] = row.get(j);
        }
        return rowArray;
    }

    private static class FutureBackedRowIterator implements Iterator<Row> {

        private final ListenableFuture<Bucket> bucketFuture;
        private Iterator<Row> bucketIt = null;

        public FutureBackedRowIterator(ListenableFuture<Bucket> bucketFuture) {
            this.bucketFuture = bucketFuture;

        }

        @Override
        public boolean hasNext() {
            waitForFuture();
            return bucketIt.hasNext();
        }

        private void waitForFuture() {
            if (bucketIt == null) {
                try {
                    bucketIt = bucketFuture.get().iterator();
                } catch (InterruptedException | ExecutionException e) {
                    throw Throwables.propagate(e);
                }
            }
        }

        @Override
        public Row next() {
            waitForFuture();
            return bucketIt.next();
        }

        @Override
        public void remove() {
            waitForFuture();
            bucketIt.remove();
        }
    }
}
