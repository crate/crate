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

package io.crate.operation.fetch;

import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Merging node response buckets together and emitting rows as soon as possible.
 * A bucket is always ordered by a positional unique long.
 * Buckets from one node can flow in in an undefined order, so every bucket from the same node
 * must be merged with existing buckets from this node.
 * A bucket from one node can have gaps which must be filled by buckets from other nodes.
 * A gap inside a bucket can never be filled by a bucket of the same node.
 * Main purpose of this implementation is merging ordered node responses.
 */
public class PositionalBucketMerger implements RowUpstream {

    private RowDownstreamHandle downstream;
    private final AtomicInteger upstreamsRemaining = new AtomicInteger(0);
    private final int orderingColumnIndex;
    private final UpstreamBucket[] remainingBuckets;
    private volatile long outputCursor = 0;
    private volatile long leastBucketCursor = -1;
    private volatile int leastBucketId = -1;
    private final AtomicBoolean consumeBuckets = new AtomicBoolean(true);

    public PositionalBucketMerger(RowDownstream downstream,
                                  int numUpstreams,
                                  int orderingColumnIndex) {
        downstream(downstream);
        this.orderingColumnIndex = orderingColumnIndex;
        remainingBuckets = new UpstreamBucket[numUpstreams];
    }

    public synchronized boolean setNextBucket(List<Row> bucket, int upstreamId) {
        if (!consumeBuckets.get()) {
            return false;
        }
        Iterator<Row> bucketIt = bucket.iterator();
        while (bucketIt.hasNext()) {
            Row firstRow = bucketIt.next();
            if (firstRow.get(orderingColumnIndex) == outputCursor) {
                bucketIt.remove();
                if (!emitRow(firstRow)) {
                    return false;
                }
            } else {
                break;
            }
        }
        if (bucket.size() > 0) {
            mergeBucket(bucket, upstreamId);
        }
        return emitBuckets();
    }

    public void mergeBucket(List<Row> newBucket, int upstreamId) {
        UpstreamBucket remainingBucket = remainingBuckets[upstreamId];
        if (remainingBucket == null) {
            remainingBuckets[upstreamId] = new UpstreamBucket(newBucket);
        } else if (remainingBucket.size() > 0) {
            Row newFirstRow = newBucket.get(0);
            Iterator<Row> bucketIt = remainingBucket.iterator();
            int idx = 0;
            while(bucketIt.hasNext()) {
                Row row = bucketIt.next();
                int compare = Long.compare((Long) row.get(orderingColumnIndex), (Long) newFirstRow.get(orderingColumnIndex));
                if (compare == 1) {
                    remainingBucket.addAll(idx, newBucket);
                    return;
                }
                idx++;
            }
            remainingBucket.addAll(remainingBucket.size(), newBucket);
        } else {
            remainingBucket.addAll(0, newBucket);
        }
    }


    public boolean emitBuckets() {
        if (leastBucketCursor != outputCursor || leastBucketId == -1) {
            findLeastBucketIt();
        }

        while (leastBucketCursor == outputCursor && leastBucketId != -1) {
            if (!emitRow(remainingBuckets[leastBucketId].poll())) {
                return false;
            }
            if (upstreamsRemaining.get() > 0) {
                findLeastBucketIt();
            }
        }

        return true;
    }

    private void findLeastBucketIt() {
        for (int i = 0; i < remainingBuckets.length; i++) {
            UpstreamBucket bucketIt = remainingBuckets[i];
            if (bucketIt == null || bucketIt.size() == 0) {
                continue;
            }
            try {
                Row row = bucketIt.getFirst();
                Long orderingValue = (Long)row.get(orderingColumnIndex);
                if (orderingValue == outputCursor) {
                    leastBucketCursor = orderingValue;
                    leastBucketId = i;
                    return;
                } else if (orderingValue <= leastBucketCursor) {
                    leastBucketCursor = orderingValue;
                    leastBucketId = i;
                }
            } catch (NoSuchElementException e) {
                // continue
            }
        }
    }

    private boolean emitRow(Row row) {
        outputCursor++;
        return downstream.setNextRow(row);
    }


    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    public PositionalBucketMerger registerUpstream(RowUpstream upstream) {
        upstreamsRemaining.incrementAndGet();
        return this;
    }

    public void finish() {
        if (upstreamsRemaining.decrementAndGet() <= 0) {
            downstream.finish();
        }
    }

    public void fail(Throwable throwable) {
        consumeBuckets.set(false);
        downstream.fail(throwable);
    }


    public static class UpstreamBucket implements Bucket {
        private LinkedList<Row> rows;

        public UpstreamBucket(LinkedList<Row> rows) {
            this.rows = rows;
        }

        public UpstreamBucket(Collection<Row> rows) {
            this(new LinkedList<>(rows));
        }

        @Override
        public int size() {
            return rows.size();
        }

        @Override
        public Iterator<Row> iterator() {
            return rows.iterator();
        }

        public Row getFirst() {
            return rows.getFirst();
        }

        public Row poll() {
            return rows.poll();
        }

        public void addAll(int idx, Collection<Row> otherBucket) {
            rows.addAll(idx, otherBucket);
        }
    }
}
