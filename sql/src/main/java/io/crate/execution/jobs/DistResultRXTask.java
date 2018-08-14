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

package io.crate.execution.jobs;

import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.engine.distribution.merge.BatchPagingIterator;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * A {@link DownstreamRXTask} which receives paged buckets from upstreams
 * and forwards the merged bucket results to the consumers for further processing.
 */
public class DistResultRXTask extends AbstractTask implements DownstreamRXTask {

    private final String name;
    private final Object lock = new Object();
    private final Executor executor;
    private final RamAccountingContext ramAccountingContext;
    private final int numBuckets;
    private final Set<Integer> buckets;
    private final Set<Integer> exhausted;
    private final PagingIterator<Integer, Row> pagingIterator;
    private final Map<Integer, PageResultListener> listenersByBucketIdx;
    private final Map<Integer, Bucket> bucketsByIdx;
    private final RowConsumer consumer;
    private final BatchPagingIterator<Integer> batchPagingIterator;
    private final PageBucketReceiver pageBucketReceiver;

    private Throwable[] lastThrowable = new Throwable[1];
    private volatile boolean receivingFirstPage = true;

    public DistResultRXTask(Logger logger,
                            String nodeName,
                            int id,
                            String name,
                            Executor executor,
                            RowConsumer rowConsumer,
                            PagingIterator<Integer, Row> pagingIterator,
                            Streamer<?>[] streamers,
                            RamAccountingContext ramAccountingContext,
                            BucketReceiverFactory.Type bucketReceiverType,
                            int numBuckets) {
        super(id, logger);
        this.name = name;
        this.executor = executor;
        this.ramAccountingContext = ramAccountingContext;
        this.numBuckets = numBuckets;
        this.buckets = new HashSet<>(numBuckets);
        this.exhausted = new HashSet<>(numBuckets);
        this.pagingIterator = pagingIterator;
        this.bucketsByIdx = new HashMap<>(numBuckets);
        this.listenersByBucketIdx = new HashMap<>(numBuckets);
        batchPagingIterator = new BatchPagingIterator<>(
            pagingIterator,
            this::fetchMore,
            this::allUpstreamsExhausted,
            () -> releaseListenersAndCloseContext(null)
        );
        this.consumer = rowConsumer;
        if (bucketReceiverType.equals(BucketReceiverFactory.Type.MERGE_BUCKETS)) {
            pageBucketReceiver = BucketReceiverFactory.createMergeBucketsReceiver(logger, nodeName, id, streamers, buckets,
                exhausted, bucketsByIdx, listenersByBucketIdx, this::kill, this::triggerConsumer, this::merge, lastThrowable, numBuckets);
        } else {
            throw new IllegalArgumentException("We only support merge buckets result receivers");
        }
    }

    private void releaseListenersAndCloseContext(@Nullable Throwable throwable) {
        synchronized (buckets) {
            for (PageResultListener resultListener : listenersByBucketIdx.values()) {
                resultListener.needMore(false);
            }
            listenersByBucketIdx.clear();
        }
        close(throwable);
    }

    private boolean allUpstreamsExhausted() {
        return exhausted.size() == numBuckets;
    }

    private void triggerConsumer() {
        boolean invokeConsumer = false;
        Throwable throwable;
        synchronized (lock) {
            if (receivingFirstPage) {
                receivingFirstPage = false;
                invokeConsumer = true;
            }
            throwable = lastThrowable[0];
        }
        final Throwable error = throwable;
        if (invokeConsumer) {
            try {
                executor.execute(() -> consumer.accept(batchPagingIterator, error));
            } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                consumer.accept(null, e);
                throwable = e;
            }
        } else {
            try {
                executor.execute(() -> batchPagingIterator.completeLoad(error));
            } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                batchPagingIterator.completeLoad(e);
                throwable = e;
            }
        }
        if (throwable != null) {
            releaseListenersAndCloseContext(throwable);
        }
    }

    private void merge(Map<Integer, Bucket> bucketsByIdx) {
        try {
            List<KeyIterable<Integer, Row>> buckets = new ArrayList<>(numBuckets);
            synchronized (lock) {
                for (Map.Entry<Integer, Bucket> entry : bucketsByIdx.entrySet()) {
                    buckets.add(new KeyIterable<>(entry.getKey(), entry.getValue()));
                }
                bucketsByIdx.clear();
            }
            pagingIterator.merge(buckets);
        } catch (Throwable t) {
            innerKill(t);
            // the iterator already returned it's loadNextBatch future, we must complete it exceptionally
            batchPagingIterator.completeLoad(t);
            return;
        }
        if (allUpstreamsExhausted()) {
            pagingIterator.finish();
        }
    }

    private boolean fetchMore(Integer exhaustedBucket) {
        if (allUpstreamsExhausted()) {
            return false;
        }
        if (exhaustedBucket == null || exhausted.contains(exhaustedBucket)) {
            fetchFromUnExhausted();
        } else {
            fetchExhausted(exhaustedBucket);
        }
        return true;
    }

    private void fetchExhausted(Integer exhaustedBucket) {
        synchronized (buckets) {
            for (Integer bucketIdx : buckets) {
                if (!bucketIdx.equals(exhaustedBucket)) {
                    setToEmptyBucket(bucketIdx);
                }
            }
            PageResultListener pageResultListener = listenersByBucketIdx.remove(exhaustedBucket);
            pageResultListener.needMore(true);
        }
    }

    private void fetchFromUnExhausted() {
        synchronized (buckets) {
            for (Integer bucketIdx : buckets) {
                if (exhausted.contains(bucketIdx)) {
                    setToEmptyBucket(bucketIdx);
                } else {
                    PageResultListener resultListener = listenersByBucketIdx.remove(bucketIdx);
                    resultListener.needMore(true);
                }
            }
        }
    }

    private void setToEmptyBucket(int idx) {
        bucketsByIdx.putIfAbsent(idx, Bucket.EMPTY);
    }

    @Override
    protected void innerClose(@Nullable Throwable throwable) {
        setBytesUsed(ramAccountingContext.totalBytes());
        ramAccountingContext.close();
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
        boolean shouldTriggerConsumer = false;
        synchronized (lock) {
            lastThrowable[0] = t;
            batchPagingIterator.kill(t); // this causes a already active consumer to fail
            batchPagingIterator.close();
            if (receivingFirstPage) {
                // no active consumer - can "activate" it with a failure
                receivingFirstPage = false;
                shouldTriggerConsumer = true;
            }
        }
        if (shouldTriggerConsumer) {
            consumer.accept(null, t);
        }
    }

    @Override
    protected void innerStart() {
        // E.g. If the upstreamPhase is a collectPhase for a partitioned table without any partitions
        // there won't be any executionNodes for that collectPhase
        // -> no upstreams -> just finish
        if (numBuckets == 0) {
            consumer.accept(batchPagingIterator, lastThrowable[0]);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "DistResultRXTask{" +
               "id=" + id() +
               ", numBuckets=" + numBuckets +
               ", exhausted=" + exhausted +
               ", closed=" + isClosed() +
               '}';
    }

    /**
     * The default behavior is to receive all upstream buckets,
     * regardless of the input id. For a {@link DownstreamRXTask}
     * which uses the inputId, see {@link JoinTask}.
     */
    @Nullable
    @Override
    public PageBucketReceiver getBucketReceiver(byte inputId) {
        return pageBucketReceiver;
    }
}
