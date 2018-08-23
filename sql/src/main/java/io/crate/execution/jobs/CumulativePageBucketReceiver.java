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

package io.crate.execution.jobs;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.engine.distribution.merge.BatchPagingIterator;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import io.netty.util.collection.IntObjectHashMap;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * A {@link PageBucketReceiver} which receives buckets from upstreams, wait to receive the page from all upstreams
 * and forwards the merged bucket results to the consumers for further processing. It then continues to receive
 * the buckets from the next page from all upstreams.
 */
public class CumulativePageBucketReceiver implements PageBucketReceiver {

    private final Object lock = new Object();
    private final String nodeName;
    private final boolean traceEnabled;
    private final int phaseId;
    private final Executor executor;
    private final Streamer<?>[] streamers;
    private final int numBuckets;
    @GuardedBy("buckets")
    private final Set<Integer> buckets;
    @GuardedBy("lock")
    private final Set<Integer> exhausted;
    @GuardedBy("buckets")
    private final Map<Integer, PageResultListener> listenersByBucketIdx;
    @GuardedBy("lock")
    private final Map<Integer, Bucket> bucketsByIdx;
    private final RowConsumer consumer;
    private final PagingIterator<Integer, Row> pagingIterator;
    private final BatchPagingIterator<Integer> batchPagingIterator;
    private final Logger logger;
    private final CompletableFuture<?> processingFuture = new CompletableFuture<>();

    private Throwable lastThrowable = null;
    private volatile boolean receivingFirstPage = true;

    public CumulativePageBucketReceiver(Logger logger,
                                        String nodeName,
                                        int phaseId,
                                        Executor executor,
                                        Streamer<?>[] streamers,
                                        RowConsumer rowConsumer,
                                        PagingIterator<Integer, Row> pagingIterator,
                                        int numBuckets) {
        this.logger = logger;
        this.nodeName = nodeName;
        this.phaseId = phaseId;
        this.executor = executor;
        this.streamers = streamers;
        this.consumer = rowConsumer;
        this.pagingIterator = pagingIterator;
        this.numBuckets = numBuckets;

        this.buckets = Collections.newSetFromMap(new IntObjectHashMap<>(numBuckets));
        this.exhausted = Collections.newSetFromMap(new IntObjectHashMap<>(numBuckets));
        this.bucketsByIdx = new IntObjectHashMap<>(numBuckets);
        this.listenersByBucketIdx = new IntObjectHashMap<>(numBuckets);
        processingFuture.whenComplete((result, ex) -> {
            synchronized (buckets) {
                for (PageResultListener resultListener : listenersByBucketIdx.values()) {
                    resultListener.needMore(false);
                }
                listenersByBucketIdx.clear();
            }
        });
        batchPagingIterator = new BatchPagingIterator<>(
            pagingIterator,
            this::fetchMore,
            this::allUpstreamsExhausted,
            () -> processingFuture.complete(null)
        );
        traceEnabled = logger.isTraceEnabled();
    }

    @Override
    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        synchronized (buckets) {
            buckets.add(bucketIdx);
            if (lastThrowable == null) {
                listenersByBucketIdx.put(bucketIdx, pageResultListener);
            } else {
                pageResultListener.needMore(false);
            }
        }
        boolean shouldTriggerConsumer = false;
        synchronized (lock) {
            traceLog("method=setBucket", bucketIdx);

            if (bucketsByIdx.putIfAbsent(bucketIdx, rows) != null) {
                processingFuture.completeExceptionally(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=setBucket phaseId=%d bucket=%d",
                    nodeName, phaseId, bucketIdx)));
            }
            setExhaustedUpstreams();
            if (isLast) {
                exhausted.add(bucketIdx);
            }
            if (bucketsByIdx.size() == numBuckets) {
                shouldTriggerConsumer = true;
            }
        }
        if (shouldTriggerConsumer) {
            mergeAndTriggerConsumer();
        } else if (isLast) {
            // release listener early here, otherwise other upstreams will be blocked
            // e.g. if 2 downstream contexts are used in the chain
            //      Phase -> DistributingDownstream -> Phase -> DistributingDownstream
            pageResultListener.needMore(false);
        }
    }

    private void triggerConsumer() {
        boolean invokeConsumer = false;
        Throwable throwable;
        synchronized (lock) {
            if (receivingFirstPage) {
                receivingFirstPage = false;
                invokeConsumer = true;
            }
            throwable = lastThrowable;
        }
        final Throwable error = throwable;
        if (invokeConsumer) {
            try {
                executor.execute(this::consumeRows);
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
            processingFuture.completeExceptionally(throwable);
        }
    }

    private void mergeAndTriggerConsumer() {
        try {
            mergeBuckets();
        } catch (Throwable t) {
            kill(t);
            // the iterator already returned it's loadNextBatch future, we must complete it exceptionally
            batchPagingIterator.completeLoad(t);
            return;
        }
        if (allUpstreamsExhausted()) {
            pagingIterator.finish();
        }
        triggerConsumer();
    }

    private void mergeBuckets() {
        List<KeyIterable<Integer, Row>> buckets = new ArrayList<>(numBuckets);
        synchronized (lock) {
            for (Map.Entry<Integer, Bucket> entry : bucketsByIdx.entrySet()) {
                buckets.add(new KeyIterable<>(entry.getKey(), entry.getValue()));
            }
            bucketsByIdx.clear();
        }
        pagingIterator.merge(buckets);
    }

    private boolean allUpstreamsExhausted() {
        return exhausted.size() == numBuckets;
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

    /**
     * need to set the futures of all upstreams that are exhausted as there won't come any more buckets from those upstreams
     */
    private void setExhaustedUpstreams() {
        exhausted.forEach(this::setToEmptyBucket);
    }

    private void setToEmptyBucket(int idx) {
        bucketsByIdx.putIfAbsent(idx, Bucket.EMPTY);
    }

    private void traceLog(String msg, int bucketIdx) {
        if (traceEnabled) {
            logger.trace("{} phaseId={} bucket={}", msg, phaseId, bucketIdx);
        }
    }

    @Override
    public Streamer<?>[] streamers() {
        return streamers;
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return processingFuture;
    }

    @Override
    public void consumeRows() {
        consumer.accept(batchPagingIterator, lastThrowable);
    }

    @Override
    public void kill(@Nonnull Throwable t) {
        boolean shouldTriggerConsumer = false;
        synchronized (lock) {
            lastThrowable = t;
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
}
