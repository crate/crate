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
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.engine.distribution.merge.BatchPagingIterator;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import io.netty.util.collection.IntObjectHashMap;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static io.crate.concurrent.CompletableFutures.failedFuture;

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
    @GuardedBy("lock")
    private final Set<Integer> exhausted;
    private final Map<Integer, PageResultListener> listenersByBucketIdx;
    @GuardedBy("lock")
    private final Map<Integer, Bucket> bucketsByIdx;
    private final RowConsumer consumer;
    private final PagingIterator<Integer, Row> pagingIterator;
    private final BatchIterator<Row> batchPagingIterator;
    private final Logger logger;
    private final CompletableFuture<?> processingFuture = new CompletableFuture<>();

    private Throwable lastThrowable = null;
    private volatile CompletableFuture<List<KeyIterable<Integer, Row>>> currentPage = new CompletableFuture<>();
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

        this.exhausted = Collections.newSetFromMap(new IntObjectHashMap<>(numBuckets));
        this.bucketsByIdx = new IntObjectHashMap<>(numBuckets);
        this.listenersByBucketIdx = new IntObjectHashMap<>(numBuckets);
        processingFuture.whenComplete((result, ex) -> {
            synchronized (listenersByBucketIdx) {
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
            throwable -> {
                if (throwable == null) {
                    processingFuture.complete(null);
                } else {
                    processingFuture.completeExceptionally(throwable);
                }
            }
        );
        traceEnabled = logger.isTraceEnabled();
    }

    @Override
    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        final boolean isLastOrHasError;
        synchronized (listenersByBucketIdx) {
            isLastOrHasError = isLast || lastThrowable != null;
            if (!isLastOrHasError) {
                listenersByBucketIdx.put(bucketIdx, pageResultListener);
            }
        }
        if (isLastOrHasError) {
            pageResultListener.needMore(false);
        }
        final boolean allBucketsOfPageReceived;
        synchronized (lock) {
            traceLog("method=setBucket", bucketIdx);

            if (bucketsByIdx.putIfAbsent(bucketIdx, rows) != null) {
                processingFuture.completeExceptionally(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=setBucket phaseId=%d bucket=%d",
                    nodeName, phaseId, bucketIdx)));
            }
            if (isLast) {
                exhausted.add(bucketIdx);
            }
            allBucketsOfPageReceived = bucketsByIdx.size() == numBuckets;
        }
        if (allBucketsOfPageReceived) {
            processPage();
        }
    }

    private void triggerConsumerOrPageFuture(List<KeyIterable<Integer, Row>> buckets) {
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
            if (error == null) {
                try {
                    pagingIterator.merge(buckets);
                    executor.execute(this::consumeRows);
                } catch (RejectedExecutionException e) {
                    consumer.accept(null, e);
                    throwable = e;
                }
            } else {
                consumer.accept(null, error);
            }
        } else {
            if (error == null) {
                try {
                    executor.execute(() -> currentPage.complete(buckets));
                } catch (RejectedExecutionException e) {
                    currentPage.completeExceptionally(e);
                    throwable = e;
                }
            } else {
                currentPage.completeExceptionally(error);
            }
        }
        if (throwable != null) {
            processingFuture.completeExceptionally(throwable);
        }
    }

    private void processPage() {
        List<KeyIterable<Integer, Row>> buckets;
        try {
            buckets = getBuckets();
        } catch (Throwable t) {
            kill(t);
            // the iterator already returned it's loadNextBatch future, we must complete it exceptionally
            currentPage.completeExceptionally(t);
            return;
        }
        if (allUpstreamsExhausted()) {
            pagingIterator.finish();
        }
        triggerConsumerOrPageFuture(buckets);
    }

    private List<KeyIterable<Integer, Row>> getBuckets() {
        List<KeyIterable<Integer, Row>> buckets = new ArrayList<>(numBuckets);
        synchronized (lock) {
            Iterator<Map.Entry<Integer, Bucket>> entryIt = bucketsByIdx.entrySet().iterator();
            while (entryIt.hasNext()) {
                Map.Entry<Integer, Bucket> entry = entryIt.next();
                Integer bucketIdx = entry.getKey();
                buckets.add(new KeyIterable<>(bucketIdx, entry.getValue()));
                if (exhausted.contains(bucketIdx)) {
                    entry.setValue(Bucket.EMPTY);
                } else {
                    entryIt.remove();
                }
            }
        }
        return buckets;
    }

    private boolean allUpstreamsExhausted() {
        return exhausted.size() == numBuckets;
    }

    private CompletableFuture<? extends Iterable<? extends KeyIterable<Integer, Row>>> fetchMore(Integer exhaustedBucket) {
        if (allUpstreamsExhausted()) {
            return failedFuture(new IllegalStateException("Source is exhausted"));
        }
        currentPage = new CompletableFuture<>();
        if (exhaustedBucket == null || exhausted.contains(exhaustedBucket)) {
            fetchFromUnExhausted();
        } else {
            fetchExhausted(exhaustedBucket);
        }
        return currentPage;
    }

    private void fetchExhausted(Integer exhaustedBucket) {
        synchronized (listenersByBucketIdx) {
            // We're only requesting data for 1 specific bucket,
            // so we need to fill in other buckets to meet the
            // "receivedAllBucketsOfPage" condition once we get the data for this bucket
            PageResultListener pageResultListener = listenersByBucketIdx.remove(exhaustedBucket);
            for (Integer bucketIdx : listenersByBucketIdx.keySet()) {
                bucketsByIdx.putIfAbsent(bucketIdx, Bucket.EMPTY);
            }
            pageResultListener.needMore(true);
        }
    }

    private void fetchFromUnExhausted() {
        synchronized (listenersByBucketIdx) {
            for (PageResultListener listener : listenersByBucketIdx.values()) {
                listener.needMore(true);
            }
            listenersByBucketIdx.clear();
        }
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
