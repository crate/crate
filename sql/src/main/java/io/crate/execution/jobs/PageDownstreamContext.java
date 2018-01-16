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
import io.crate.operation.PageResultListener;
import io.crate.execution.engine.distribution.merge.BatchPagingIterator;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DownstreamExecutionSubContext} which receives paged buckets from upstreams
 * and forwards the merged bucket results to the consumers for further processing.
 */
public class PageDownstreamContext extends AbstractExecutionSubContext implements DownstreamExecutionSubContext, PageBucketReceiver {

    private final String name;
    private final Object lock = new Object();
    private final String nodeName;
    private final boolean traceEnabled;
    private final Streamer<?>[] streamers;
    private final RamAccountingContext ramAccountingContext;
    private final int numBuckets;
    private final Set<Integer> buckets;
    private final Set<Integer> exhausted;
    private final PagingIterator<Integer, Row> pagingIterator;
    private final Map<Integer, PageResultListener> listenersByBucketIdx;
    private final Map<Integer, Bucket> bucketsByIdx;
    private final RowConsumer consumer;
    private final BatchPagingIterator<Integer> batchPagingIterator;

    private Throwable lastThrowable = null;
    private volatile boolean receivingFirstPage = true;

    public PageDownstreamContext(Logger logger,
                                 String nodeName,
                                 int id,
                                 String name,
                                 RowConsumer rowConsumer,
                                 PagingIterator<Integer, Row> pagingIterator,
                                 Streamer<?>[] streamers,
                                 RamAccountingContext ramAccountingContext,
                                 int numBuckets) {
        super(id, logger);
        this.nodeName = nodeName;
        this.name = name;
        this.streamers = streamers;
        this.ramAccountingContext = ramAccountingContext;
        this.numBuckets = numBuckets;
        this.buckets = new HashSet<>(numBuckets);
        traceEnabled = logger.isTraceEnabled();
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
                kill(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=setBucket phaseId=%d bucket=%d",
                    nodeName, id, bucketIdx)));
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
        if (invokeConsumer) {
            consumer.accept(batchPagingIterator, throwable);
        } else {
            batchPagingIterator.completeLoad(throwable);
        }
        if (throwable != null) {
            releaseListenersAndCloseContext(throwable);
        }
    }

    private void mergeAndTriggerConsumer() {
        try {
            mergeBuckets();
        } catch (Throwable t) {
            innerKill(t);
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

    private void traceLog(String msg, int bucketIdx) {
        if (traceEnabled) {
            logger.trace("{} phaseId={} bucket={}", msg, id, bucketIdx);
        }
    }

    private void traceLog(String msg, int bucketIdx, Throwable t) {
        if (traceEnabled) {
            logger.trace("{} phaseId={} bucket={} throwable={}", msg, id, bucketIdx, t);
        }
    }

    @Override
    public void failure(int bucketIdx, Throwable throwable) {
        traceLog("method=failure", bucketIdx, throwable);

        boolean shouldTriggerConsumer;
        synchronized (lock) {
            if (bucketsByIdx.putIfAbsent(bucketIdx, Bucket.EMPTY) != null) {
                kill(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=failure phaseId=%d bucket=%d",
                    nodeName, id(), bucketIdx)));
                return;
            }
            shouldTriggerConsumer = setBucketFailure(bucketIdx, throwable);
        }
        if (shouldTriggerConsumer) {
            triggerConsumer();
        }
    }

    @Override
    public void killed(int bucketIdx, Throwable throwable) {
        traceLog("method=killed", bucketIdx, throwable);

        boolean shouldTriggerConsumer;
        synchronized (lock) {
            if (bucketsByIdx.putIfAbsent(bucketIdx, Bucket.EMPTY) != null) {
                traceLog("method=killed future already set", bucketIdx);
                return;
            }
            shouldTriggerConsumer = setBucketFailure(bucketIdx, throwable);
        }
        if (shouldTriggerConsumer) {
            triggerConsumer();
        }
    }

    private boolean setBucketFailure(int bucketIdx, Throwable throwable) {
        // can't trigger failure on pageDownstream immediately as it would remove the context which the other
        // upstreams still require

        lastThrowable = throwable;
        exhausted.add(bucketIdx);
        return bucketsByIdx.size() == numBuckets;
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

    @Override
    public Streamer<?>[] streamers() {
        return streamers;
    }

    @Override
    protected void innerClose(@Nullable Throwable throwable) {
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
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

    @Override
    public void cleanup() {
        setBytesUsed(ramAccountingContext.totalBytes());
        ramAccountingContext.close();
    }

    @Override
    protected void innerStart() {
        // E.g. If the upstreamPhase is a collectPhase for a partitioned table without any partitions
        // there won't be any executionNodes for that collectPhase
        // -> no upstreams -> just finish
        if (numBuckets == 0) {
            consumer.accept(batchPagingIterator, lastThrowable);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "PageDownstreamContext{" +
               "id=" + id() +
               ", numBuckets=" + numBuckets +
               ", exhausted=" + exhausted +
               ", closed=" + isClosed() +
               '}';
    }

    /**
     * The default behavior is to receive all upstream buckets,
     * regardless of the input id. For a {@link DownstreamExecutionSubContext}
     * which uses the inputId, see {@link NestedLoopContext}.
     */
    @Nullable
    @Override
    public PageBucketReceiver getBucketReceiver(byte inputId) {
        return this;
    }
}
