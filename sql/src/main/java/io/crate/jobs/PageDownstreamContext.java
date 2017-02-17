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

package io.crate.jobs;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.operation.PageResultListener;
import io.crate.operation.merge.BatchPagingIterator;
import io.crate.operation.merge.KeyIterable;
import io.crate.operation.merge.PagingIterator;
import io.crate.operation.projectors.BatchConsumerToRowReceiver;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;

public class PageDownstreamContext extends AbstractExecutionSubContext implements DownstreamExecutionSubContext, PageBucketReceiver {

    private final String name;
    private final Object lock = new Object();
    private final String nodeName;
    private final boolean traceEnabled;
    private final Streamer<?>[] streamers;
    private final RamAccountingContext ramAccountingContext;
    private final int numBuckets;
    private final BitSet exhausted;
    private final PagingIterator<Integer, Row> pagingIterator;
    private final IntObjectHashMap<PageResultListener> listenersByBucketIdx;
    private final IntObjectHashMap<Bucket> bucketsByIdx;
    private final BatchConsumerToRowReceiver consumer;
    private final BatchPagingIterator batchIterator;
    private final RowReceiver rowReceiver;

    private Throwable lastThrowable = null;
    private boolean receivingFirstPage = true;

    public PageDownstreamContext(ESLogger logger,
                                 String nodeName,
                                 int id,
                                 String name,
                                 RowReceiver rowReceiver,
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
        this.rowReceiver = rowReceiver;
        traceEnabled = logger.isTraceEnabled();
        this.exhausted = new BitSet(numBuckets);
        this.pagingIterator = pagingIterator;
        this.bucketsByIdx = new IntObjectHashMap<>(numBuckets);
        this.listenersByBucketIdx = new IntObjectHashMap<>(numBuckets);
        this.consumer = new BatchConsumerToRowReceiver(rowReceiver);
        this.batchIterator = new BatchPagingIterator(
            pagingIterator,
            this::fetchMore,
            this::allUpstreamsExhausted,
            this::releaseListenersAndCloseContext
        );
    }

    private void releaseListenersAndCloseContext() {
        for (ObjectCursor<PageResultListener> cursor : listenersByBucketIdx.values()) {
            cursor.value.needMore(false);
        }
        listenersByBucketIdx.clear();
        close();
    }

    private boolean allUpstreamsExhausted() {
        return exhausted.cardinality() == numBuckets;
    }

    @Override
    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        synchronized (listenersByBucketIdx) {
            listenersByBucketIdx.put(bucketIdx, pageResultListener);
        }
        synchronized (lock) {
            traceLog("method=setBucket", bucketIdx);

            if (bucketsByIdx.putIfAbsent(bucketIdx, rows) == false) {
                rowReceiver.fail(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=setBucket phaseId=%d bucket=%d",
                    nodeName, id, bucketIdx)));
                return;
            }
            setExhaustedUpstreams();
            if (isLast) {
                exhausted.set(bucketIdx);
            }
            if (bucketsByIdx.size() == numBuckets) {
                triggerConsumption();
            }
        }
    }

    private void triggerConsumption() {
        mergeBuckets();
        if (allUpstreamsExhausted()) {
            pagingIterator.finish();
        }
        if (receivingFirstPage) {
            consumer.accept(batchIterator, lastThrowable);
            receivingFirstPage = false;
        } else {
            batchIterator.completeLoad(lastThrowable);
        }
    }

    private void mergeBuckets() {
        List<KeyIterable<Integer, Row>> buckets = new ArrayList<>(numBuckets);
        for (IntObjectCursor<Bucket> cursor : bucketsByIdx) {
            buckets.add(new KeyIterable<>(cursor.key, cursor.value));
        }
        bucketsByIdx.clear();
        pagingIterator.merge(buckets);
    }

    private boolean fetchMore(Integer exhaustedBucket) {
        if (exhausted.cardinality() == numBuckets) {
            return false;
        }
        if (exhaustedBucket == null || exhausted.get(exhaustedBucket)) {
            fetchFromUnExhausted();
        } else {
            fetchExhausted(exhaustedBucket);
        }
        return true;
    }

    private void fetchExhausted(Integer exhaustedBucket) {
        for (int i = 0; i < numBuckets; i++) {
            if (exhaustedBucket.equals(i) == false) {
                setToEmptyBucket(i);
            }
        }
        PageResultListener pageResultListener = listenersByBucketIdx.remove(exhaustedBucket);
        pageResultListener.needMore(true);
    }

    private void fetchFromUnExhausted() {
        for (int idx = 0; idx < numBuckets; idx++) {
            if (exhausted.get(idx)) {
                setToEmptyBucket(idx);
            } else {
                PageResultListener resultListener = listenersByBucketIdx.remove(idx);
                resultListener.needMore(true);
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
        synchronized (lock) {
            if (bucketsByIdx.putIfAbsent(bucketIdx, Bucket.EMPTY) == false) {
                rowReceiver.fail(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=failure phaseId=%d bucket=%d",
                    nodeName, id(), bucketIdx)));
                return;
            }
            setBucketFailure(bucketIdx, throwable);
        }
    }

    @Override
    public void killed(int bucketIdx, Throwable throwable) {
        traceLog("method=killed", bucketIdx, throwable);
        synchronized (lock) {
            if (bucketsByIdx.putIfAbsent(bucketIdx, Bucket.EMPTY) == false) {
                traceLog("method=killed future already set", bucketIdx);
                return;
            }
            setBucketFailure(bucketIdx, throwable);
        }
    }

    private void setBucketFailure(int bucketIdx, Throwable throwable) {
        // can't trigger failure on pageDownstream immediately as it would remove the context which the other
        // upstreams still require

        exhausted.set(bucketIdx);
        if (bucketsByIdx.size() == numBuckets) {
            rowReceiver.fail(throwable);
            releaseListenersAndCloseContext();
        } else {
            lastThrowable = throwable;
        }
    }

    /**
     * need to set the futures of all upstreams that are exhausted as there won't come any more buckets from those upstreams
     */
    private void setExhaustedUpstreams() {
        exhausted.stream().forEach(this::setToEmptyBucket);
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
        rowReceiver.kill(t);
    }

    @Override
    public void cleanup() {
        future.bytesUsed(ramAccountingContext.totalBytes());
        ramAccountingContext.close();
    }

    @Override
    protected void innerStart() {
        // E.g. If the upstreamPhase is a collectPhase for a partitioned table without any partitions
        // there won't be any executionNodes for that collectPhase
        // -> no upstreams -> just finish
        if (numBuckets == 0) {
            rowReceiver.finish(new RepeatHandle() {
                @Override
                public void repeat() {
                    rowReceiver.finish(this);
                }
            });
            close();
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
               ", closed=" + future.closed() +
               '}';
    }

    @Nullable
    @Override
    public PageBucketReceiver getBucketReceiver(byte inputId) {
        assert inputId == 0 : "This downstream context only supports 1 input";
        return this;
    }
}
