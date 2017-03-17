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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageResultListener;
import org.elasticsearch.common.logging.ESLogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Locale;

public class PageDownstreamContext extends AbstractExecutionSubContext implements DownstreamExecutionSubContext, PageBucketReceiver {

    private final Object lock = new Object();
    private final String nodeName;
    private final boolean traceEnabled;
    private String name;
    private final PageDownstream pageDownstream;
    private final Streamer<?>[] streamers;
    private final RamAccountingContext ramAccountingContext;
    private final int numBuckets;
    private final ArrayList<SettableFuture<Bucket>> bucketFutures;
    private final BitSet allFuturesSet;
    private final BitSet exhausted;
    private final ArrayList<PageResultListener> listeners = new ArrayList<>();

    private Throwable lastFailure = null;

    public PageDownstreamContext(ESLogger logger,
                                 String nodeName,
                                 int id,
                                 String name,
                                 PageDownstream pageDownstream,
                                 Streamer<?>[] streamers,
                                 RamAccountingContext ramAccountingContext,
                                 int numBuckets) {
        super(id, logger);
        this.nodeName = nodeName;
        this.name = name;
        this.pageDownstream = pageDownstream;
        this.streamers = streamers;
        this.ramAccountingContext = ramAccountingContext;
        this.numBuckets = numBuckets;
        bucketFutures = new ArrayList<>(numBuckets);
        allFuturesSet = new BitSet(numBuckets);
        exhausted = new BitSet(numBuckets);
        initBucketFutures();
        traceEnabled = logger.isTraceEnabled();
    }

    private void initBucketFutures() {
        bucketFutures.clear();
        for (int i = 0; i < numBuckets; i++) {
            bucketFutures.add(SettableFuture.<Bucket>create());
        }
    }

    private boolean pageEmpty() {
        return allFuturesSet.cardinality() == 0;
    }

    private boolean allExhausted() {
        return exhausted.cardinality() == numBuckets;
    }

    private boolean isExhausted(int bucketIdx) {
        return exhausted.get(bucketIdx);
    }

    @Override
    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        synchronized (listeners) {
            if (lastFailure == null) {
                listeners.add(pageResultListener);
            } else {
                pageResultListener.needMore(false);
            }
        }
        synchronized (lock) {
            traceLog("method=setBucket", bucketIdx);
            if (allFuturesSet.get(bucketIdx)) {
                pageDownstream.fail(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=setBucket phaseId=%d bucket=%d",
                    nodeName, id, bucketIdx)));
                return;
            }

            if (pageEmpty()) {
                logger.trace("calling nextPage method=setBucket", bucketIdx);
                pageDownstream.nextPage(new BucketPage(bucketFutures), new ResultListenerBridgingConsumeListener());
            }
            setExhaustedUpstreams();

            if (isLast) {
                exhausted.set(bucketIdx);
            }
            bucketFutures.get(bucketIdx).set(rows);
            allFuturesSet.set(bucketIdx);

            clearPageIfFull(bucketIdx);
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
            lastFailure = throwable;
            if (allFuturesSet.get(bucketIdx)) {
                pageDownstream.fail(new IllegalStateException(String.format(Locale.ENGLISH,
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
            lastFailure = throwable;
            if (allFuturesSet.get(bucketIdx)) {
                traceLog("method=killed future already set", bucketIdx);
                return;
            }
            setBucketFailure(bucketIdx, throwable);
        }
    }

    private void setBucketFailure(int bucketIdx, Throwable throwable) {
        // can't trigger failure on pageDownstream immediately as it would remove the context which the other
        // upstreams still require
        if (pageEmpty()) {
            traceLog("calling nextPage. method=setBucketFailure", bucketIdx);
            pageDownstream.nextPage(new BucketPage(bucketFutures), new ResultListenerBridgingConsumeListener());
        }
        setExhaustedUpstreams();

        exhausted.set(bucketIdx);
        bucketFutures.get(bucketIdx).setException(throwable);
        allFuturesSet.set(bucketIdx);
        clearPageIfFull(bucketIdx);
    }

    private void clearPageIfFull(int bucketIdx) {
        if (allFuturesSet.cardinality() == numBuckets) {
            traceLog("page is full, clearing it", bucketIdx);
            allFuturesSet.clear();
            initBucketFutures();
        }
    }

    /**
     * need to set the futures of all upstreams that are exhausted as there won't come any more buckets from those upstreams
     */
    private void setExhaustedUpstreams() {
        for (int i = 0; i < exhausted.size(); i++) {
            if (exhausted.get(i)) {
                bucketFutures.get(i).set(Bucket.EMPTY);
                allFuturesSet.set(i);
            }
        }
    }

    @Override
    public Streamer<?>[] streamers() {
        return streamers;
    }

    @Override
    protected void innerClose(@Nullable Throwable throwable) {
        if (throwable == null) {
            pageDownstream.finish();
        } else {
            pageDownstream.fail(throwable);
        }
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
        lastFailure = t;
        pageDownstream.kill(t);
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
            pageDownstream.nextPage(new BucketPage(Futures.immediateFuture(Bucket.EMPTY)), new ResultListenerBridgingConsumeListener());
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
               ", allFuturesSet=" + allFuturesSet +
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

    private class ResultListenerBridgingConsumeListener implements PageConsumeListener {

        @Override
        public void needMore() {
            boolean allExhausted = allExhausted();
            synchronized (listeners) {
                if (traceEnabled) {
                    logger.trace("phase={} allExhausted={}", id, allExhausted);
                    logger.trace("calling needMore on all listeners({}) phase={}", listeners.size(), id);
                }
                for (PageResultListener listener : listeners) {
                    if (allExhausted) {
                        listener.needMore(false);
                    } else {
                        listener.needMore(!isExhausted(listener.buckedIdx()));
                    }
                }
                listeners.clear();
            }
            if (allExhausted) {
                PageDownstreamContext.this.close();
            }
        }

        @Override
        public void finish() {
            synchronized (listeners) {
                if (traceEnabled) {
                    logger.trace("calling finish() on all listeners({}) phase={}", listeners.size(), id);
                }
                for (PageResultListener listener : listeners) {
                    listener.needMore(false);
                }
                listeners.clear();
                PageDownstreamContext.this.close();
            }
        }
    }
}
