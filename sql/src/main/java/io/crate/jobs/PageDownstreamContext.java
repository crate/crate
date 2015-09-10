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

import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageResultListener;
import io.crate.operation.projectors.FlatProjectorChain;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;

public class PageDownstreamContext extends AbstractExecutionSubContext implements DownstreamExecutionSubContext {

    private static final ESLogger LOGGER = Loggers.getLogger(PageDownstreamContext.class);

    private final Object lock = new Object();
    private String name;
    private final PageDownstream pageDownstream;
    private final Streamer<?>[] streamer;
    private final RamAccountingContext ramAccountingContext;
    private final int numBuckets;
    private final ArrayList<SettableFuture<Bucket>> bucketFutures;
    private final BitSet allFuturesSet;
    private final BitSet exhausted;
    private final ArrayList<PageResultListener> listeners = new ArrayList<>();

    @Nullable
    private final FlatProjectorChain projectorChain;

    public PageDownstreamContext(int id,
                                 String name,
                                 PageDownstream pageDownstream,
                                 Streamer<?>[] streamer,
                                 RamAccountingContext ramAccountingContext,
                                 int numBuckets,
                                 @Nullable FlatProjectorChain projectorChain) {
        super(id);
        this.name = name;
        this.pageDownstream = pageDownstream;
        this.streamer = streamer;
        this.ramAccountingContext = ramAccountingContext;
        this.numBuckets = numBuckets;
        this.projectorChain = projectorChain;
        bucketFutures = new ArrayList<>(numBuckets);
        allFuturesSet = new BitSet(numBuckets);
        exhausted = new BitSet(numBuckets);
        initBucketFutures();
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

    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        synchronized (listeners) {
            listeners.add(pageResultListener);
        }
        synchronized (lock) {
            LOGGER.trace("setBucket: {}", bucketIdx);
            if (allFuturesSet.get(bucketIdx)) {
                pageDownstream.fail(new IllegalStateException("May not set the same bucket of a page more than once"));
                return;
            }

            if (pageEmpty()) {
                LOGGER.trace("calling nextPage");
                pageDownstream.nextPage(new BucketPage(bucketFutures), new ResultListenerBridgingConsumeListener());
            }
            setExhaustedUpstreams();

            if (isLast) {
                exhausted.set(bucketIdx);
            }
            bucketFutures.get(bucketIdx).set(rows);
            allFuturesSet.set(bucketIdx);

            clearPageIfFull();
        }
    }

    public synchronized void failure(int bucketIdx, Throwable throwable) {
        // can't trigger failure on pageDownstream immediately as it would remove the context which the other
        // upstreams still require
        synchronized (lock) {
            LOGGER.trace("failure: bucket: {} {}", bucketIdx, throwable);
            if (allFuturesSet.get(bucketIdx)) {
                pageDownstream.fail(new IllegalStateException("May not set the same bucket %d of a page more than once"));
                return;
            }
            if (pageEmpty()) {
                LOGGER.trace("calling nextPage");
                pageDownstream.nextPage(new BucketPage(bucketFutures), new ResultListenerBridgingConsumeListener());
            }
            setExhaustedUpstreams();

            LOGGER.trace("failure: {}", bucketIdx);
            exhausted.set(bucketIdx);
            bucketFutures.get(bucketIdx).setException(throwable);
            allFuturesSet.set(bucketIdx);
            clearPageIfFull();
        }
    }

    private void clearPageIfFull() {
        if (allFuturesSet.cardinality() == numBuckets) {
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

    public Streamer<?>[] streamer() {
        return streamer;
    }

    @Override
    protected void innerClose(@Nullable Throwable throwable) {
        if (throwable == null){
            pageDownstream.finish();
        } else {
            pageDownstream.fail(throwable);
        }

        future.bytesUsed(ramAccountingContext.totalBytes());
        ramAccountingContext.close();
    }

    @Override
    protected void innerKill(@Nullable Throwable t) {
        innerClose(t);
    }

    @Override
    public void innerStart() {
        if (projectorChain != null) {
            projectorChain.startProjections(this);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Nullable
    @Override
    public PageDownstreamContext pageDownstreamContext(byte inputId) {
        assert inputId == 0 : "This downstream context only supports 1 input";
        return this;
    }

    private class ResultListenerBridgingConsumeListener implements PageConsumeListener {

        @Override
        public void needMore() {
            boolean allExhausted = allExhausted();
            LOGGER.trace("allExhausted: {}", allExhausted);
            synchronized (listeners) {
                LOGGER.trace("calling needMore on all listeners({})", listeners.size());
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
                LOGGER.trace("calling finish() on all listeners({})", listeners.size());
                for (PageResultListener listener : listeners) {
                    listener.needMore(false);
                }
                listeners.clear();
                PageDownstreamContext.this.close();
            }
        }
    }
}
