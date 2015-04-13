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
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.PageDownstream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.BitSet;

public class PageDownstreamContext {

    private static final ESLogger LOGGER = Loggers.getLogger(PageDownstreamContext.class);

    private final Object lock = new Object();
    private final PageDownstream pageDownstream;
    private final Streamer<?>[] streamer;
    private final int numBuckets;
    private final ArrayList<SettableFuture<Bucket>> bucketFutures;
    private final BitSet allFuturesSet;
    private final BitSet exhausted;

    private volatile boolean failed = false;

    public PageDownstreamContext(PageDownstream pageDownstream, Streamer<?>[] streamer, int numBuckets) {
        this.pageDownstream = pageDownstream;
        this.streamer = streamer;
        this.numBuckets = numBuckets;
        bucketFutures = new ArrayList<>(numBuckets);
        initBucketFutures();
        allFuturesSet = new BitSet(numBuckets);
        exhausted = new BitSet(numBuckets);
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

    public boolean allExhausted() {
        if (failed) {
            return true;
        }
        synchronized (lock) {
            return exhausted.cardinality() == numBuckets;
        }
    }

    public boolean isExhausted(int bucketIdx) {
        if (failed) {
            return true;
        }
        synchronized (lock) {
            return exhausted.get(bucketIdx);
        }
    }

    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageConsumeListener pageConsumeListener) {
        if (failed) {
            return;
        }
        synchronized (lock) {
            LOGGER.trace("setBucket: {}", bucketIdx);

            if (pageEmpty()) {
                LOGGER.trace("calling nextPage");
                pageDownstream.nextPage(new BucketPage(bucketFutures), pageConsumeListener);
            }

            if (isLast) {
                exhausted.set(bucketIdx);
            }
            bucketFutures.get(bucketIdx).set(rows);
            allFuturesSet.set(bucketIdx);

            if (allFuturesSet.cardinality() == numBuckets) {
                allFuturesSet.clear();
                initBucketFutures();
            }
        }
    }

    public Streamer<?>[] streamer() {
        return streamer;
    }

    public void finish() {
        if (!failed) {
            pageDownstream.finish();
        }
    }

    public void failure(Throwable throwable) {
        failed = true;
        pageDownstream.fail(throwable);
    }
}
