/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport.distributed;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.exceptions.UnknownUpstreamFailure;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.PageDownstream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DownstreamOperationContext {

    private static final ESLogger LOGGER = Loggers.getLogger(DownstreamOperationContext.class);

    private final AtomicInteger mergeOperationsLeft;
    private final PageDownstream pageDownstream;
    private final List<SettableFuture<Bucket>> upstreamFutures;
    private final Streamer<?>[] streamers;
    private final DistributedRequestContextManager.DoneCallback doneCallback;
    private final AtomicBoolean needsMoreRows;
    private final AtomicBoolean alreadyFinished;

    public DownstreamOperationContext(PageDownstream pageDownstream,
                                      int numUpstreams,
                                      Streamer<?>[] streamers,
                                      DistributedRequestContextManager.DoneCallback doneCallback) {
        this.pageDownstream = pageDownstream;
        this.mergeOperationsLeft = new AtomicInteger(numUpstreams);
        this.upstreamFutures = new ArrayList<>(numUpstreams);
        for (int i = 0; i < numUpstreams; i++) {
            upstreamFutures.add(SettableFuture.<Bucket>create());
        }
        this.needsMoreRows = new AtomicBoolean(true);
        this.alreadyFinished = new AtomicBoolean(false);
        this.streamers = streamers;
        this.doneCallback = doneCallback;
        BucketPage bucketPage = new BucketPage(upstreamFutures);
        this.pageDownstream.nextPage(bucketPage, new PageConsumeListener() {
            // PageConsumeCallback
            @Override
            public void finish() {
                needsMoreRows.set(false);
                finishContext();
            }

            @Override
            public void needMore() {
                // in the current state, we only have one page
                // so we can finish here
                finishContext();
            }
        });
    }

    public void addFailure(@Nullable Throwable failure) {
        int bucketNum = mergeOperationsLeft.decrementAndGet();
        LOGGER.trace("bucket #{} failed", bucketNum);

        if (failure != null) {
            LOGGER.error("addFailure local", failure);
        } else {
            failure = new UnknownUpstreamFailure();
        }
        if (bucketNum > 0 && !alreadyFinished.get()) {
            // this will bubble the exception up to the result listener
            upstreamFutures.get(bucketNum).setException(failure);
        }
    }

    public void add(Bucket bucket) {
        assert bucket != null;
        int bucketNum = mergeOperationsLeft.decrementAndGet();
        LOGGER.trace("add bucket #{}: {}", bucketNum, bucket);
        if (bucketNum >= 0 && needsMoreRows.get()) {
            upstreamFutures.get(bucketNum).set(bucket);
        }
    }

    public Streamer<?>[] streamers() {
        return streamers;
    }

    private void finishContext() {
        if (!alreadyFinished.getAndSet(true)) {
            LOGGER.trace("finishing context: {}", hashCode());
            doneCallback.finished();
            pageDownstream.finish();
        }
    }
}
