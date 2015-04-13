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

package io.crate.operation.merge;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.projectors.NoOpProjector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BucketMerger implementation that does not care about sorting
 * and just emits a stream of rows, whose order is undeterministic
 * as it is not guaranteed which row from which bucket ends up in the stream at which position.
 */
public class NonSortingBucketMerger implements BucketMerger {

    private static final ESLogger LOGGER = Loggers.getLogger(NonSortingBucketMerger.class);

    private RowDownstreamHandle downstream;
    private final AtomicBoolean wantMore;
    private final AtomicBoolean alreadyFinished;
    private final Optional<Executor>  executor;

    public NonSortingBucketMerger() {
        this(Optional.<Executor>absent());
    }

    public NonSortingBucketMerger(Optional<Executor> executor) {
        this.downstream = NoOpProjector.INSTANCE;
        this.wantMore = new AtomicBoolean(true);
        this.alreadyFinished = new AtomicBoolean(false);
        this.executor = executor;
    }

    @Override
    public void nextPage(BucketPage page, final PageConsumeListener listener) {
        final AtomicBoolean listenerNotified = new AtomicBoolean(false);
        final AtomicInteger bucketsPending = new AtomicInteger(page.buckets().size());
        FutureCallback<Bucket> callback = new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(@Nullable Bucket result) {
                LOGGER.trace("received bucket");
                if (result != null && wantMore.get() && !listenerNotified.get()) {
                    for (Row row : result) {
                        if (!emitRow(row)) {
                            wantMore.set(false);
                            notifyListener();
                            break;
                        }
                    }
                }
                if (bucketsPending.decrementAndGet() == 0) {
                    notifyListener();
                }
            }

            private void notifyListener() {
                if (!listenerNotified.getAndSet(true)) {
                    if (wantMore.get()) {
                        listener.needMore();
                    } else {
                        listener.finish();
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.trace("error in {}", t, NonSortingBucketMerger.this.getClass().getSimpleName());
                wantMore.set(false);
                fail(t);
                notifyListener();
            }
        };
        for (ListenableFuture<Bucket> bucketFuture : page.buckets()) {
            Futures.addCallback(bucketFuture, callback, executor.isPresent() ? executor.get() : MoreExecutors.directExecutor());
        }
    }

    @Override
    public void finish() {
        if (!alreadyFinished.getAndSet(true)) {
            LOGGER.trace("{} finished.", hashCode());
            downstream.finish();
        }
    }

    private synchronized boolean emitRow(Row row) {
        return downstream.setNextRow(row);
    }

    @Override
    public void fail(Throwable t) {
        if (!alreadyFinished.getAndSet(true)) {
            LOGGER.trace("{} failed.", t, hashCode());
            downstream.fail(t);
        }
    }

    @Override
    public void downstream(RowDownstream downstream) {
        assert downstream != null : "downstream must not be null";
        this.downstream = downstream.registerUpstream(this);
    }
}
