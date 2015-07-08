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
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row;
import io.crate.operation.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BucketMerger implementation that does not care about sorting
 * and just emits a stream of rows, whose order is undeterministic
 * as it is not guaranteed which row from which bucket ends up in the stream at which position.
 */
public class NonSortingBucketMerger implements PageDownstream, RowUpstream {

    private static final ESLogger LOGGER = Loggers.getLogger(NonSortingBucketMerger.class);

    private final RowDownstreamHandle downstream;
    private final AtomicBoolean alreadyFinished;
    private final Optional<Executor>  executor;

    public NonSortingBucketMerger(RowDownstream rowDownstream) {
        this(rowDownstream, Optional.<Executor>absent());
    }

    public NonSortingBucketMerger(RowDownstream rowDownstream, Optional<Executor> executor) {
        this.downstream = rowDownstream.registerUpstream(this);
        this.alreadyFinished = new AtomicBoolean(false);
        this.executor = executor;
    }

    @Override
    public void nextPage(BucketPage page, final PageConsumeListener listener) {
        FutureCallback<List<Bucket>> callback = new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(List<Bucket> buckets) {
                LOGGER.trace("received bucket");
                for (Bucket bucket : buckets) {
                    for (Row row : bucket) {
                        try {
                            boolean needMore = downstream.setNextRow(row);
                            if (!needMore) {
                                listener.finish();
                                return;
                            }
                        } catch (Throwable t) {
                            onFailure(t);
                            return;
                        }
                    }
                }
                listener.needMore();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                fail(t);
                listener.finish();
            }
        };

        Executor executor = this.executor.or(MoreExecutors.directExecutor());
        /**
         * Wait for all buckets to arrive before doing any work to make sure that the job context is present on all nodes
         * Otherwise there could be race condition.
         * E.g. if a FetchProjector finishes early with data from one node and wants to close the remaining contexts it
         * could be that one node doesn't even have a context to close yet and that context would remain open.
         */
        Futures.addCallback(Futures.allAsList(page.buckets()), callback, executor);
    }

    @Override
    public void finish() {
        if (!alreadyFinished.getAndSet(true)) {
            LOGGER.trace("NonSortingBucketMerger {} finished.", hashCode());
            downstream.finish();
        }
    }


    @Override
    public void fail(Throwable t) {
        if (!alreadyFinished.getAndSet(true)) {
            LOGGER.trace("NonSortingBucketMerger {} failed.", t, hashCode());
            downstream.fail(t);
        }
    }
}
