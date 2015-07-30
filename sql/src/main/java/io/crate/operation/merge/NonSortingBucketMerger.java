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
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.MultiFutureCallback;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row;
import io.crate.operation.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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


    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final boolean keepPages;

    private volatile boolean pendingPause = false;
    private volatile Iterator<Row> pausedRowIterator;
    private volatile PageConsumeListener pausedListener;
    private final List<List<Bucket>> pages = new ArrayList<>();

    public NonSortingBucketMerger(RowDownstream rowDownstream, boolean downstreamRequiresRepeat) {
        this(rowDownstream, downstreamRequiresRepeat, Optional.<Executor>absent());
    }

    public NonSortingBucketMerger(RowDownstream rowDownstream, boolean downstreamRequiresRepeat, Optional<Executor> executor) {
        this.downstream = rowDownstream.registerUpstream(this);
        keepPages = downstreamRequiresRepeat;
        this.alreadyFinished = new AtomicBoolean(false);
        this.executor = executor;
    }

    @Override
    public void pause() {
        LOGGER.trace("received pause (will pause sending rows to {})", downstream);
        pendingPause = true;
    }

    @Override
    public void resume() {
        if (paused.compareAndSet(true, false)) {
            LOGGER.trace("resume sending rows to: {}", downstream);
            processBuckets(pausedListener, pausedRowIterator);
        } else {
            pendingPause = false;
        }
    }

    private Iterator<Row> repeatIt() {
        Iterable<Row> iterable = Collections.emptyList();
        for (List<Bucket> page : pages) {
            iterable = Iterables.concat(iterable, Iterables.concat(page));
        }
        return iterable.iterator();
    }

    @Override
    public void repeat() {
        if (alreadyFinished.compareAndSet(true, false)) {
            LOGGER.trace("received repeat: {}", downstream);
            paused.set(false);
            processBuckets(PageConsumeListener.NO_OP_LISTENER, repeatIt());
            downstream.finish();
            alreadyFinished.set(true);
        } else {
            LOGGER.trace("received repeat, but wasn't finished {}", downstream);
        }
    }

    private void processBuckets(PageConsumeListener listener, Iterator<Row> rowIterator) {
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            try {
                if (alreadyFinished.get()) {
                    listener.finish();
                    return;
                }
                boolean needMore = downstream.setNextRow(row);
                if (pendingPause) {
                    pausedListener = listener;
                    pausedRowIterator = rowIterator;
                    paused.set(true);
                    pendingPause = false;
                    LOGGER.trace("pause sending rows to {}", downstream);
                    return;
                }

                if (!needMore) {
                    listener.finish();
                    return;
                }
            } catch (Throwable t) {
                fail(t);
                listener.finish();
                return;
            }
        }
        listener.needMore();
    }

    @Override
    public void nextPage(BucketPage page, final PageConsumeListener listener) {
        final FutureCallback<List<Bucket>> callback = new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(List<Bucket> buckets) {
                if (keepPages) {
                    pages.add(buckets);
                }
                LOGGER.trace("received bucket");
                Iterable<Row> rows = Iterables.concat(buckets);
                processBuckets(listener, rows.iterator());
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                fail(t);
                listener.finish();
            }
        };

        Executor executor = RejectionAwareExecutor.wrapExecutor(this.executor.or(MoreExecutors.directExecutor()), callback);
        /**
         * Wait for all buckets to arrive before doing any work to make sure that the job context is present on all nodes
         * Otherwise there could be race condition.
         * E.g. if a FetchProjector finishes early with data from one node and wants to close the remaining contexts it
         * could be that one node doesn't even have a context to close yet and that context would remain open.
         *
         * NOTE: this doesn't use Futures.allAsList because in the case of failures it should still wait for the other
         * upstreams before taking any action
         */

        MultiFutureCallback<Bucket> multiFutureCallback = new MultiFutureCallback<>(page.buckets().size(), callback);
        for (ListenableFuture<Bucket> bucketFuture : page.buckets()) {
            Futures.addCallback(bucketFuture, multiFutureCallback, executor);
        }
    }

    @Override
    public void finish() {
        if (!alreadyFinished.getAndSet(true)) {
            LOGGER.trace("NonSortingBucketMerger {} finished. Propagate finish on {}", hashCode(), downstream);
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
