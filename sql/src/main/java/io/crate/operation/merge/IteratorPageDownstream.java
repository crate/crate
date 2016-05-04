/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Function;
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
import io.crate.operation.PageConsumeListener;
import io.crate.operation.PageDownstream;
import io.crate.operation.RejectionAwareExecutor;
import io.crate.operation.collect.collectors.TopRowUpstream;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class IteratorPageDownstream implements PageDownstream {

    private static final ESLogger LOGGER = Loggers.getLogger(IteratorPageDownstream.class);

    private final RowReceiver rowReceiver;
    private final Executor executor;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final PagingIterator<Void, Row> pagingIterator;
    private final TopRowUpstream topRowUpstream;

    private volatile PageConsumeListener pausedListener;
    private volatile Iterator<Row> pausedIterator;
    private boolean downstreamWantsMore = true;

    public IteratorPageDownstream(final RowReceiver rowReceiver,
                                  final PagingIterator<Void, Row> pagingIterator,
                                  Optional<Executor> executor) {
        this.pagingIterator = pagingIterator;
        this.executor = executor.or(MoreExecutors.directExecutor());
        this.rowReceiver = rowReceiver;
        this.topRowUpstream = new TopRowUpstream(
                this.executor,
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            processBuckets(pausedIterator, pausedListener);
                        } catch (Throwable t) {
                            fail(t);
                            pausedListener.finish();
                        }
                    }
                },
                new Runnable() {
                    @Override
                    public void run() {
                        if (finished.compareAndSet(true, false)) {
                            try {
                                if (processBuckets(pagingIterator.repeat().iterator(), PageConsumeListener.NO_OP_LISTENER)) {
                                    consumeRemaining();
                                }
                                finished.set(true);
                                rowReceiver.finish();
                            } catch (Throwable t) {
                                fail(t);
                            }
                        } else {
                            LOGGER.trace("Received repeat, but wasn't finished");
                        }
                    }
                }
        );
        rowReceiver.setUpstream(topRowUpstream);
    }

    private boolean processBuckets(Iterator<Row> iterator, PageConsumeListener listener) {
        while (iterator.hasNext()) {
            if (finished.get()) {
                listener.finish();
                return false;
            }
            Row row = iterator.next();
            boolean wantMore = rowReceiver.setNextRow(row);
            if (topRowUpstream.shouldPause()) {
                pausedListener = listener;
                pausedIterator = iterator;
                topRowUpstream.pauseProcessed();
                return true;
            }
            if (!wantMore) {
                downstreamWantsMore = false;
                listener.finish();
                return false;
            }
        }
        listener.needMore();
        return true;
    }

    @Override
    public void nextPage(BucketPage page, final PageConsumeListener listener) {
        FutureCallback<List<Bucket>> finalCallback = new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(List<Bucket> buckets) {
                pagingIterator.merge(numberedBuckets(buckets));
                try {
                    processBuckets(pagingIterator, listener);
                } catch (Throwable t) {
                    fail(t);
                    listener.finish();
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                fail(t);
                listener.finish();
            }
        };

        /**
         * Wait for all buckets to arrive before doing any work to make sure that the job context is present on all nodes
         * Otherwise there could be race condition.
         * E.g. if a FetchProjector finishes early with data from one node and wants to close the remaining contexts it
         * could be that one node doesn't even have a context to close yet and that context would remain open.
         *
         * NOTE: this doesn't use Futures.allAsList because in the case of failures it should still wait for the other
         * upstreams before taking any action
         */
        Executor executor = RejectionAwareExecutor.wrapExecutor(this.executor, finalCallback);
        MultiFutureCallback<Bucket> multiFutureCallback = new MultiFutureCallback<>(page.buckets().size(), finalCallback);
        for (ListenableFuture<Bucket> bucketFuture : page.buckets()) {
            Futures.addCallback(bucketFuture, multiFutureCallback, executor);
        }
    }

    private Iterable<? extends KeyIterable<Void, Row>> numberedBuckets(List<Bucket> buckets) {
        return Iterables.transform(buckets, new Function<Bucket, KeyIterable<Void, Row>>() {

            @Nullable
            @Override
            public KeyIterable<Void, Row> apply(Bucket input) {
                return new KeyIterable<>(null, input);
            }
        });
    }

    @Override
    public void finish() {
        if (finished.compareAndSet(false, true)) {
            if (downstreamWantsMore) {
                consumeRemaining();
            }
            rowReceiver.finish();
        }
    }

    private void consumeRemaining() {
        pagingIterator.finish();
        while (pagingIterator.hasNext()) {
            Row row = pagingIterator.next();
            boolean wantMore = rowReceiver.setNextRow(row);
            if (!wantMore) {
                break;
            }
        }
    }

    @Override
    public void fail(Throwable t) {
        if (finished.compareAndSet(false, true)) {
            rowReceiver.fail(t);
        }
    }
}
