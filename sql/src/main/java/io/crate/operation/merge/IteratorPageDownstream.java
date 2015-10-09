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
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class IteratorPageDownstream implements PageDownstream, RowUpstream {

    private static final ESLogger LOGGER = Loggers.getLogger(IteratorPageDownstream.class);

    private final RowReceiver rowReceiver;
    private final Executor executor;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final PagingIterator<Row> pagingIterator;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    private volatile PageConsumeListener pausedListener;
    private volatile Iterator<Row> pausedIterator;
    private volatile boolean pendingPause;
    private boolean downstreamWantsMore = true;

    public IteratorPageDownstream(RowReceiver rowReceiver,
                                  PagingIterator<Row> pagingIterator,
                                  Optional<Executor> executor) {
        this.pagingIterator = pagingIterator;
        this.executor = executor.or(MoreExecutors.directExecutor());
        this.rowReceiver = rowReceiver;
        rowReceiver.setUpstream(this);
    }

    @Override
    public void pause() {
        pendingPause = true;
    }

    @Override
    public void resume(boolean async) {
        pendingPause = false;
        if (paused.compareAndSet(true, false)) {
            LOGGER.trace("resume");
            processBuckets(pausedIterator, pausedListener);
        }
    }

    /**
     * tells the RowUpstream that it should push all rows again
     */
    @Override
    public void repeat() {
        if (finished.compareAndSet(true, false)) {
            LOGGER.trace("received repeat: {}", rowReceiver);
            paused.set(false);
            if (processBuckets(pagingIterator.repeat(), PageConsumeListener.NO_OP_LISTENER)) {
                consumeRemaining();
            }
            rowReceiver.finish();
            finished.set(true);
        } else {
            LOGGER.trace("received repeat, but wasn't finished {}", rowReceiver);
         }
    }

    private boolean processBuckets(Iterator<Row> iterator, PageConsumeListener listener) {
        while (iterator.hasNext()) {
            if (finished.get()) {
                listener.finish();
                return false;
            }
            Row row = iterator.next();
            boolean wantMore = rowReceiver.setNextRow(row);
            if (pendingPause) {
                pausedListener = listener;
                pausedIterator = iterator;
                paused.set(true);
                pendingPause = false;
                return wantMore;
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
                processBuckets(pagingIterator, listener);
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

    private Iterable<? extends NumberedIterable<Row>> numberedBuckets(List<Bucket> buckets) {
        return Iterables.transform(buckets, new Function<Bucket, NumberedIterable<Row>>() {

            int number = -1;

            @Nullable
            @Override
            public NumberedIterable<Row> apply(Bucket input) {
                number++;
                return new NumberedIterable<>(number, input);
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
