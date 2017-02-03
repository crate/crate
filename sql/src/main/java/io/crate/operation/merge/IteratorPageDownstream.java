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
import io.crate.data.Bucket;
import io.crate.data.BucketPage;
import io.crate.data.Row;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.PageDownstream;
import io.crate.operation.RejectionAwareExecutor;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.operation.projectors.RowReceiver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class IteratorPageDownstream implements PageDownstream, ResumeHandle, RepeatHandle {

    private final RowReceiver rowReceiver;
    private final Executor executor;
    private final AtomicBoolean upstreamHasMoreData = new AtomicBoolean(true);
    private final PagingIterator<Void, Row> pagingIterator;

    private volatile PageConsumeListener lastListener;
    private volatile Iterator<Row> lastIterator;
    private boolean downstreamWantsMore = true;

    public IteratorPageDownstream(final RowReceiver rowReceiver,
                                  final PagingIterator<Void, Row> pagingIterator,
                                  Optional<Executor> executor) {
        this.pagingIterator = pagingIterator;
        lastIterator = pagingIterator;
        this.executor = executor.or(MoreExecutors.directExecutor());
        this.rowReceiver = rowReceiver;
    }

    private void processBuckets(Iterator<Row> iterator, PageConsumeListener listener) {
        while (iterator.hasNext()) {
            Row row = iterator.next();
            RowReceiver.Result result = rowReceiver.setNextRow(row);
            switch (result) {
                case CONTINUE:
                    continue;
                case PAUSE:
                    rowReceiver.pauseProcessed(this);
                    return;
                case STOP:
                    downstreamWantsMore = false;
                    if (upstreamHasMoreData.get()) {
                        listener.finish();
                    } else {
                        rowReceiver.finish(this);
                    }
                    return;
            }
            throw new AssertionError("Unrecognized setNextRow result: " + result);
        }
        if (upstreamHasMoreData.get()) {
            listener.needMore();
        } else {
            rowReceiver.finish(this);
        }
    }

    @Override
    public void nextPage(BucketPage page, final PageConsumeListener listener) {
        FutureCallback<List<Bucket>> finalCallback = new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(List<Bucket> buckets) {
                pagingIterator.merge(numberedBuckets(buckets));
                lastIterator = pagingIterator;
                lastListener = listener;
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
        if (upstreamHasMoreData.compareAndSet(true, false)) {
            if (downstreamWantsMore) {
                pagingIterator.finish();
                processBuckets(lastIterator, lastListener);
            } else {
                rowReceiver.finish(this);
            }
        }
    }

    @Override
    public void fail(Throwable t) {
        if (upstreamHasMoreData.compareAndSet(true, false)) {
            rowReceiver.fail(t);
        }
    }

    @Override
    public void repeat() {
        try {
            Iterator<Row> iterator = pagingIterator.repeat().iterator();
            lastIterator = iterator;
            lastListener = PageConsumeListener.NO_OP_LISTENER;
            processBuckets(iterator, PageConsumeListener.NO_OP_LISTENER);
        } catch (Throwable t) {
            rowReceiver.fail(t);
        }
    }

    @Override
    public void resume(boolean async) {
        try {
            processBuckets(lastIterator, lastListener);
        } catch (Throwable t) {
            fail(t);
            lastListener.finish();
        }
    }

    @Override
    public void kill(Throwable t) {
        rowReceiver.kill(t);
    }
}
