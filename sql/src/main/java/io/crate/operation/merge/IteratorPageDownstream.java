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
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.MultiFutureCallback;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row;
import io.crate.operation.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class IteratorPageDownstream implements PageDownstream, RowUpstream {

    public static final Function<Bucket, Iterator<Row>> BUCKET_TO_ROW_ITERATOR = new Function<Bucket, Iterator<Row>>() {
        @Nullable
        @Override
        public Iterator<Row> apply(Bucket input) {
            return input.iterator();
        }
    };
    private final RowDownstreamHandle downstream;
    private final Executor executor;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final PagingIterator<Row> pagingIterator;

    public IteratorPageDownstream(RowDownstream rowDownstream,
                                  PagingIterator<Row> pagingIterator,
                                  Optional<Executor> executor) {
        this.pagingIterator = pagingIterator;
        this.executor = executor.or(MoreExecutors.directExecutor());
        downstream = rowDownstream.registerUpstream(this);
    }

    @Override
    public void nextPage(BucketPage page, final PageConsumeListener listener) {
        FutureCallback<List<Bucket>> finalCallback = new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(List<Bucket> buckets) {
                pagingIterator.merge(getBucketIterators(buckets));
                while (pagingIterator.hasNext()) {
                    if (finished.get()) {
                        listener.finish();
                        return;
                    }
                    Row row = pagingIterator.next();
                    boolean wantMore = downstream.setNextRow(row);
                    if (!wantMore) {
                        listener.finish();
                        return;
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

    @Override
    public void finish() {
        if (finished.compareAndSet(false, true)) {
            consumeRemaining();
            downstream.finish();
        }
    }

    private void consumeRemaining() {
        pagingIterator.finish();
        while (pagingIterator.hasNext()) {
            Row row = pagingIterator.next();
            boolean wantMore = downstream.setNextRow(row);
            if (!wantMore) {
                break;
            }
        }
    }

    @Override
    public void fail(Throwable t) {
        if (finished.compareAndSet(false, true)) {
            downstream.fail(t);
        }
    }

    private Iterable<Iterator<Row>> getBucketIterators(List<Bucket> buckets) {
        return FluentIterable.from(buckets).transform(BUCKET_TO_ROW_ITERATOR);
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }
}
