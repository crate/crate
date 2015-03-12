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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.projectors.NoOpProjector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * BucketMerger implementation that does not care about sorting
 * and just emits a stream of rows, whose order is undeterministic
 * as it is not guaranteed which row from which bucket ends up in the stream at which position.
 */
public class NonSortingBucketMerger implements BucketMerger {

    private RowDownstreamHandle downstream;
    private AtomicBoolean wantMore;

    public NonSortingBucketMerger() {
        this.downstream = NoOpProjector.INSTANCE;
        this.wantMore = new AtomicBoolean(true);
    }

    @Override
    public void merge(List<ListenableFuture<Bucket>> buckets) {
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        final CountDownLatch countDownLatch = new CountDownLatch(buckets.size());
        FutureCallback<Bucket> callback = new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(@Nullable Bucket result) {
                if (result != null && wantMore.get()) {
                    for (Row row : result) {
                        if (!emitRow(row)) {
                            stop();
                            break;
                        }
                    }
                }
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                exception.set(t);
                stop();
            }

            private void stop() {
                wantMore.set(false);
                // exceed countdownlatch
                while(countDownLatch.getCount() > 0) {
                    countDownLatch.countDown();
                }
            }
        };
        for (ListenableFuture<Bucket> bucketFuture : buckets) {
            Futures.addCallback(bucketFuture, callback);
        }
        try {
            countDownLatch.await();
            Throwable caught = exception.get();
            if (caught != null) {
                downstream.fail(caught);
            }
        } catch (InterruptedException e) {
            downstream.fail(e);
        }
    }

    @Override
    public void finish() {
        downstream.finish();
    }

    private boolean emitRow(Row row) {
        return downstream.setNextRow(row);
    }

    @Override
    public void fail(Throwable t) {
        downstream.fail(t);
    }

    @Override
    public void downstream(RowDownstream downstream) {
        assert downstream != null : "downstream must not be null";
        this.downstream = downstream.registerUpstream(this);
    }
}
