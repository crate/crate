/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.jetbrains.annotations.NotNull;

import io.crate.Streamer;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.Bucket;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.SentinelRow;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.PageResultListener;

public class IncrementalPageBucketReceiver<T> implements PageBucketReceiver {

    private final Function<T, Iterable<Row>> finisher;
    private final BiConsumer<T, Row> accumulator;
    private final T state;
    private final AtomicInteger remainingUpstreams;
    private final CompletableFuture<Iterable<Row>> processingFuture = new CompletableFuture<>();
    private final Executor executor;
    private final Streamer<?>[] streamers;

    private CompletableFuture<?> currentlyAccumulating;

    public IncrementalPageBucketReceiver(Collector<Row, T, Iterable<Row>> collector,
                                         RowConsumer rowConsumer,
                                         Executor executor,
                                         Streamer<?>[] streamers,
                                         int upstreamsCount) {
        this.state = collector.supplier().get();
        this.accumulator = collector.accumulator();
        this.finisher = collector.finisher();
        this.executor = executor;
        this.streamers = streamers;
        this.remainingUpstreams = new AtomicInteger(upstreamsCount);

        processingFuture.whenComplete((it, t) -> {
            if (t == null) {
                assert it != null : "Iterable must not be NULL";
                rowConsumer.accept(InMemoryBatchIterator.of(it, SentinelRow.SENTINEL, false), t);
            } else {
                rowConsumer.accept(null, t);
            }
        });
    }

    private void processRows(Bucket rows) {
        for (Row row : rows) {
            accumulator.accept(state, row);
        }
    }

    @Override
    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        if (processingFuture.isCompletedExceptionally()) {
            pageResultListener.needMore(false);
            return;
        } else {
            pageResultListener.needMore(!isLast);
        }

        // We make sure only one accumulation operation runs at a time because the state is not thread-safe.
        synchronized (state) {
            if (currentlyAccumulating == null) {
                try {
                    currentlyAccumulating = CompletableFuture.runAsync(() -> processRows(rows), executor);
                } catch (EsRejectedExecutionException e) {
                    processingFuture.completeExceptionally(e);
                }
            } else {
                currentlyAccumulating = currentlyAccumulating.whenComplete((r, t) -> {
                    if (t == null) {
                        try {
                            processRows(rows);
                        } catch (Exception e) {
                            var runtimeErr = Exceptions.toRuntimeException(t);
                            processingFuture.completeExceptionally(runtimeErr);
                            throw runtimeErr;
                        }
                    } else {
                        var runtimeErr = Exceptions.toRuntimeException(t);
                        processingFuture.completeExceptionally(runtimeErr);
                        throw runtimeErr;
                    }
                });
            }
        }
        if (isLast) {
            if (remainingUpstreams.decrementAndGet() == 0) {
                currentlyAccumulating.whenComplete((r, t) -> consumeRows());
            }
        }
    }

    @Override
    public Streamer<?>[] streamers() {
        return streamers;
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return processingFuture;
    }

    @Override
    public void consumeRows() {
        try {
            processingFuture.complete(finisher.apply(state));
        } catch (Exception e) {
            processingFuture.completeExceptionally(e);
        }
    }

    @Override
    public void kill(@NotNull Throwable t) {
        processingFuture.completeExceptionally(t);
    }
}
