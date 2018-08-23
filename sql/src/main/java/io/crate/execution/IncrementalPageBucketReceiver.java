/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.SentinelRow;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.PageResultListener;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class IncrementalPageBucketReceiver<T> implements PageBucketReceiver {

    private final Function<T, Iterable<Row>> finisher;
    private final BiConsumer<T, Row> accumulator;
    private final T state;
    private final AtomicInteger remainingUpstreams;
    private final RowConsumer consumer;
    private final CompletableFuture<?> processingFuture = new CompletableFuture<>();
    private final Streamer<?>[] streamers;

    private volatile Throwable lastThrowable = null;

    public IncrementalPageBucketReceiver(Collector<Row, T, Iterable<Row>> collector,
                                         RowConsumer rowConsumer,
                                         Streamer<?>[] streamers,
                                         int upstreamsCount) {
        this.state = collector.supplier().get();
        this.accumulator = collector.accumulator();
        this.finisher = collector.finisher();
        this.consumer = rowConsumer;
        this.streamers = streamers;
        this.remainingUpstreams = new AtomicInteger(upstreamsCount);
    }

    @Override
    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        synchronized (state) {
            try {
                for (Row row : rows) {
                    accumulator.accept(state, row);
                }
            } catch (Throwable e) {
                lastThrowable = e;
            }
        }

        pageResultListener.needMore(!isLast);
        if (isLast) {
            if (remainingUpstreams.decrementAndGet() == 0) {
                consumeRows();
                if (lastThrowable == null) {
                    processingFuture.complete(null);
                } else {
                    processingFuture.completeExceptionally(lastThrowable);
                }
            }
        }
    }

    @Override
    public void releasePageResultListeners() {
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
        consumer.accept(InMemoryBatchIterator.of(finisher.apply(state), SentinelRow.SENTINEL), lastThrowable);
    }

    @Override
    public void kill(@Nonnull Throwable t) {
        lastThrowable = t;
        consumer.accept(null, t);
        processingFuture.completeExceptionally(t);
    }
}
