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

package io.crate.session;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import io.crate.data.Row;

/**
 * Collect rows using a {@link Collector}
 **/
public final class CollectingResultReceiver<A, R> implements ResultReceiver<R> {

    private final BiConsumer<A, Row> accumulator;
    private final Function<A, R> finisher;
    private final CompletableFuture<R> result;
    private A state;

    public CollectingResultReceiver(Collector<Row, A, R> collector) {
        this.state = collector.supplier().get();
        this.accumulator = collector.accumulator();
        this.finisher = collector.finisher();
        this.result = new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<R> completionFuture() {
        return result;
    }

    @Override
    public void setNextRow(Row row) {
        accumulator.accept(state, row);
    }

    @Override
    public void batchFinished() {
    }

    @Override
    public void allFinished() {
        result.complete(finisher.apply(state));
    }

    @Override
    public void fail(Throwable t) {
        result.completeExceptionally(t);
    }
}
