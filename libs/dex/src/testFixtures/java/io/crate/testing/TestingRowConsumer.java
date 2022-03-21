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

package io.crate.testing;

import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Bucket;
import io.crate.data.CollectingRowConsumer;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.Exceptions;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class TestingRowConsumer implements RowConsumer {

    private final CollectingRowConsumer<?, List<Object[]>> consumer;

    static CompletionStage<?> moveToEnd(BatchIterator<Row> it) {
        return BatchIterators.collect(it, Collectors.counting());
    }

    public TestingRowConsumer() {
        consumer = new CollectingRowConsumer<>(Collectors.mapping(Row::materialize, Collectors.toList()));
    }

    @Override
    public void accept(BatchIterator<Row> it, Throwable failure) {
        consumer.accept(it, failure);
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return consumer.completionFuture();
    }

    public List<Object[]> getResult() throws Exception {
        return getResultWithTimeout(10);
    }

    public List<Object[]> getResultWithTimeout(int timeout) throws Exception {
        try {
            return consumer.completionFuture().get(timeout, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                Exceptions.rethrowUnchecked(cause);
            }
            throw e;
        }
    }

    public Bucket getBucket() throws Exception {
        return new CollectionBucket(getResult());
    }
}
