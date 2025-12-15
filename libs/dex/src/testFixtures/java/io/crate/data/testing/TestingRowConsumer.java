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

package io.crate.data.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;

import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.data.RowConsumer;

public final class TestingRowConsumer implements RowConsumer {

    private final ArrayList<Object[]> rows = new ArrayList<>();
    private final CompletableFuture<List<Object[]>> result = new CompletableFuture<>();
    private final boolean autoClose;
    private final boolean requiresScroll;

    static CompletionStage<?> moveToEnd(BatchIterator<Row> it) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        it.move(Integer.MAX_VALUE, row -> {}, err -> {
            if (err == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(err);
            }
        });
        return future;
    }

    public TestingRowConsumer() {
        this.autoClose = true;
        this.requiresScroll = false;
    }

    public TestingRowConsumer(boolean autoClose, boolean requiresScroll) {
        this.autoClose = autoClose;
        this.requiresScroll = requiresScroll;
    }

    @Override
    public boolean requiresScroll() {
        return requiresScroll;
    }

    @Override
    public void accept(BatchIterator<Row> it, @Nullable Throwable failure) {
        if (failure == null) {
            it.move(Integer.MAX_VALUE, row -> rows.add(row.materialize()), err -> {
                if (autoClose) {
                    it.close();
                }
                if (err == null) {
                    result.complete(rows);
                } else {
                    result.completeExceptionally(err);
                }
            });
        } else {
            if (it != null && autoClose) {
                it.close();
            }
            result.completeExceptionally(failure);
        }
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return result;
    }

    public List<Object[]> getResult() throws Exception {
        return getResult(10000);
    }

    public List<Object[]> getResult(int timeoutInMs) throws Exception {
        try {
            return result.get(timeoutInMs, TimeUnit.MILLISECONDS);
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
