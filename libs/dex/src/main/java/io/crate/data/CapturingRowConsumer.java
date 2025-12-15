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

package io.crate.data;

import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

public final class CapturingRowConsumer implements RowConsumer {

    private final CompletableFuture<BatchIterator<Row>> batchIterator;
    private final CompletableFuture<?> completionFuture;
    private final boolean requiresScroll;

    public CapturingRowConsumer(boolean requiresScroll, CompletableFuture<?> completionFuture) {
        this.batchIterator = new CompletableFuture<>();
        this.completionFuture = completionFuture;
        this.requiresScroll = requiresScroll;
    }

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        if (failure == null) {
            batchIterator.complete(iterator);
        } else {
            batchIterator.completeExceptionally(failure);
        }
    }

    public CompletableFuture<BatchIterator<Row>> capturedBatchIterator() {
        return batchIterator;
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return completionFuture;
    }

    @Override
    public boolean requiresScroll() {
        return requiresScroll;
    }

    @Override
    public String toString() {
        return "CapturingRowConsumer{captured=" + batchIterator.isDone() + '}';
    }
}
