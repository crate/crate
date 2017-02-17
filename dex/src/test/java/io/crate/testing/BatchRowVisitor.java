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

package io.crate.testing;

import io.crate.data.BatchIterator;
import io.crate.data.Row;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Visitor that iterates to the end of a BatchIterator consuming all rows.
 * The returned future will complete once the end of the batchIterator has been reached.
 *
 * This is no proper {@link io.crate.data.BatchConsumer} and it does *NOT* close the BatchIterator.
 */
public class BatchRowVisitor {

    public static CompletableFuture<?> visitRows(BatchIterator it, Consumer<Row> rowConsumer) {
        return visitRows(it, rowConsumer, new CompletableFuture<>());
    }

    public static CompletableFuture<?> visitRows(BatchIterator it,
                                                 Consumer<Row> rowConsumer,
                                                 CompletableFuture<?> completableFuture) {

        while (it.moveNext()) {
            rowConsumer.accept(it.currentRow());
        }
        if (it.allLoaded()) {
            completableFuture.complete(null);
        } else {
            it.loadNextBatch().whenComplete((r, t) -> {
                if (t == null) {
                    visitRows(it, rowConsumer, completableFuture);
                } else {
                    completableFuture.completeExceptionally(t);
                }
            });
        }
        return completableFuture;
    }
}
