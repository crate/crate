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

import org.jspecify.annotations.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collector;

public class CollectingRowConsumer<S, R> implements RowConsumer {

    private final Collector<Row, S, R> collector;
    private final CompletableFuture<R> resultFuture = new CompletableFuture<>();

    public CollectingRowConsumer(Collector<Row, S, R> collector) {
        this.collector = collector;
    }

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        if (failure == null) {
            BatchIterators.collect(iterator, collector.supplier().get(), collector, resultFuture);
        } else {
            if (iterator != null) {
                iterator.close();
            }
            resultFuture.completeExceptionally(failure);
        }
    }

    @Override
    public CompletableFuture<R> completionFuture() {
        return resultFuture;
    }
}
