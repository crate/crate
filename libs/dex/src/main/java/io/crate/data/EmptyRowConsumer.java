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
import javax.annotation.Nullable;

public final class EmptyRowConsumer implements RowConsumer {

    private final CompletableFuture<?> completionFuture = new CompletableFuture<>();

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        if (failure == null) {
            consumeIt(iterator);
        } else {
            if (iterator != null) {
                iterator.close();
            }
            completionFuture.completeExceptionally(failure);
        }
    }

    private void consumeIt(BatchIterator<Row> iterator) {
        try {
            while (iterator.moveNext()) {
            }
            if (iterator.allLoaded()) {
                completionFuture.complete(null);
                iterator.close();
            } else {
                iterator.loadNextBatch().whenComplete((r, f) -> {
                    if (f == null) {
                        consumeIt(iterator);
                    } else {
                        iterator.close();
                    }
                });
            }
        } catch (Throwable t) {
            iterator.close();
            completionFuture.completeExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return completionFuture;
    }
}
