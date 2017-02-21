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

import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class CollectingBatchConsumer implements BatchConsumer {

    private final List<Object[]> rows = new ArrayList<>();
    private final CompletableFuture<List<Object[]>> result = new CompletableFuture<>();

    public static CompletionStage<?> moveToEnd(BatchIterator it) {
        return BatchRowVisitor.visitRows(it, r -> {});
    }

    @Override
    public void accept(BatchIterator it, Throwable failure) {
        if (failure == null) {
            BatchRowVisitor.visitRows(it, r -> rows.add(r.materialize())).whenComplete((r, t) -> {
                if (t == null) {
                    result.complete(rows);
                } else {
                    result.completeExceptionally(t);
                }
                it.close();
            });
        } else {
            result.completeExceptionally(failure);
        }
    }

    public List<Object[]> getResult() {
        try {
            return result.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
