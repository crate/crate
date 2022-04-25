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

package io.crate.concurrent;

import io.crate.common.SuppressForbidden;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public final class CompletableFutures {

    private CompletableFutures() {
    }

    /**
     * Return a new {@link CompletableFuture} that is completed when all of the given futures
     * complete. The future contains a List containing the results of all futures that completed successfully.
     *
     * Failed futures are skipped. Their result is not included in the result list.
     */
    public static <T> CompletableFuture<List<T>> allSuccessfulAsList(Collection<? extends CompletableFuture<? extends T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .handle((ignored, ignoredErr) -> {
                ArrayList<T> results = new ArrayList<>(futures.size());
                for (CompletableFuture<? extends T> future : futures) {
                    if (future.isCompletedExceptionally()) {
                        continue;
                    }
                    results.add(future.join());
                }
                return results;
            });
    }

    /**
     * Return a new {@link CompletableFuture} that is completed when all of the given futures
     * complete. The future contains a List containing the result of all futures.
     *
     * If any future failed, the result is a failed future.
     */
    public static <T> CompletableFuture<List<T>> allAsList(Collection<? extends CompletableFuture<? extends T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(aVoid -> {
                ArrayList<T> results = new ArrayList<>(futures.size());
                for (CompletableFuture<? extends T> future : futures) {
                    results.add(future.join());
                }
                return results;
            });
    }

    @SuppressForbidden(reason = "This is the wrapper for supplyAsync that should be used - it handles exceptions")
    public static <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier, Executor executor) {
        try {
            return CompletableFuture.supplyAsync(supplier, executor);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

}
