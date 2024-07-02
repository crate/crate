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

package io.crate.execution.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import io.crate.common.collections.Iterables;
import io.crate.common.collections.Lists;
import io.crate.common.concurrent.CompletableFutures;

public class ThreadPools {

    public static IntSupplier numIdleThreads(ThreadPoolExecutor executor, int numProcessors) {
        return () -> Math.min(
            Math.max(executor.getMaximumPoolSize() - executor.getActiveCount(), 1),
            // poolSize can be > number of processors but we don't want to utilize more threads than numProcessors
            // per execution. Thread contention would go up and we're running into RejectedExecutions earlier on
            // concurrent queries
            numProcessors
        );
    }

    /**
     * Uses up to availableThreads threads to run all suppliers.
     * if availableThreads is smaller than the number of suppliers it will run multiple suppliers
     * grouped within the available threads.
     *
     * @param executor           executor that is used to execute the callableList
     * @param availableThreads   A function returning the number of threads which can be utilized
     * @param suppliers          a collection of callable that should be executed
     * @param <T>                type of the final result
     * @return a future that will return a list of the results of the suppliers or a failed future in case an exception
     * is encountered
     */
    public static <T> CompletableFuture<List<T>> runWithAvailableThreads(
        Executor executor,
        IntSupplier availableThreads,
        Collection<Supplier<T>> suppliers) throws RejectedExecutionException {

        int threadsToUse = availableThreads.getAsInt();
        if (threadsToUse < suppliers.size()) {
            Iterable<List<Supplier<T>>> partitions = Iterables.partition(suppliers, suppliers.size() / threadsToUse);

            ArrayList<CompletableFuture<List<T>>> futures = new ArrayList<>(threadsToUse + 1);
            for (List<Supplier<T>> partition : partitions) {
                Supplier<List<T>> executePartition = () -> Lists.map(partition, Supplier::get);
                futures.add(CompletableFutures.supplyAsync(executePartition, executor));
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(aVoid -> {
                    ArrayList<T> finalResult = new ArrayList<>(suppliers.size());
                    for (CompletableFuture<List<T>> future: futures) {
                        finalResult.addAll(future.join());
                    }
                    return finalResult;
                });
        } else {
            ArrayList<CompletableFuture<T>> futures = new ArrayList<>(suppliers.size());
            for (Supplier<T> supplier : suppliers) {
                futures.add(CompletableFutures.supplyAsync(supplier, executor));
            }
            return CompletableFutures.allAsList(futures);
        }
    }

    /**
     * Execute the given runnable using the executor.
     * If the executor throws a RejectedExecutionException the runnable is invoked directly in the calling thread
     */
    public static void forceExecute(Executor executor, Runnable runnable) {
        try {
            executor.execute(runnable);
        } catch (RejectedExecutionException e) {
            runnable.run();
        }
    }
}
