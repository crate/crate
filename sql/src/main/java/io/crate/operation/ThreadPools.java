/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPools {

    /**
     * runs each runnable of the runnableCollection in it's own thread unless there aren't enough threads available.
     * In that case it will partition the runnableCollection to match the number of available threads.
     *
     * @throws RejectedExecutionException in case all threads are busy and overloaded.
     */
    public static void runWithAvailableThreads(ThreadPoolExecutor executor,
                                               int poolSize,
                                               Collection<Runnable> runnableCollection) throws RejectedExecutionException {
        int availableThreads = Math.max(poolSize - executor.getActiveCount(), 2);
        if (availableThreads < runnableCollection.size()) {
            Iterable<List<Runnable>> partition = Iterables.partition(runnableCollection,
                    runnableCollection.size() / availableThreads);

            for (final List<Runnable> runnableList : partition) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        for (Runnable runnable : runnableList) {
                            runnable.run();
                        }
                    }
                });
            }
        } else {
            for (Runnable runnable : runnableCollection) {
                executor.execute(runnable);
            }
        }
    }

    /**
     * Similar to {@link #runWithAvailableThreads(ThreadPoolExecutor, int, Collection)}
     * but this function will return a Future that wraps the futures of each callable
     *
     * @param executor executor that is used to execute the callableList
     * @param poolSize the corePoolSize of the given executor
     * @param callableCollection a collection of callable that should be executed
     * @param mergeFunction function that will be applied to merge the results of multiple callable in case that they are
     *                      executed together if the threadPool is exhausted
     * @param <T> type of the final result
     * @return a future that will return a list of the results of the callableList
     * @throws RejectedExecutionException
     */
    public static <T> ListenableFuture<List<T>> runWithAvailableThreads(
            ThreadPoolExecutor executor,
            int poolSize,
            Collection<Callable<T>> callableCollection,
            final Function<List<T>, T> mergeFunction) throws RejectedExecutionException {

        ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(executor);

        List<ListenableFuture<T>> futures;
        int availableThreads = Math.max(poolSize - executor.getActiveCount(), 1);
        if (availableThreads < callableCollection.size()) {
            Iterable<List<Callable<T>>> partition = Iterables.partition(callableCollection,
                    callableCollection.size() / availableThreads);

            futures = new ArrayList<>(availableThreads + 1);
            for (final List<Callable<T>> callableList : partition) {
                futures.add(listeningExecutorService.submit(new Callable<T>() {
                    @Override
                    public T call() throws Exception {
                        List<T> results = new ArrayList<T>(callableList.size());
                        for (Callable<T> tCallable : callableList) {
                            results.add(tCallable.call());
                        }
                        return mergeFunction.apply(results);
                    }
                }));
            }
        } else {
            futures = new ArrayList<>(callableCollection.size());
            for (Callable<T> callable : callableCollection) {
                futures.add(listeningExecutorService.submit(callable));
            }
        }
        return Futures.allAsList(futures);
    }
}
