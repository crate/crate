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

package io.crate.execution.engine.sort;

import static io.crate.common.concurrent.CompletableFutures.supplyAsync;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import io.crate.common.collections.Iterables;
import io.crate.common.collections.Lists;

public final class Sort {

    public static <T> CompletableFuture<List<T>> parallelSort(List<T> list,
                                                              Comparator<? super T> comparator,
                                                              int minItemsPerThread,
                                                              int numAvailableThreads,
                                                              Executor executor) {

        int itemsPerThread = list.size() / Math.max(1, numAvailableThreads);
        if (numAvailableThreads <= 1 || itemsPerThread < minItemsPerThread) {
            list.sort(comparator);
            return completedFuture(list);
        }
        List<List<T>> partitions = Lists.partition(list, itemsPerThread);
        ArrayList<CompletableFuture<List<T>>> futures = new ArrayList<>(partitions.size());
        for (List<T> partition : partitions) {
            futures.add(supplyAsync(() -> {
                partition.sort(comparator);
                return partition;
            }, executor));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(aVoid -> {

                ArrayList<T> result = new ArrayList<>(list.size());
                ArrayList<List<T>> parts = new ArrayList<>(futures.size());
                for (int i = 0; i < futures.size(); i++) {
                    parts.add(futures.get(i).join());
                }
                Iterables.addAll(result, Iterables.mergeSorted(parts, comparator));
                return result;
            });
    }
}
