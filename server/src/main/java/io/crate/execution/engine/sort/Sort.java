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
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.LongConsumer;


import org.apache.lucene.util.RamUsageEstimator;

import io.crate.common.collections.Iterables;
import io.crate.common.collections.Lists;

public final class Sort {

    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<List<T>> parallelSort(List<T> list,
                                                              LongConsumer allocateBytes,
                                                              Comparator<? super T> comparator,
                                                              int minItemsPerThread,
                                                              int numAvailableThreads,
                                                              Executor executor) {

        int itemsPerThread = list.size() / Math.max(1, numAvailableThreads);
        if (numAvailableThreads <= 1 || itemsPerThread < minItemsPerThread) {
            list.sort(comparator);
            allocateBytes.accept(sortOverheadBytes(list.size()));
            return completedFuture(list);
        }
        List<List<T>> partitions = Lists.partition(list, itemsPerThread);
        allocateBytes.accept(RamUsageEstimator.sizeOfObject(partitions)); // Accounts for nested list
        CompletableFuture<List<T>>[] futures = new CompletableFuture[partitions.size()];
        allocateBytes.accept(RamUsageEstimator.shallowSizeOf(futures));
        int index = 0;
        for (List<T> partition : partitions) {
            futures[index] = supplyAsync(() -> {
                partition.sort(comparator);
                allocateBytes.accept(sortOverheadBytes(partition.size()));
                return partition;
            }, executor);
            index++;
        }
        return CompletableFuture.allOf(futures)
            .thenApply(_ -> {

                ArrayList<T> result = new ArrayList<>(list.size());
                ArrayList<List<T>> parts = new ArrayList<>(futures.length);
                for (int i = 0; i < futures.length; i++) {
                    parts.add(futures[i].join());
                }
                Iterables.addAll(result, Iterables.mergeSorted(parts, comparator));
                allocateBytes.accept(RamUsageEstimator.sizeOfObject(parts));
                allocateBytes.accept(RamUsageEstimator.sizeOfObject(result));
                return result;
            });
    }

    /**
     * Inlining {@link RamUsageEstimator#shallowSizeOf(Object[])}
     * to account for intermediate sorting array.
     */
    static long sortOverheadBytes(int size) {
        return RamUsageEstimator.alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * size);
    }
}
