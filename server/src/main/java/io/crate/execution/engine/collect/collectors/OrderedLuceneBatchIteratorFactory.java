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

package io.crate.execution.engine.collect.collectors;

import static java.util.Collections.singletonList;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.IntSupplier;

import org.elasticsearch.index.shard.ShardId;

import io.crate.common.collections.Lists;
import io.crate.common.concurrent.KillableCompletionStage;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.breaker.RowAccounting;
import io.crate.execution.engine.distribution.merge.BatchPagingIterator;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import io.crate.execution.engine.distribution.merge.PassThroughPagingIterator;
import io.crate.execution.engine.distribution.merge.RamAccountingPageIterator;
import io.crate.execution.support.ThreadPools;

/**
 * Factory to create a BatchIterator which is backed by 1 or more {@link OrderedDocCollector}.
 * This BatchIterator exposes data stored in a Lucene index and utilizes Lucene sort for efficient sorting.
 */
public class OrderedLuceneBatchIteratorFactory {

    public static BatchIterator<Row> newInstance(List<OrderedDocCollector> orderedDocCollectors,
                                                 Comparator<Row> rowComparator,
                                                 RowAccounting<Row> rowAccounting,
                                                 Executor executor,
                                                 IntSupplier availableThreads,
                                                 boolean requiresScroll) {
        return new Factory(
            orderedDocCollectors, rowComparator, rowAccounting, executor, availableThreads, requiresScroll).create();
    }

    private static class Factory {

        private final List<OrderedDocCollector> orderedDocCollectors;
        private final Executor executor;
        private final IntSupplier availableThreads;
        private final PagingIterator<ShardId, Row> pagingIterator;
        private final Map<ShardId, OrderedDocCollector> collectorsByShardId;

        Factory(List<OrderedDocCollector> orderedDocCollectors,
                Comparator<Row> rowComparator,
                RowAccounting<Row> rowAccounting,
                Executor executor,
                IntSupplier availableThreads,
                boolean requiresScroll) {
            this.orderedDocCollectors = orderedDocCollectors;
            this.executor = executor;
            this.availableThreads = availableThreads;
            if (orderedDocCollectors.size() == 1) {
                pagingIterator = requiresScroll ?
                    new RamAccountingPageIterator<>(PassThroughPagingIterator.repeatable(), rowAccounting)
                    : PassThroughPagingIterator.oneShot();
                collectorsByShardId = null;
            } else {
                collectorsByShardId = toMapByShardId(orderedDocCollectors);
                pagingIterator = new RamAccountingPageIterator<>(
                    PagingIterator.createSorted(rowComparator, requiresScroll),
                    rowAccounting
                );
            }
        }

        BatchIterator<Row> create() {
            return new BatchPagingIterator<>(
                pagingIterator,
                this::tryFetchMore,
                this::allExhausted,
                throwable -> close()
            );
        }

        private KillableCompletionStage<List<KeyIterable<ShardId, Row>>> tryFetchMore(ShardId shardId) {
            if (allExhausted()) {
                return KillableCompletionStage.whenKilled(
                    CompletableFuture.failedFuture(new IllegalStateException("Cannot fetch more if source is exhausted")),
                    t -> { });
            }
            CompletionStage<List<KeyIterable<ShardId, Row>>> stage;
            if (shardId == null) {
                // when running inside threads, the threads must be cancelled/interrupted to stop further processing
                stage = ThreadPools.runWithAvailableThreads(
                        executor,
                        availableThreads,
                        Lists.map(orderedDocCollectors, Function.identity()));
            } else {
                stage = loadFrom(collectorsByShardId.get(shardId));
            }
            return KillableCompletionStage.whenKilled(stage, this::kill);
        }

        private static CompletionStage<List<KeyIterable<ShardId, Row>>> loadFrom(OrderedDocCollector collector) {
            try {
                return CompletableFuture.completedFuture(singletonList(collector.get()));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        private void close() {
            for (OrderedDocCollector collector : orderedDocCollectors) {
                collector.close();
            }
        }

        private boolean allExhausted() {
            for (OrderedDocCollector collector : orderedDocCollectors) {
                if (!collector.exhausted) {
                    return false;
                }
            }
            return true;
        }

        private void kill(Throwable t) {
            for (OrderedDocCollector collector : orderedDocCollectors) {
                collector.kill(t);
            }
        }
    }


    private static Map<ShardId, OrderedDocCollector> toMapByShardId(List<OrderedDocCollector> collectors) {
        Map<ShardId, OrderedDocCollector> collectorsByShardId = new HashMap<>(collectors.size());
        for (OrderedDocCollector collector : collectors) {
            collectorsByShardId.put(collector.shardId(), collector);
        }
        return collectorsByShardId;
    }
}
