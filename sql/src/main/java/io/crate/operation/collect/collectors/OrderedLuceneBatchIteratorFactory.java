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

package io.crate.operation.collect.collectors;

import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.operation.merge.BatchPagingIterator;
import io.crate.operation.merge.KeyIterable;
import io.crate.operation.merge.PagingIterator;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.operation.merge.SortedPagingIterator;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Factory to create a BatchIterator which is backed by 1 or more {@link OrderedDocCollector}.
 * This BatchIterator exposes data stored in a Lucene index and utilizes Lucene sort for efficient sorting.
 */
public class OrderedLuceneBatchIteratorFactory {

    public static BatchIterator<Row> newInstance(List<OrderedDocCollector> orderedDocCollectors,
                                                 Comparator<Row> rowComparator,
                                                 Executor executor,
                                                 boolean requiresScroll) {
        return new Factory(
            orderedDocCollectors, rowComparator, executor, requiresScroll).create();
    }

    private static class Factory {

        private final List<OrderedDocCollector> orderedDocCollectors;
        private final Executor executor;
        private final PagingIterator<ShardId, Row> pagingIterator;
        private final Map<ShardId, OrderedDocCollector> collectorsByShardId;

        private BatchPagingIterator<ShardId> batchPagingIterator;

        Factory(List<OrderedDocCollector> orderedDocCollectors,
                Comparator<Row> rowComparator,
                Executor executor,
                boolean requiresScroll) {
            this.orderedDocCollectors = orderedDocCollectors;
            this.executor = executor;
            if (orderedDocCollectors.size() == 1) {
                pagingIterator = requiresScroll ?
                    PassThroughPagingIterator.repeatable() : PassThroughPagingIterator.oneShot();
                collectorsByShardId = null;
            } else {
                collectorsByShardId = toMapByShardId(orderedDocCollectors);
                pagingIterator = new SortedPagingIterator<>(rowComparator, requiresScroll);
            }
        }

        BatchIterator<Row> create() {
            batchPagingIterator = new BatchPagingIterator<>(
                pagingIterator,
                this::tryFetchMore,
                this::allExhausted,
                this::close
            );
            return batchPagingIterator;
        }

        private boolean tryFetchMore(ShardId shardId) {
            if (allExhausted()) {
                return false;
            }
            if (shardId == null) {
                loadFromAllUnExhausted(orderedDocCollectors, executor).whenComplete(this::onNextRows);
                return true;
            } else {
                return loadFrom(collectorsByShardId.get(shardId));
            }
        }

        private boolean loadFrom(OrderedDocCollector collector) {
            KeyIterable<ShardId, Row> rows;
            try {
                rows = collector.get();
            } catch (Exception e) {
                batchPagingIterator.completeLoad(e);
                return true;
            }
            mergeAndMaybeFinish(Collections.singletonList(rows));
            batchPagingIterator.completeLoad(null);
            return true;
        }

        private void onNextRows(List<KeyIterable<ShardId, Row>> rowsList, @Nullable Throwable throwable) {
            if (throwable == null) {
                mergeAndMaybeFinish(rowsList);
            }
            batchPagingIterator.completeLoad(throwable);
        }

        private void mergeAndMaybeFinish(List<KeyIterable<ShardId, Row>> rowsList) {
            pagingIterator.merge(rowsList);
            if (allExhausted()) {
                pagingIterator.finish();
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
    }

    private static CompletableFuture<List<KeyIterable<ShardId, Row>>> loadFromAllUnExhausted(List<OrderedDocCollector> collectors,
                                                                                             Executor executor) {
        List<CompletableFuture<KeyIterable<ShardId, Row>>> futures = new ArrayList<>(collectors.size());
        for (OrderedDocCollector collector : collectors.subList(1, collectors.size())) {
            try {
                futures.add(CompletableFuture.supplyAsync(collector, executor));
            } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                futures.add(CompletableFuture.completedFuture(collector.get()));
            }
        }
        futures.add(CompletableFuture.completedFuture(collectors.get(0).get()));
        return CompletableFutures.allAsList(futures);
    }

    private static Map<ShardId, OrderedDocCollector> toMapByShardId(List<OrderedDocCollector> collectors) {
        Map<ShardId, OrderedDocCollector> collectorsByShardId = new HashMap<>(collectors.size());
        for (OrderedDocCollector collector : collectors) {
            collectorsByShardId.put(collector.shardId(), collector);
        }
        return collectorsByShardId;
    }
}
