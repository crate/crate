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

package io.crate.execution.engine.collect.count;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.Supplier;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.cursors.IntCursor;

import io.crate.common.concurrent.CompletableFutures;
import io.crate.exceptions.JobKilledException;
import io.crate.execution.support.ThreadPools;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;

@Singleton
public class InternalCountOperation implements CountOperation {

    private final LuceneQueryBuilder queryBuilder;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ThreadPoolExecutor executor;
    private final int numProcessors;
    private final Schemas schemas;

    @Inject
    public InternalCountOperation(Settings settings,
                                  NodeContext nodeContext,
                                  LuceneQueryBuilder queryBuilder,
                                  ClusterService clusterService,
                                  ThreadPool threadPool,
                                  IndicesService indicesService) {
        this.schemas = nodeContext.schemas();
        this.queryBuilder = queryBuilder;
        this.clusterService = clusterService;
        executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        this.indicesService = indicesService;
        this.numProcessors = EsExecutors.numberOfProcessors(settings);
    }

    @Override
    public CompletableFuture<Long> count(TransactionContext txnCtx,
                                         Map<String, IntIndexedContainer> indexShardMap,
                                         Symbol filter) {
        List<CompletableFuture<Supplier<Long>>> futureSuppliers = new ArrayList<>();
        Metadata metadata = clusterService.state().metadata();
        for (Map.Entry<String, IntIndexedContainer> entry : indexShardMap.entrySet()) {
            String indexName = entry.getKey();
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata == null) {
                if (IndexParts.isPartitioned(indexName)) {
                    continue;
                }
                throw new IndexNotFoundException(indexName);
            }
            final Index index = indexMetadata.getIndex();
            for (IntCursor shardCursor : entry.getValue()) {
                int shardValue = shardCursor.value;
                futureSuppliers.add(prepareGetCount(txnCtx, index, shardValue, filter));
            }
        }
        MergePartialCountFunction mergeFunction = new MergePartialCountFunction();

        return CompletableFutures.allAsList(futureSuppliers)
            .thenCompose(suppliers -> ThreadPools.runWithAvailableThreads(
                executor,
                ThreadPools.numIdleThreads(executor, numProcessors),
                suppliers
            )).thenApply(mergeFunction);
    }

    public CompletableFuture<Supplier<Long>> prepareGetCount(TransactionContext txnCtx, Index index, int shardId, Symbol filter) {
        IndexService indexService;
        try {
            indexService = indicesService.indexServiceSafe(index);
        } catch (IndexNotFoundException e) {
            if (IndexParts.isPartitioned(index.getName())) {
                return CompletableFuture.completedFuture(() -> 0L);
            } else {
                return CompletableFuture.failedFuture(e);
            }
        }
        IndexShard indexShard = indexService.getShard(shardId);
        CompletableFuture<Supplier<Long>> futureCount = new CompletableFuture<>();
        indexShard.awaitShardSearchActive(b -> {
            try {
                futureCount.complete(() -> syncCount(indexService, indexShard, txnCtx, filter));
            } catch (Throwable t) {
                futureCount.completeExceptionally(t);
            }
        });
        return futureCount;
    }

    @VisibleForTesting
    long syncCount(IndexService indexService,
                   IndexShard indexShard,
                   TransactionContext txnCtx,
                   Symbol filter) {
        try (Engine.Searcher searcher = indexShard.acquireSearcher("count-operation")) {
            String indexName = indexShard.shardId().getIndexName();
            var relationName = RelationName.fromIndexName(indexName);
            DocTableInfo table = schemas.getTableInfo(relationName);
            LuceneQueryBuilder.Context queryCtx = queryBuilder.convert(
                filter,
                txnCtx,
                indexName,
                indexService.newQueryShardContext(),
                table,
                indexService.cache()
            );
            if (Thread.interrupted()) {
                throw JobKilledException.of("thread interrupted during count-operation");
            }
            return searcher.count(queryCtx.query());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (IllegalIndexShardStateException e) {
            if (IndexParts.isPartitioned(e.getIndex().getName())) {
                return 0L;
            }
            throw e;
        }
    }

    private static class MergePartialCountFunction implements Function<List<Long>, Long> {

        @Override
        public Long apply(List<Long> partialResults) {
            long result = 0L;
            for (Long partialResult : partialResults) {
                result += partialResult;
            }
            return result;
        }
    }
}
