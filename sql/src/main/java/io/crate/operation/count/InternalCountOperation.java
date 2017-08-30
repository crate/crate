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

package io.crate.operation.count;

import io.crate.analyze.WhereClause;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.IndexParts;
import io.crate.operation.ThreadPools;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.Supplier;

@Singleton
public class InternalCountOperation implements CountOperation {

    private final LuceneQueryBuilder queryBuilder;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ThreadPoolExecutor executor;
    private final int corePoolSize;

    @Inject
    public InternalCountOperation(LuceneQueryBuilder queryBuilder,
                                  ClusterService clusterService,
                                  ThreadPool threadPool,
                                  IndicesService indicesService) {
        this.queryBuilder = queryBuilder;
        this.clusterService = clusterService;
        executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        corePoolSize = executor.getMaximumPoolSize();
        this.indicesService = indicesService;
    }

    @Override
    public CompletableFuture<Long> count(Map<String, ? extends Collection<Integer>> indexShardMap,
                                         final WhereClause whereClause) throws IOException, InterruptedException {

        List<Supplier<Long>> suppliers = new ArrayList<>();
        MetaData metaData = clusterService.state().getMetaData();
        for (Map.Entry<String, ? extends Collection<Integer>> entry : indexShardMap.entrySet()) {
            String indexName = entry.getKey();
            IndexMetaData indexMetaData = metaData.index(indexName);
            if (indexMetaData == null) {
                if (IndexParts.isPartitioned(indexName)) {
                    continue;
                }
                throw new IndexNotFoundException(indexName);
            }
            final Index index = indexMetaData.getIndex();
            for (final Integer shardId : entry.getValue()) {
                suppliers.add(() -> {
                    try {
                        return count(index, shardId, whereClause);
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
        MergePartialCountFunction mergeFunction = new MergePartialCountFunction();
        CompletableFuture<List<Long>> futurePartialCounts = ThreadPools.runWithAvailableThreads(
            executor, corePoolSize, suppliers, mergeFunction);
        return futurePartialCounts.thenApply(mergeFunction);
    }

    @Override
    public long count(Index index, int shardId, WhereClause whereClause) throws IOException, InterruptedException {
        IndexService indexService;
        try {
            indexService = indicesService.indexServiceSafe(index);
        } catch (IndexNotFoundException e) {
            if (IndexParts.isPartitioned(index.getName())) {
                return 0L;
            }
            throw e;
        }

        IndexShard indexShard = indexService.getShard(shardId);
        try (Engine.Searcher searcher = indexShard.acquireSearcher("count-operation")) {
            LuceneQueryBuilder.Context queryCtx = queryBuilder.convert(
                whereClause,
                indexService.mapperService(),
                indexService.newQueryShardContext(shardId, searcher.reader(), System::currentTimeMillis),
                indexService.fieldData(),
                indexService.cache());
            if (Thread.interrupted()) {
                throw new InterruptedException("thread interrupted during count-operation");
            }
            return searcher.searcher().count(queryCtx.query());
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
