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

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.job.SharedShardContexts;
import io.crate.action.job.SharedShardContext;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.TableUnknownException;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.PartitionName;
import io.crate.operation.ThreadPools;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

@Singleton
public class InternalCountOperation implements CountOperation {

    private final LuceneQueryBuilder queryBuilder;
    private final ThreadPoolExecutor executor;
    private final int corePoolSize;

    @Inject
    public InternalCountOperation(ScriptService scriptService,
                                  LuceneQueryBuilder queryBuilder,
                                  ThreadPool threadPool) {
        this.queryBuilder = queryBuilder;
        executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        corePoolSize = executor.getCorePoolSize();
    }

    @Override
    public ListenableFuture<Long> count(Map<String, ? extends Collection<Integer>> indexShardMap,
                                        final WhereClause whereClause,
                                        final SharedShardContexts sharedShardContexts)
            throws IOException, InterruptedException {

        List<Callable<Long>> callableList = new ArrayList<>();
        for (Map.Entry<String, ? extends Collection<Integer>> entry : indexShardMap.entrySet()) {
            final String index = entry.getKey();
            for (final Integer shardId : entry.getValue()) {
                callableList.add(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        return count(index, shardId, whereClause, sharedShardContexts);
                    }
                });
            }
        }
        ListenableFuture<List<Long>> listListenableFuture = ThreadPools.runWithAvailableThreads(
                executor, corePoolSize, callableList, new MergePartialCountFunction());

        return Futures.transform(listListenableFuture, new MergePartialCountFunction());
    }

    @Override
    public long count(String index, int shardId, WhereClause whereClause, SharedShardContexts sharedShardContexts)
            throws IOException, InterruptedException {

        SharedShardContext allocatedShardContext = sharedShardContexts.getOrCreateContext(new ShardId(index, shardId));
        Engine.Searcher searcher = null;
        try {
            IndexService indexService = allocatedShardContext.indexService();
            searcher = allocatedShardContext.searcher();
            LuceneQueryBuilder.Context queryCtx = queryBuilder.convert(
                    whereClause, indexService.mapperService(), indexService.fieldData(), indexService.cache());
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            return Lucene.count(searcher.searcher(), queryCtx.query());
        } catch (IndexMissingException e) {
            if (PartitionName.isPartition(index)) {
                return 0L;
            }
            throw new TableUnknownException(index, e);
        } finally {
            if (searcher != null) {
                searcher.close();
            }
        }
    }

    private static class MergePartialCountFunction implements Function<List<Long>, Long> {
        @Nullable
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
