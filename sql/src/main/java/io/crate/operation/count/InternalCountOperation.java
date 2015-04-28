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
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

@Singleton
public class InternalCountOperation implements CountOperation {

    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;
    private final PageCacheRecycler pageCacheRecycler;
    private final LuceneQueryBuilder queryBuilder;
    private final BigArrays bigArrays;
    private final ThreadPool threadPool;
    private final IndicesService indicesService;

    @Inject
    public InternalCountOperation(ClusterService clusterService,
                                  ScriptService scriptService,
                                  CacheRecycler cacheRecycler,
                                  PageCacheRecycler pageCacheRecycler,
                                  LuceneQueryBuilder queryBuilder,
                                  BigArrays bigArrays,
                                  ThreadPool threadPool,
                                  IndicesService indicesService) {
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        this.queryBuilder = queryBuilder;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.indicesService = indicesService;
    }

    @Override
    public long count(Map<String, ? extends Collection<Integer>> indexShardMap, WhereClause whereClause) throws IOException {
        long count = 0L;
        for (Map.Entry<String, ? extends Collection<Integer>> entry : indexShardMap.entrySet()) {
            String index = entry.getKey();
            for (Integer shardId : entry.getValue()) {
                count += count(index, shardId, whereClause);
            }
        }
        return count;
    }

    @Override
    public long count(String index, int shardId, WhereClause whereClause) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(index);
        IndexShard indexShard = indexService.shardSafe(shardId);

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), index, shardId);
        SearchContext context = new DefaultSearchContext(0,
                new ShardSearchLocalRequest(
                        Strings.EMPTY_ARRAY,
                        System.currentTimeMillis(),
                        null
                ),
                shardTarget,
                indexShard.acquireSearcher("count-operation"),
                indexService,
                indexShard,
                scriptService,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays,
                threadPool.estimatedTimeInMillisCounter()
        );
        SearchContext.setCurrent(context);

        try {
            LuceneQueryBuilder.Context queryCtx = queryBuilder.convert(whereClause, context, indexService.cache());
            return Lucene.count(context.searcher(), queryCtx.query());
        } finally {
            context.close();
            SearchContext.removeCurrent();
        }
    }
}
