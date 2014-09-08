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

package io.crate.action.sql.query;

import io.crate.Constants;
import io.crate.executor.transport.task.elasticsearch.ESQueryBuilder;
import io.crate.metadata.Routing;
import io.crate.planner.node.dql.ESSearchNode;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.InternalSearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;

public class CrateSearchService extends InternalSearchService {

    private static final ESQueryBuilder ESQueryBuilder = new ESQueryBuilder();

    @Inject
    public CrateSearchService(Settings settings,
                              ClusterService clusterService,
                              IndicesService indicesService,
                              IndicesLifecycle indicesLifecycle,
                              IndicesWarmer indicesWarmer,
                              ThreadPool threadPool,
                              ScriptService scriptService,
                              CacheRecycler cacheRecycler,
                              PageCacheRecycler pageCacheRecycler,
                              BigArrays bigArrays,
                              DfsPhase dfsPhase,
                              QueryPhase queryPhase,
                              FetchPhase fetchPhase) {
        super(settings, clusterService, indicesService, indicesLifecycle,
                indicesWarmer,
                threadPool,
                scriptService,
                cacheRecycler, pageCacheRecycler, bigArrays, dfsPhase, queryPhase, fetchPhase);
    }

    public QuerySearchResult executeQueryPhase(QueryShardRequest request) {
        SearchContext context = createAndPutContext(request);
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);
            queryPhase.execute(context);

            assert context.searchType() != SearchType.COUNT : "searchType COUNT is not supported using QueryShardRequests";
            contextProcessedSuccessfully(context);

            context.indexShard().searchService().onQueryPhase(context, System.nanoTime() - time);
            return context.queryResult();
        } catch (Throwable e) {
            context.indexShard().searchService().onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            freeContext(context);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    private SearchContext createAndPutContext(QueryShardRequest request) {
        SearchContext context = createContext(request, null);
        boolean success = false;
        try {
            activeContexts.put(context.id(), context);
            context.indexShard().searchService().onNewContext(context);
            success = true;
            return context;
        } finally {
            if (!success) {
                freeContext(context);
            }
        }
    }

    /**
     * Creates a new SearchContext. <br />
     * <p>
     * This is similar to
     * {@link org.elasticsearch.search.InternalSearchService#createContext(org.elasticsearch.search.internal.ShardSearchRequest, org.elasticsearch.index.engine.Engine.Searcher)}
     * but uses Symbols to create the lucene query / sorting.
     * </p>
     *
     * <p>
     * Note: Scrolling isn't supported.
     * </p>
     */
    private SearchContext createContext(QueryShardRequest request, @Nullable Engine.Searcher searcher) {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());

        SearchShardTarget searchShardTarget = new SearchShardTarget(
                clusterService.localNode().id(),
                request.index(),
                request.shardId()
        );

        Engine.Searcher engineSearcher = searcher == null ? indexShard.acquireSearcher("search") : searcher;

        ShardSearchRequest shardSearchRequest = new ShardSearchRequest();
        shardSearchRequest.types(new String[] {Constants.DEFAULT_MAPPING_TYPE });

        // TODO: use own CrateSearchContext that doesn't require ShardSearchRequest
        SearchContext context = new DefaultSearchContext(
                idGenerator.incrementAndGet(),
                shardSearchRequest,
                searchShardTarget,
                engineSearcher,
                indexService,
                indexShard,
                scriptService,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays
        );
        SearchContext.setCurrent(context);

        try {
            ESSearchNode searchNode = new ESSearchNode(
                    new Routing(null),
                    request.outputs(),
                    request.orderBy(),
                    request.reverseFlags(),
                    request.nullsFirst(),
                    request.limit(),
                    request.offset(),
                    request.whereClause(),
                    request.partitionBy()
            );

            // TODO: remove xcontent
            BytesReference source = ESQueryBuilder.convert(searchNode);
            parseSource(context, source);


            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(0);
            }
            if (context.size() == -1) {
                context.size(10);
            }

            // pre process
            dfsPhase.preProcess(context);
            queryPhase.preProcess(context);
            fetchPhase.preProcess(context);

            // compute the context keep alive
            long keepAlive = defaultKeepAlive;
            context.keepAlive(keepAlive);
        } catch (Throwable e) {
            context.close();
            throw ExceptionsHelper.convertToRuntime(e);
        }
        return context;
    }

}
