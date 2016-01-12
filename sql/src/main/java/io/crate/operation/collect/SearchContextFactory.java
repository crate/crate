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

package io.crate.operation.collect;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.analyze.WhereClause;
import io.crate.jobs.JobContextService;
import io.crate.lucene.LuceneQueryBuilder;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.threadpool.ThreadPool;

@Singleton
public class SearchContextFactory {

    private LuceneQueryBuilder luceneQueryBuilder;
    private ClusterService clusterService;
    private final ScriptService scriptService;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final ThreadPool threadPool;
    private final TimeValue jobKeepAlive;
    private final ImmutableMap<String, Query> EMPTY_NAMED_FILTERS = ImmutableMap.of();

    @Inject
    public SearchContextFactory(LuceneQueryBuilder luceneQueryBuilder,
                                ClusterService clusterService,
                                ScriptService scriptService,
                                PageCacheRecycler pageCacheRecycler,
                                BigArrays bigArrays,
                                ThreadPool threadPool,
                                @JobContextService.JobKeepAlive TimeValue jobKeepAlive) {
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.jobKeepAlive = jobKeepAlive;
    }

    public CrateSearchContext createContext(
            int jobSearchContextId,
            IndexShard indexshard,
            Engine.Searcher engineSearcher,
            WhereClause whereClause) {

        ShardId shardId = indexshard.shardId();
        SearchShardTarget searchShardTarget = new SearchShardTarget(
                clusterService.state().nodes().localNodeId(),
                shardId.getIndex(),
                shardId.id()
        );
        IndexService indexService = indexshard.indexService();
        CrateSearchContext searchContext = new CrateSearchContext(
                jobSearchContextId,
                System.currentTimeMillis(),
                searchShardTarget,
                engineSearcher,
                indexService,
                indexshard,
                scriptService,
                pageCacheRecycler,
                bigArrays,
                threadPool.estimatedTimeInMillisCounter(),
                Optional.<Scroll>absent(),
                jobKeepAlive.getMillis()
        );
        LuceneQueryBuilder.Context context = luceneQueryBuilder.convert(
                whereClause,  indexService.mapperService(), indexService.fieldData(), indexService.cache());
        searchContext.parsedQuery(new ParsedQuery(context.query(), EMPTY_NAMED_FILTERS));

        Float minScore = context.minScore();
        if (minScore != null) {
            searchContext.minimumScore(minScore);
        }

        return searchContext;
    }
}
