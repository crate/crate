/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.action;

import org.apache.lucene.search.Query;
import org.cratedb.Constants;
import org.cratedb.action.collect.CollectorContext;
import org.cratedb.action.collect.scope.ScopedExpression;
import org.cratedb.action.groupby.GlobalSQLGroupingCollector;
import org.cratedb.action.groupby.SQLGroupingCollector;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.GlobalExpressionService;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.util.Map;

/**
 * Currently the SQLQueryService is used to query Lucene and gather the result using the
 * {@link SQLGroupingCollector}
 *
 * Therefore it is currently only used for group by queries.
 */
public class SQLQueryService {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;
    private final SQLXContentQueryParser parser;
    private final IndicesService indicesService;
    private final Map<String, AggFunction> aggFunctionMap;
    private final GlobalExpressionService globalExpressionService;

    @Inject
    public SQLQueryService(ClusterService clusterService,
                           ScriptService scriptService,
                           IndicesService indicesService,
                           Map<String, AggFunction> aggFunctionMap,
                           SQLXContentQueryParser sqlxContentQueryParser,
                           CacheRecycler cacheRecycler,
                           GlobalExpressionService globalExpressionService)
    {
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.indicesService = indicesService;
        this.parser = sqlxContentQueryParser;
        this.aggFunctionMap = aggFunctionMap;
        this.globalExpressionService = globalExpressionService;
    }

    public Rows query(int numReducers, String concreteIndex, ParsedStatement stmt, int shardId)
        throws Exception
    {
        SearchContext searchContext = buildSearchContext(concreteIndex, shardId);
        SearchContext.setCurrent(searchContext);

        if (logger.isTraceEnabled()) {
            logger.trace("Parsing xcontentQuery:\n " + stmt.xcontent.toUtf8());
        }
        parser.parse(searchContext, stmt.xcontent);
        searchContext.preProcess();

        applyScope(stmt, concreteIndex, shardId);

        // TODO: prematch and change query
        Query query = searchContext.query();

        SQLGroupingCollector collector;
        CollectorContext collectorContext = new CollectorContext().searchContext(searchContext);
        if (stmt.isGlobalAggregate()) {
            collector = new GlobalSQLGroupingCollector(
                    stmt,
                    collectorContext,
                    aggFunctionMap,
                    numReducers
            );
        } else {
            collector = new SQLGroupingCollector(
                    stmt,
                    collectorContext,
                    aggFunctionMap,
                    numReducers
            );
        }

        try {
            searchContext.searcher().search(query, collector);
        } finally {
            searchContext.release();
            SearchContext.removeCurrent();
        }

        return collector.rows();
    }

    private SearchContext buildSearchContext(String concreteIndex, int shardId) {
        SearchShardTarget shardTarget = new SearchShardTarget(
            clusterService.localNode().id(), concreteIndex, shardId
        );

        IndexService indexService = indicesService.indexServiceSafe(concreteIndex);
        IndexShard indexShard = indexService.shardSafe(shardId);
        ShardSearchRequest request = new ShardSearchRequest();
        request.types(new String[]{Constants.DEFAULT_MAPPING_TYPE});

        return new SearchContext(0,
            request, shardTarget, indexShard.acquireSearcher("search"), indexService,
            indexShard, scriptService, cacheRecycler
        );
    }

    private void applyScope(ParsedStatement stmt, String concreteIndex, int shardId) {
        String nodeId = clusterService.localNode().id();
        if (stmt.globalExpressionCount() > 0) {
            for (ScopedExpression<?> expression: stmt.globalExpressions()) {
                expression.applyScope(nodeId, concreteIndex, shardId);
            }
        }
    }
}
