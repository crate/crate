package org.cratedb.action;

import org.apache.lucene.search.Query;
import org.cratedb.Constants;
import org.cratedb.action.collect.CollectorContext;
import org.cratedb.action.groupby.GlobalSQLGroupingCollector;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.SQLGroupingCollector;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
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

    @Inject
    public SQLQueryService(ClusterService clusterService,
                           ScriptService scriptService,
                           IndicesService indicesService,
                           Map<String, AggFunction> aggFunctionMap,
                           SQLXContentQueryParser sqlxContentQueryParser,
                           CacheRecycler cacheRecycler)
    {
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.indicesService = indicesService;
        this.parser = sqlxContentQueryParser;
        this.aggFunctionMap = aggFunctionMap;
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
}
