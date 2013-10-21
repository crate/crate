package org.cratedb.action;

import org.apache.lucene.search.Query;
import org.cratedb.action.groupby.*;
import org.cratedb.action.groupby.aggregate.AggFunction;
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

    public Map<String, Map<Integer, GroupByRow>> query(String[] reducers,
                                                       ParsedStatement stmt, int shardId)
        throws Exception
    {
        SearchContext context = buildSearchContext(stmt, shardId);
        SearchContext.setCurrent(context);
        logger.trace("Parsing xcontentQuery:\n " + stmt.getXContentAsBytesRef().toUtf8());
        parser.parse(context, stmt.getXContentAsBytesRef());
        context.preProcess();

        Query query = context.query();
        SQLGroupingCollector collector = new SQLGroupingCollector(
            stmt,
            new ESDocLookup(context.lookup().doc()),
            aggFunctionMap,
            reducers
        );

        context.searcher().search(query, collector);
        context.release();
        SearchContext.removeCurrent();

        return collector.partitionedResult;
    }

    private SearchContext buildSearchContext(ParsedStatement stmt, int shardId) {
        assert stmt.indices().size() == 1;
        SearchShardTarget shardTarget = new SearchShardTarget(
            clusterService.localNode().id(), stmt.indices().get(0), shardId
        );

        IndexService indexService = indicesService.indexServiceSafe(stmt.indices().get(0));
        IndexShard indexShard = indexService.shardSafe(shardId);

        return new SearchContext(0,
            new ShardSearchRequest(), shardTarget, indexShard.acquireSearcher(), indexService,
            indexShard, scriptService, cacheRecycler
        );
    }
}
