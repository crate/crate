package org.cratedb.service;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.stats.ShardStatsTable;
import org.cratedb.stats.StatsTable;
import org.cratedb.stats.StatsTableUnknownException;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;

import java.util.List;
import java.util.Map;

public class StatsService extends AbstractLifecycleComponent<StatsService> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final Map<String, AggFunction> aggFunctionMap;
    protected final ESLogger logger;

    @Inject
    public StatsService(Settings settings, ClusterService clusterService,
                        IndicesService indicesService,
                        Map<String, AggFunction> aggFunctionMap) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.aggFunctionMap = aggFunctionMap;
        logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    protected void doStart() throws ElasticSearchException {

        logger.info("starting...");

    }

    @Override
    protected void doStop() throws ElasticSearchException {
        logger.info("stopping...");
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    public Map<String, Map<GroupByKey, GroupByRow>> queryGroupBy(String virtualTableName,
                                                                 String[]
                                                                 reducers,
                                                                 String concreteIndex,
                                                                 ParsedStatement stmt,
                                                                 int shardId)
            throws Exception
    {
        StatsTable statsTable = newStatsTable(virtualTableName, concreteIndex, shardId);

        return statsTable.queryGroupBy(reducers, stmt);
    }

    public List<List<Object>> query(String virtualTableName,
                                    String concreteIndex,
                                    ParsedStatement stmt,
                                    int shardId) throws Exception
    {
        StatsTable statsTable = newStatsTable(virtualTableName, concreteIndex, shardId);

        return statsTable.query(stmt);
    }

    private StatsTable newStatsTable(String virtualTableName, String concreteIndex,
                                     int shardId) throws Exception {
        if (virtualTableName.equalsIgnoreCase("shards")) {
            return newShardsStatsTable(concreteIndex, shardId);
        }
        throw new StatsTableUnknownException(virtualTableName);
    }

    private ShardStatsTable newShardsStatsTable(String concreteIndex, int shardId) throws Exception {
        final InternalIndexShard shard =
                (InternalIndexShard) indicesService.indexServiceSafe(concreteIndex).shardSafe(shardId);

        return new ShardStatsTable(aggFunctionMap, concreteIndex, shard,
                clusterService.localNode().id());

    }

}
