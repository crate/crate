package org.cratedb.service;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.stats.ShardStatsTable;
import org.cratedb.stats.StatsInfo;
import org.cratedb.stats.StatsTableUnknownException;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;

import java.util.List;
import java.util.Map;

public class StatsService extends AbstractLifecycleComponent<StatsService> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final ShardStatsTable shardStatsTable;
    private final Map<String, AggFunction> aggFunctionMap;
    protected final ESLogger logger;

    @Inject
    public StatsService(Settings settings, ClusterService clusterService,
                        IndicesService indicesService,
                        ShardStatsTable shardStatsTable,
                        Map<String, AggFunction> aggFunctionMap) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStatsTable = shardStatsTable;
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
                                                                 int shardId,
                                                                 String nodeId)
            throws Exception
    {
        switch (virtualTableName.toLowerCase()) {
            case "shards":
                StatsInfo shardInfo = newShardInfo(concreteIndex, shardId, nodeId);
                return shardStatsTable.queryGroupBy(reducers, stmt, shardInfo);
            default:
                throw new StatsTableUnknownException(virtualTableName);
        }

    }

    public List<List<Object>> query(String virtualTableName,
                                    String concreteIndex,
                                    ParsedStatement stmt,
                                    int shardId,
                                    String nodeId) throws Exception
    {
        switch (virtualTableName.toLowerCase()) {
            case "shards":
                StatsInfo shardInfo = newShardInfo(concreteIndex, shardId, nodeId);
                return shardStatsTable.query(stmt, shardInfo);
            default:
                throw new StatsTableUnknownException(virtualTableName);
        }

    }

    private StatsInfo newShardInfo(String index, int shardId, String nodeId) {
        InternalIndexShard shard = null;
        if (nodeId != null) {
            try {
                shard = (InternalIndexShard) indicesService.indexServiceSafe(index).shardSafe(shardId);
            } catch (IndexShardMissingException e) {
                // shard is not yet unassigned, do nothing, ShardStatsTable handles this
            }
        }

        return new ShardStatsTable.ShardInfo(index, nodeId, shardId, shard);
    }


}
