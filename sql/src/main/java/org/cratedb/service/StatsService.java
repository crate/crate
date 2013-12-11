package org.cratedb.service;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.key.Rows;
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

    public Rows queryGroupBy(String virtualTableName,
            int numReducers,
            String concreteIndex,
            ParsedStatement stmt,
            int shardId)
            throws Exception
    {
        switch (virtualTableName.toLowerCase()) {
            case "shards":
                StatsInfo shardInfo = newShardInfo(concreteIndex, shardId);
                return shardStatsTable.queryGroupBy(numReducers, stmt, shardInfo);
            default:
                throw new StatsTableUnknownException(virtualTableName);
        }

    }

    public List<List<Object>> query(String virtualTableName,
                                    String concreteIndex,
                                    ParsedStatement stmt,
                                    int shardId) throws Exception
    {
        switch (virtualTableName.toLowerCase()) {
            case "shards":
                StatsInfo shardInfo = newShardInfo(concreteIndex, shardId);
                return shardStatsTable.query(stmt, shardInfo);
            default:
                throw new StatsTableUnknownException(virtualTableName);
        }

    }

    private StatsInfo newShardInfo(String index, int shardId) {
        final InternalIndexShard shard =
                (InternalIndexShard) indicesService.indexServiceSafe(index).shardSafe(shardId);
        final String nodeId = (shard == null) ? null : clusterService.localNode().id();

        return new ShardStatsTable.ShardInfo(index, nodeId, shardId, shard);
    }


}
