package org.cratedb.action.collect.scope;

import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;

public abstract class ShardLevelExpression<ReturnType> implements ScopedExpression<ReturnType> {

    private final IndicesService indicesService;
    private IndexService indexService;
    private IndexShard indexShard;

    public ShardLevelExpression(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public ExpressionScope getScope() {
        return ExpressionScope.SHARD;
    }

    @Override
    public void applyScope(String nodeId, String indexName, int shardId) {
        this.indexService = this.indicesService.indexServiceSafe(indexName);
        this.indexShard = this.indexService.shardSafe(shardId);
    }
}
