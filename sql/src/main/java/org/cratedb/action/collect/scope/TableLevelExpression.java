package org.cratedb.action.collect.scope;

import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;

public abstract class TableLevelExpression<ReturnType> implements ScopedExpression<ReturnType> {

    private final IndicesService indicesService;
    private IndexService indexService;

    public TableLevelExpression(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public ExpressionScope getScope() {
        return ExpressionScope.TABLE;
    }

    @Override
    public void applyScope(String nodeId, String indexName, int shardId) {
        indexService = indicesService.indexService(indexName);
    }
}
