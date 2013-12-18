package org.cratedb.action.collect.scope;

public abstract class ClusterLevelExpression<ReturnType> implements ScopedExpression<ReturnType> {

    @Override
    public void putInScope(String nodeId, String indexName, int shardId) {

    }

    @Override
    public ExpressionScope getScope() {
        return ExpressionScope.CLUSTER;
    }
}
