package org.cratedb.action.collect.scope;

public abstract class ShardLevelExpression<ReturnType> implements ScopedExpression<ReturnType> {
    @Override
    public ExpressionScope getScope() {
        return ExpressionScope.SHARD;
    }
}
