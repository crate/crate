package org.cratedb.action.collect.scope;

public abstract class IndexLevelExpression<ReturnType> implements ScopedExpression<ReturnType> {
    @Override
    public ExpressionScope getScope() {
        return ExpressionScope.INDEX;
    }
}
