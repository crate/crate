package org.cratedb.action.collect.scope;

public abstract class NodeLevelExpression<ReturnType> implements ScopedExpression<ReturnType> {
    @Override
    public ExpressionScope getScope() {
        return ExpressionScope.NODE;
    }
}
