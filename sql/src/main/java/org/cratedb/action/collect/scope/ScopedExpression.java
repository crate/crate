package org.cratedb.action.collect.scope;


import org.cratedb.action.collect.Expression;

public interface ScopedExpression<ReturnType> extends Expression<ReturnType> {
    /**
     * Get the {@link org.cratedb.action.collect.scope.ExpressionScope} this expression has to be executed in.
     */
    public ExpressionScope getScope();

    public String getFullyQualifiedName();
}
