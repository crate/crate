package org.cratedb.action.collect.scope;


import org.cratedb.action.collect.Expression;

public interface ScopedExpression<ReturnType> extends Expression<ReturnType> {

    /**
     * Get the {@link org.cratedb.action.collect.scope.ExpressionScope} this expression has to be executed in.
     */
    public ExpressionScope getScope();

    /**
     * Apply the given scope to this expression as needed.
     * For example shard scoped expressions would use this information
     * to go and get the appropriate IndexShard for extracting their information
     *
     * @param nodeId
     * @param indexName
     * @param shardId
     */
    public void applyScope(String nodeId, String indexName, int shardId);

    public String name();
}
