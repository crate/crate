package org.cratedb.action.collect.scope;

public enum ExpressionScope {

    /**
     * Expressions with Cluster Scope depend only on the cluster and its state.
     * They can be evaluated anywhere in the cluster.
     */
    CLUSTER("cluster"),

    /**
     * Expressions with Node Scope depend on the node they are evaluated on.
     * They yield different values for different nodes.
     */
    NODE("node"),

    /**
     * Index scoped Expressions depend are scoped to a single index and
     * can only be executed on nodes that have shards of that index or on any shard of that index.
     */
    INDEX("index"),

    /**
     * Expressions scoped to a single shard must be executed on shard-level.
     * E.g. getting a field value from an index is scoped to shard-level.
     */
    SHARD("shard");

    private String tableName;

    private ExpressionScope(String tableName) {
        this.tableName = tableName;
    }

    public String tableName() {
        return tableName;
    }
}
