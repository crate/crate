package org.cratedb.action.parser.context;

import org.cratedb.action.collect.scope.ExpressionScope;
import org.elasticsearch.common.Nullable;

/**
 * Context for parsing
 *
 * Contract: if scope denotes that we are e.g. on a shard, then shard information must be available,
 * means not null.
 */
public class ParseContext {

    private final String nodeId;
    private final String indexName;
    private final Integer shardId;
    private final ExpressionScope scope;

    public ParseContext() {
        this(null, null, null);
    }


    public ParseContext(@Nullable String nodeId, @Nullable String indexName, @Nullable Integer shardId) {
        this.nodeId = nodeId;
        this.indexName = indexName;
        this.shardId = shardId;
        if (nodeId==null) {
            this.scope = ExpressionScope.CLUSTER;
        } else if (indexName == null) {
            this.scope = ExpressionScope.NODE;
        } else if (shardId == null) {
            this.scope = ExpressionScope.TABLE;
        } else {
            this.scope = ExpressionScope.SHARD;
        }
    }

    public @Nullable String getNodeId() {
        return nodeId;
    }

    public @Nullable String indexName() {
        return indexName;
    }

    public @Nullable Integer shardId() {
        return shardId;
    }

    public ExpressionScope getScope() {
        return this.scope;
    }

    /**
     * returns true if current context is on handler node
     */
    public boolean onHandler() {
        return false;
    }
}
