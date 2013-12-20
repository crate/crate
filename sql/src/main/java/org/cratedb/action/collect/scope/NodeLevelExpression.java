package org.cratedb.action.collect.scope;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.node.service.NodeService;

public abstract class NodeLevelExpression<ReturnType> implements ScopedExpression<ReturnType> {

    private final ClusterService clusterService;
    private final NodeService nodeService;
    private DiscoveryNode node;

    public NodeLevelExpression(ClusterService clusterService, NodeService nodeService) {
        this.clusterService = clusterService;
        this.nodeService = nodeService;
    }

    @Override
    public ExpressionScope getScope() {
        return ExpressionScope.NODE;
    }

    @Override
    public void applyScope(String nodeId, String indexName, int shardId) {
        this.node = clusterService.state().nodes().get(nodeId);
    }
}
