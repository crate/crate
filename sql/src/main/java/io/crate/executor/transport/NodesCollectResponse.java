package io.crate.executor.transport;

import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.cluster.ClusterName;

public class NodesCollectResponse extends NodesOperationResponse<NodeCollectResponse> {

    public NodesCollectResponse(ClusterName clusterName, NodeCollectResponse[] nodes) {
        super(clusterName, nodes);
    }
}
