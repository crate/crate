package io.crate.executor.transport;

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;

public class NodeCollectResponse extends NodeOperationResponse {

    public NodeCollectResponse(DiscoveryNode node) {
        super(node);
    }

    public Object[][] value() {
        return null;
    }
}
