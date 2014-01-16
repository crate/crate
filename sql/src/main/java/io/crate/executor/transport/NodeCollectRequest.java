package io.crate.executor.transport;

import io.crate.planner.plan.CollectNode;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeCollectRequest extends NodeOperationRequest {

    private CollectNode collectNode;

    public NodeCollectRequest() {
    }

    public NodeCollectRequest(NodesCollectRequest request, String nodeId) {
        super(request, nodeId);
        this.collectNode = request.collectNode();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        // collectNode = new CollectNode(null);
        // collectNode.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        // collectNode.writeTo(out);
    }
}
