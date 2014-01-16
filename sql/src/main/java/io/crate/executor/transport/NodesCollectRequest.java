package io.crate.executor.transport;

import io.crate.planner.plan.CollectNode;
import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodesCollectRequest extends NodesOperationRequest<NodesCollectRequest> {

    private CollectNode collectNode;

    public NodesCollectRequest() {

    }

    public NodesCollectRequest(CollectNode collectNode, String... nodesIds) {
        super(nodesIds);
        this.collectNode = collectNode;
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        // collectNode.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        // collectNode = new CollectNode(null);
        // collectNode.readFrom(in);
    }

}
