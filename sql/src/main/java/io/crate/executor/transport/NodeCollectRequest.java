package io.crate.executor.transport;

import io.crate.planner.plan.CollectNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class NodeCollectRequest extends TransportRequest {

    private CollectNode collectNode;

    public NodeCollectRequest() {
    }

    public NodeCollectRequest(CollectNode collectNode) {
        this.collectNode = collectNode;
    }

    public CollectNode collectNode() {
        return collectNode;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        collectNode = new CollectNode();
        collectNode.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        collectNode.writeTo(out);
    }
}
