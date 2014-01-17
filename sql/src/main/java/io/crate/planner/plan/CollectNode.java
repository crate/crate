package io.crate.planner.plan;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class CollectNode extends PlanNode {

    private Routing routing;

    public CollectNode() {
        super();
    }

    public CollectNode(String id, Routing routing) {
        super(id);
        this.routing = routing;
    }

    public Routing routing() {
        return routing;
    }

    public boolean isRouted() {
        return routing != null && routing.hasLocations();
    }


    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitCollect(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            routing = new Routing();
            routing.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (routing != null) {
            out.writeBoolean(true);
            routing.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }
}
