package io.crate.planner.plan;

// PRESTOBORROW

public class TopNNode extends PlanNode {

    public TopNNode(String id) {
        super(id);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitTopNNode(this, context);
    }

}
