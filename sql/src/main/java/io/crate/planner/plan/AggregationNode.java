package io.crate.planner.plan;

// PRESTOBORROW

public class AggregationNode extends PlanNode {

    public AggregationNode(String id) {
        super(id);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitAggregation(this, context);
    }

}
