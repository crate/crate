package io.crate.planner.plan;

// PRESTOBORROW

import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.ValueSymbol;

public class AggregationNode extends PlanNode {

    public AggregationNode(String id) {
        super(id);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitAggregation(this, context);
    }

}
