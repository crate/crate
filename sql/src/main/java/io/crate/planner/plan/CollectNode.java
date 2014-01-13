package io.crate.planner.plan;

import io.crate.planner.symbol.Symbol;

public class CollectNode extends PlanNode {

    public CollectNode(String id) {
        super(id);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitCollect(this, context);
    }


}
