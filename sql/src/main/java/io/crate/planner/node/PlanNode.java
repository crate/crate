package io.crate.planner.node;

public interface PlanNode {

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context);
}
