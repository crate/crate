package io.crate.planner.node;


public interface PlanNode {

    <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context);

}
