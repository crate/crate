package io.crate.planner.node;


import io.crate.types.DataType;

import java.util.List;

public interface PlanNode {

    <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context);

    List<DataType> outputTypes();
}
