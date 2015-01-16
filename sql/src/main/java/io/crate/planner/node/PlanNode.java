package io.crate.planner.node;


import io.crate.types.DataType;

import java.util.List;

public interface PlanNode {

    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context);

    List<DataType> outputTypes();
    void outputTypes(List<DataType> outputTypes);
}
