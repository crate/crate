package io.crate.planner.node;

import org.cratedb.DataType;

import java.util.List;

public interface PlanNode {

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context);

    List<DataType> outputTypes();
    void outputTypes(List<DataType> outputTypes);
}
