package io.crate.planner.node;


import io.crate.planner.projection.Projection;
import io.crate.types.DataType;

import java.util.List;

public interface PlanNode {

    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context);

    void addProjection(Projection projection);

    List<DataType> outputTypes();
    void outputTypes(List<DataType> outputTypes);
}
