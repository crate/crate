package io.crate.planner.node;


import io.crate.core.collections.nested.MutableNested;
import io.crate.types.DataType;

import java.util.List;

public interface PlanNode extends MutableNested<PlanNode> {

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context);

    List<DataType> outputTypes();
    void outputTypes(List<DataType> outputTypes);
}
