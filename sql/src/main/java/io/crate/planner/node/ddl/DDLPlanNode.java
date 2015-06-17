package io.crate.planner.node.ddl;

import com.google.common.collect.ImmutableList;
import io.crate.planner.node.PlanNode;
import io.crate.types.DataType;
import io.crate.types.LongType;

import java.util.List;

public abstract class DDLPlanNode implements PlanNode {

    // this is for the number of affected rows
    private final List<DataType> outputTypes = ImmutableList.<DataType>of(LongType.INSTANCE);

    @Override
    public List<DataType> outputTypes() {
        return outputTypes;
    }
}
