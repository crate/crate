package io.crate.planner.node.dql;

import io.crate.planner.node.PlanNode;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;

import java.util.List;
import java.util.Set;

public interface DQLPlanNode extends PlanNode {

    boolean hasProjections();
    List<Projection> projections();
    void addProjection(Projection projection);

    Set<String> executionNodes();

    void inputTypes(List<DataType> dataTypes);
    List<DataType> inputTypes();
}
