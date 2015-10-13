package io.crate.planner.node.dql;

import io.crate.planner.node.PlanNode;
import io.crate.planner.projection.Projection;

import java.util.Collection;
import java.util.List;

public interface DQLPlanNode extends PlanNode {

    boolean hasProjections();
    List<Projection> projections();
    void addProjection(Projection projection);

    Collection<String> executionNodes();
}
