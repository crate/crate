package io.crate.operator.operations.collect;

import io.crate.operator.collector.CrateCollector;
import io.crate.operator.projectors.Projector;
import io.crate.planner.node.dql.CollectNode;

public interface CollectService {

    public CrateCollector getCollector(CollectNode node, Projector projector);
}
