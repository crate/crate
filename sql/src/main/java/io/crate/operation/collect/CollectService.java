package io.crate.operation.collect;

import io.crate.operation.projectors.Projector;
import io.crate.planner.node.dql.QueryAndFetchNode;

public interface CollectService {

    public CrateCollector getCollector(QueryAndFetchNode node, Projector projector);
}
