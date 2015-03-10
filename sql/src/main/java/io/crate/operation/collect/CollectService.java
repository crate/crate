package io.crate.operation.collect;

import io.crate.operation.RowDownstream;
import io.crate.planner.node.dql.CollectNode;

public interface CollectService {

    public CrateCollector getCollector(CollectNode node, RowDownstream downstream);
}
