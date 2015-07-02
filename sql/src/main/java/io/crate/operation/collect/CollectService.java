package io.crate.operation.collect;

import io.crate.operation.RowDownstream;
import io.crate.planner.node.dql.CollectPhase;

public interface CollectService {

    public CrateCollector getCollector(CollectPhase node, RowDownstream downstream);
}
