package io.crate.operation.collect.sources;

import io.crate.operation.RowDownstream;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.planner.node.dql.CollectPhase;

import java.util.Collection;

public interface CollectSource {

    Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowDownstream downstream, JobCollectContext jobCollectContext);
}
