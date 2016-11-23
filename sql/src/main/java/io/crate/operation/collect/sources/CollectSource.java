package io.crate.operation.collect.sources;

import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.projectors.RowReceiver;

import java.util.Collection;

public interface CollectSource {

    Collection<CrateCollector> getCollectors(RowReceiver downstream, JobCollectContext jobCollectContext);
}
