package io.crate.executor;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.planner.plan.PlanNode;

import java.util.List;

public interface Executor {

    public Job newJob(PlanNode node);

    public List<ListenableFuture<Object[][]>> execute(Job job);

}
