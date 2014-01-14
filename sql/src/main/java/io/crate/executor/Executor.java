package io.crate.executor;

import io.crate.planner.plan.PlanNode;

import java.util.List;

public interface Executor {

    public Job newJob(PlanNode node);

    public List execute(Job job);

}
