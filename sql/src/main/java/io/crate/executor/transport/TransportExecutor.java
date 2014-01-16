package io.crate.executor.transport;

import io.crate.executor.Executor;
import io.crate.executor.Job;
import io.crate.executor.Task;
import io.crate.planner.plan.PlanNode;

import java.util.List;

public class TransportExecutor implements Executor {

    public TransportExecutor() {
    }

    @Override
    public Job newJob(PlanNode node) {
        return null;
    }

    @Override
    public List execute(Job job) {
        assert job.tasks().size() > 0;

        Task lastTask = null;
        for (Task task : job.tasks()) {
            task.start();
            lastTask = task;
        }

        assert lastTask != null;
        return lastTask.result();
    }
}
