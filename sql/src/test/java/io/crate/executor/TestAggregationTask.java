package io.crate.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.task.LocalAggregationTask;
import io.crate.metadata.Functions;
import io.crate.planner.plan.AggregationNode;

import java.util.List;

public class TestAggregationTask extends LocalAggregationTask {

    private final SettableFuture<Object[][]> result = SettableFuture.create();
    List<ListenableFuture<Object[][]>> results = ImmutableList.of((ListenableFuture<Object[][]>) result);

    public TestAggregationTask(AggregationNode node, Functions functions) {
        super(node, functions);
    }

    @Override
    public void start() {
        startCollect();
        for (final ListenableFuture<Object[][]> f : upstreamResults) {
            f.addListener(new Runnable() {

                @Override
                public void run() {
                    Object[][] value = null;
                    try {
                        value = f.get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (java.util.concurrent.ExecutionException e) {
                        e.printStackTrace();
                    }
                    processUpstreamResult(value);
                }
            }, MoreExecutors.sameThreadExecutor());
        }
        result.set(finishCollect());
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }


}
