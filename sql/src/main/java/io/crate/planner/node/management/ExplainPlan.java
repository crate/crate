/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.node.management;

import com.google.common.annotations.VisibleForTesting;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.RowConsumerToResultReceiver;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.NodeOperationGrouper;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.engine.profile.TransportCollectProfileNodeAction;
import io.crate.execution.engine.profile.TransportCollectProfileOperation;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.ExplainLogicalPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.statement.CopyStatementPlanner;
import io.crate.profile.ProfilingContext;
import io.crate.profile.ProfilingResult;
import io.crate.profile.TimeMeasurable;
import org.elasticsearch.common.collect.MapBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static io.crate.data.SentinelRow.SENTINEL;

public class ExplainPlan implements Plan {

    public enum Phase {
        Analyze,
        Plan,
        Execute
    }

    private final Plan subPlan;
    private final ProfilingContext context;

    public ExplainPlan(Plan subExecutionPlan, ProfilingContext context) {
        this.subPlan = subExecutionPlan;
        this.context = context;
    }

    public Plan subPlan() {
        return subPlan;
    }

    @Override
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        SubQueryResults subQueryResults) {
        if (context.enabled()) {
            assert subPlan instanceof LogicalPlan : "subPlan must be a LogicalPlan";
            LogicalPlan plan = (LogicalPlan) subPlan;
            /**
             * EXPLAIN ANALYZE does not support analyzing {@link io.crate.planner.MultiPhasePlan}s
             */
            if (plan.dependencies().isEmpty()) {
                UUID jobId = plannerContext.jobId();
                BaseResultReceiver resultReceiver = new BaseResultReceiver();
                RowConsumer noopRowConsumer = new RowConsumerToResultReceiver(resultReceiver, 0);

                TimeMeasurable timeMeasurable = context.createMeasurable(Phase.Execute.name());
                timeMeasurable.start();

                NodeOperationTree operationTree = LogicalPlanner.getNodeOperationTree(
                    plan, executor, plannerContext, params, subQueryResults);

                resultReceiver.completionFuture()
                    .whenComplete(createResultConsumer(executor, consumer, jobId, timeMeasurable, operationTree));

                LogicalPlanner.executeNodeOpTree(executor, jobId, noopRowConsumer, context.enabled(), operationTree);
            } else {
                consumer.accept(null,
                    new UnsupportedOperationException("EXPLAIN ANALYZE does not support profiling multi-phase plans, " +
                                                      "such as queries with scalar subselects."));
            }
        } else {
            try {
                Map<String, Object> map;
                if (subPlan instanceof LogicalPlan) {
                    map = ExplainLogicalPlan.explainMap((LogicalPlan) subPlan, plannerContext, executor.projectionBuilder());
                } else if (subPlan instanceof CopyStatementPlanner.CopyFrom) {
                    ExecutionPlan executionPlan = CopyStatementPlanner.planCopyFromExecution(
                        executor.clusterService().state().nodes(),
                        ((CopyStatementPlanner.CopyFrom) subPlan).copyFrom,
                        plannerContext
                    );
                    map = PlanPrinter.objectMap(executionPlan);
                } else {
                    consumer.accept(null, new UnsupportedOperationException("EXPLAIN not supported for " + subPlan));
                    return;
                }
                consumer.accept(InMemoryBatchIterator.of(new Row1(map), SENTINEL), null);
            } catch (Throwable t) {
                consumer.accept(null, t);
            }
        }
    }

    private BiConsumer<Void, Throwable> createResultConsumer(DependencyCarrier executor,
                                                             RowConsumer consumer,
                                                             UUID jobId,
                                                             TimeMeasurable timeMeasurable,
                                                             NodeOperationTree operationTree) {
        return (ignored, t) -> {
            context.stopAndAddMeasurable(timeMeasurable);
            if (t == null) {
                OneRowActionListener<List<ProfilingResult>> actionListener =
                    new OneRowActionListener<>(consumer, resp -> buildResponse(context.getResults(), resp));
                collectTimingResults(jobId, executor, operationTree.nodeOperations())
                    .whenComplete(actionListener);
            } else {
                consumer.accept(null, t);
            }
        };
    }

    private TransportCollectProfileOperation getTransportCollectProfileOperation(DependencyCarrier executor, UUID jobId) {
        TransportCollectProfileNodeAction nodeAction = executor.transportActionProvider()
            .transportCollectProfileNodeAction();
        return new TransportCollectProfileOperation(nodeAction, jobId);
    }

    private Row buildResponse(Collection<ProfilingResult> apeTimings, List<ProfilingResult> nodeResults) {
        MapBuilder<String, Object> mapBuilder = MapBuilder.newMapBuilder();
        // apeTimings.forEach(mapBuilder::put);

        MapBuilder<String, Object> executionTimingsMap = MapBuilder.newMapBuilder();
        // nodeResults.forEach(executionTimingsMap::put);
        // executionTimingsMap.put("Total", apeTimings.get(Phase.Execute.name()));

        mapBuilder.put(Phase.Execute.name(), executionTimingsMap.immutableMap());
        return new Row1(mapBuilder.immutableMap());
    }

    private CompletableFuture<List<ProfilingResult>> collectTimingResults(UUID jobId,
                                                                    DependencyCarrier executor,
                                                                    Collection<NodeOperation> nodeOperations) {
        Set<String> nodeIds = NodeOperationGrouper.groupByServer(nodeOperations).keySet();

        if (nodeIds.size() > 0) {
            CompletableFuture<List<ProfilingResult>> resultFuture = new CompletableFuture<>();
            TransportCollectProfileOperation collectProfileOperation = getTransportCollectProfileOperation(executor, jobId);

            List<ProfilingResult> results = Collections.synchronizedList(new ArrayList<>(nodeIds.size()));
            AtomicInteger counter = new AtomicInteger(nodeIds.size());

            for (String nodeId : nodeIds) {
                collectProfileOperation.collect(nodeId)
                    .whenComplete((profilingResult, throwable) -> {
                        if (throwable == null) {
                            results.add(profilingResult);
                            if (counter.decrementAndGet() == 0) {
                                resultFuture.complete(results);
                            }
                        } else {
                            resultFuture.completeExceptionally(throwable);
                        }
                    });
            }
            return resultFuture;
        } else {
            // Collect from local JobExecutionContext
            return executor
                .transportActionProvider()
                .transportCollectProfileNodeAction()
                .collectExecutionTimesAndFinishContext(jobId)
                .thenApply(Collections::singletonList);
        }
    }

    @VisibleForTesting
    public boolean doAnalyze() {
        return context.enabled();
    }
}
