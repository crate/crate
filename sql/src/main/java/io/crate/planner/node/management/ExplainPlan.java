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
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.NodeOperationGrouper;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.engine.profile.TransportCollectProfileNodeAction;
import io.crate.execution.engine.profile.TransportCollectProfileOperation;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.profile.TimerToken;
import io.crate.profile.ProfilingContext;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Plan;
import io.crate.planner.PlanPrinter;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.ExplainLogicalPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.statement.CopyStatementPlanner;
import org.elasticsearch.common.collect.MapBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
                        Map<SelectSymbol, Object> valuesBySubQuery) {

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

                TimerToken timerToken = context.createTimer(Phase.Execute.name());
                timerToken.start();

                NodeOperationTree operationTree = LogicalPlanner.getNodeOperationTree(
                    plan, executor, plannerContext, params, valuesBySubQuery);

                resultReceiver.completionFuture()
                    .whenComplete(createResultConsumer(executor, consumer, jobId, timerToken, operationTree));

                LogicalPlanner.executeNodeOpTree(executor, jobId, noopRowConsumer, context.enabled(), operationTree);
            } else {
                consumer.accept(null, new UnsupportedOperationException("EXPLAIN ANALYZE not supported for " + plan));
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

    private BiConsumer<Object, Throwable> createResultConsumer(DependencyCarrier executor,
                                                               RowConsumer consumer,
                                                               UUID jobId,
                                                               TimerToken timerToken,
                                                               NodeOperationTree operationTree) {
        return (ignored, t) -> {
            context.stopAndAddTimer(timerToken);
            if (t == null) {
                OneRowActionListener<Map<String, Map<String, Long>>> actionListener =
                    new OneRowActionListener<>(consumer, resp -> buildResponse(context.getAsMap(), resp));
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

    private Row buildResponse(Map<String, Long> apeTimings, Map<String, Map<String, Long>> nodeTimings) {
        MapBuilder<String, Object> mapBuilder = MapBuilder.newMapBuilder();
        apeTimings.forEach(mapBuilder::put);

        MapBuilder<String, Object> executionTimingsMap = MapBuilder.newMapBuilder();
        nodeTimings.forEach(executionTimingsMap::put);
        executionTimingsMap.put("Total", apeTimings.get(Phase.Execute.name()));

        mapBuilder.put(Phase.Execute.name(), executionTimingsMap.immutableMap());
        return new Row1(mapBuilder.immutableMap());
    }

    private CompletableFuture<Map<String, Map<String, Long>>> collectTimingResults(UUID jobId,
                                                                                   DependencyCarrier executor,
                                                                                   Collection<NodeOperation> nodeOperations) {
        CompletableFuture<Map<String, Map<String, Long>>> resultFuture = new CompletableFuture<>();
        Set<String> nodeIds = NodeOperationGrouper.groupByServer(nodeOperations).keySet();
        TransportCollectProfileOperation collectProfileOperation = getTransportCollectProfileOperation(executor, jobId);

        ConcurrentHashMap<String, Map<String, Long>> mergedMap = new ConcurrentHashMap<>(nodeIds.size());
        AtomicInteger counter = new AtomicInteger(nodeIds.size());

        for (String nodeId : nodeIds) {
            collectProfileOperation.collect(nodeId)
                .whenComplete((map, throwable) -> {
                    if (throwable == null) {
                        mergedMap.put(nodeId, map);
                        if (counter.decrementAndGet() == 0) {
                            resultFuture.complete(mergedMap);
                        }
                    } else {
                        resultFuture.completeExceptionally(throwable);
                    }
                });
        }
        return resultFuture;
    }

    @VisibleForTesting
    public boolean doAnalyze() {
        return context.enabled();
    }
}
