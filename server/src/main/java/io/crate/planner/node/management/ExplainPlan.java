/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.node.management;

import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.RowConsumerToResultReceiver;
import io.crate.analyze.BoundCopyFrom;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.MapBuilder;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.ExecutionPhase;
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
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.PrintContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.statement.CopyFromPlan;
import io.crate.profile.ProfilingContext;
import io.crate.profile.Timer;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
    @Nullable
    private final ProfilingContext context;

    public ExplainPlan(Plan subExecutionPlan, @Nullable ProfilingContext context) {
        this.subPlan = subExecutionPlan;
        this.context = context;
    }

    public Plan subPlan() {
        return subPlan;
    }

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        if (context != null) {
            assert subPlan instanceof LogicalPlan : "subPlan must be a LogicalPlan";
            LogicalPlan plan = (LogicalPlan) subPlan;
            /**
             * EXPLAIN ANALYZE does not support analyzing {@link io.crate.planner.MultiPhasePlan}s
             */
            if (plan.dependencies().isEmpty()) {
                UUID jobId = plannerContext.jobId();
                BaseResultReceiver resultReceiver = new BaseResultReceiver();
                RowConsumer noopRowConsumer = new RowConsumerToResultReceiver(resultReceiver, 0, t -> {});

                Timer timer = context.createTimer(Phase.Execute.name());
                timer.start();

                NodeOperationTree operationTree = LogicalPlanner.getNodeOperationTree(
                    plan, dependencies, plannerContext, params, subQueryResults);

                resultReceiver.completionFuture()
                    .whenComplete(createResultConsumer(dependencies, consumer, jobId, timer, operationTree));

                LogicalPlanner.executeNodeOpTree(
                    dependencies,
                    plannerContext.transactionContext(),
                    jobId,
                    noopRowConsumer,
                    true,
                    operationTree
                );
            } else {
                consumer.accept(null,
                    new UnsupportedOperationException("EXPLAIN ANALYZE does not support profiling multi-phase plans, " +
                                                      "such as queries with scalar subselects."));
            }
        } else {
            if (subPlan instanceof LogicalPlan) {
                PrintContext printContext = new PrintContext();
                ((LogicalPlan) subPlan).print(printContext);
                consumer.accept(InMemoryBatchIterator.of(new Row1(printContext.toString()), SENTINEL), null);
            } else if (subPlan instanceof CopyFromPlan) {
                BoundCopyFrom boundCopyFrom = CopyFromPlan.bind(
                    ((CopyFromPlan) subPlan).copyFrom(),
                    plannerContext.transactionContext(),
                    plannerContext.nodeContext(),
                    params,
                    subQueryResults);
                ExecutionPlan executionPlan = CopyFromPlan.planCopyFromExecution(
                    ((CopyFromPlan) subPlan).copyFrom(),
                    boundCopyFrom,
                    dependencies.clusterService().state().nodes(),
                    plannerContext,
                    params,
                    subQueryResults);
                String planAsJson = DataTypes.STRING.implicitCast(PlanPrinter.objectMap(executionPlan));
                consumer.accept(InMemoryBatchIterator.of(new Row1(planAsJson), SENTINEL), null);
            } else {
                consumer.accept(InMemoryBatchIterator.of(
                    new Row1("EXPLAIN not supported for " + subPlan.getClass().getSimpleName()), SENTINEL), null);
            }
        }
    }

    private BiConsumer<Void, Throwable> createResultConsumer(DependencyCarrier executor,
                                                             RowConsumer consumer,
                                                             UUID jobId,
                                                             Timer timer,
                                                             NodeOperationTree operationTree) {
        assert context != null : "profilingContext must be available if createResultconsumer is used";
        return (ignored, t) -> {
            context.stopTimerAndStoreDuration(timer);
            if (t == null) {
                OneRowActionListener<Map<String, Map<String, Object>>> actionListener =
                    new OneRowActionListener<>(consumer,
                        resp -> buildResponse(context.getDurationInMSByTimer(), resp, operationTree));
                collectTimingResults(jobId, executor, operationTree.nodeOperations())
                    .whenComplete(actionListener);
            } else {
                consumer.accept(null, t);
            }
        };
    }

    private TransportCollectProfileOperation getRemoteCollectOperation(DependencyCarrier executor, UUID jobId) {
        TransportCollectProfileNodeAction nodeAction = executor.transportActionProvider()
            .transportCollectProfileNodeAction();
        return new TransportCollectProfileOperation(nodeAction, jobId);
    }

    private Row buildResponse(Map<String, Object> apeTimings,
                              Map<String, Map<String, Object>> timingsByNodeId,
                              NodeOperationTree operationTree) {
        MapBuilder<String, Object> mapBuilder = MapBuilder.newMapBuilder();
        apeTimings.forEach(mapBuilder::put);

        // Each node collects the timings for each phase it executes. We want to extract the phases from each node
        // under a dedicated "Phases" key so it's easier for the user to follow the execution.
        // So we'll transform the response from what the nodes send which looks like this:
        //
        // "Execute": {
        //      "nodeId1": {"0-collect": 23, "2-fetchPhase": 334, "QueryBreakDown": {...}}
        //      "nodeId2": {"0-collect": 12, "2-fetchPhase": 222, "QueryBreakDown": {...}}
        //  }
        //
        // To:
        // "Execute": {
        //      "Phases": {
        //         "0-collect": {
        //              "nodes": {"nodeId1": 23, "nodeId2": 12}
        //          },
        //         "2-fetchPhase": {
        //              "nodes": {"nodeId1": 334, "nodeId2": 222}
        //          }
        //      }
        //      "nodeId1": {"QueryBreakDown": {...}}
        //      "nodeId2": {"QueryBreakDown": {...}}
        //  }

        Map<String, Object> phasesTimings = extractPhasesTimingsFrom(timingsByNodeId, operationTree);
        Map<String, Map<String, Object>> resultNodeTimings = getNodeTimingsWithoutPhases(phasesTimings.keySet(), timingsByNodeId);
        MapBuilder<String, Object> executionTimingsMap = MapBuilder.newMapBuilder();
        executionTimingsMap.put("Phases", phasesTimings);
        resultNodeTimings.forEach(executionTimingsMap::put);
        executionTimingsMap.put("Total", apeTimings.get(Phase.Execute.name()));

        mapBuilder.put(Phase.Execute.name(), executionTimingsMap.immutableMap());
        return new Row1(mapBuilder.immutableMap());
    }

    private static Map<String, Object> extractPhasesTimingsFrom(Map<String, Map<String, Object>> timingsByNodeId,
                                                                NodeOperationTree operationTree) {
        Map<String, Object> allPhases = new TreeMap<>();
        for (NodeOperation operation : operationTree.nodeOperations()) {
            ExecutionPhase phase = operation.executionPhase();
            getPhaseTimingsAndAddThemToPhasesMap(phase, timingsByNodeId, allPhases);
        }

        ExecutionPhase leafExecutionPhase = operationTree.leaf();
        getPhaseTimingsAndAddThemToPhasesMap(leafExecutionPhase, timingsByNodeId, allPhases);

        return allPhases;
    }

    private static void getPhaseTimingsAndAddThemToPhasesMap(ExecutionPhase leafExecutionPhase,
                                                             Map<String, Map<String, Object>> timingsByNodeId,
                                                             Map<String, Object> allPhases) {
        String phaseName = ProfilingContext.generateProfilingKey(leafExecutionPhase.phaseId(), leafExecutionPhase.name());
        Map<String, Object> phaseTimingsAcrossNodes = getPhaseTimingsAcrossNodes(phaseName, timingsByNodeId);

        if (!phaseTimingsAcrossNodes.isEmpty()) {
            allPhases.put(phaseName, Map.of("nodes", phaseTimingsAcrossNodes));
        }
    }

    private static Map<String, Object> getPhaseTimingsAcrossNodes(String phaseName,
                                                                  Map<String, Map<String, Object>> timingsByNodeId) {
        Map<String, Object> timingsForPhaseAcrossNodes = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> nodeToTimingsEntry : timingsByNodeId.entrySet()) {
            Map<String, Object> timingsForNode = nodeToTimingsEntry.getValue();
            if (timingsForNode != null) {
                Object phaseTiming = timingsForNode.get(phaseName);
                if (phaseTiming != null) {
                    String node = nodeToTimingsEntry.getKey();
                    timingsForPhaseAcrossNodes.put(node, phaseTiming);
                }
            }
        }

        return Collections.unmodifiableMap(timingsForPhaseAcrossNodes);
    }

    private static Map<String, Map<String, Object>> getNodeTimingsWithoutPhases(Set<String> phasesNames,
                                                                                Map<String, Map<String, Object>> timingsByNodeId) {
        Map<String, Map<String, Object>> nodeTimingsWithoutPhases = new HashMap<>(timingsByNodeId.size());
        for (Map.Entry<String, Map<String, Object>> nodeToTimingsEntry : timingsByNodeId.entrySet()) {
            nodeTimingsWithoutPhases.put(nodeToTimingsEntry.getKey(), new HashMap<>(nodeToTimingsEntry.getValue()));
        }

        for (Map<String, Object> timings : nodeTimingsWithoutPhases.values()) {
            for (String phaseToRemove : phasesNames) {
                timings.remove(phaseToRemove);
            }
        }

        return Collections.unmodifiableMap(nodeTimingsWithoutPhases);
    }

    private CompletableFuture<Map<String, Map<String, Object>>> collectTimingResults(UUID jobId,
                                                                                     DependencyCarrier executor,
                                                                                     Collection<NodeOperation> nodeOperations) {
        Set<String> nodeIds = NodeOperationGrouper.groupByServer(nodeOperations).keySet();

        CompletableFuture<Map<String, Map<String, Object>>> resultFuture = new CompletableFuture<>();
        TransportCollectProfileOperation remoteCollectOperation = getRemoteCollectOperation(executor, jobId);

        ConcurrentHashMap<String, Map<String, Object>> timingsByNodeId = new ConcurrentHashMap<>(nodeIds.size());
        boolean needsCollectLocal = !nodeIds.contains(executor.localNodeId());

        AtomicInteger remainingCollectOps = new AtomicInteger(nodeIds.size());
        if (needsCollectLocal) {
            remainingCollectOps.incrementAndGet();
        }

        for (String nodeId : nodeIds) {
            remoteCollectOperation.collect(nodeId)
                .whenComplete(mergeResultsAndCompleteFuture(resultFuture, timingsByNodeId, remainingCollectOps, nodeId));
        }

        if (needsCollectLocal) {
            executor
                .transportActionProvider()
                .transportCollectProfileNodeAction()
                .collectExecutionTimesAndFinishContext(jobId)
                .whenComplete(mergeResultsAndCompleteFuture(resultFuture, timingsByNodeId, remainingCollectOps, executor.localNodeId()));
        }
        return resultFuture;
    }

    private static BiConsumer<Map<String, Object>, Throwable> mergeResultsAndCompleteFuture(CompletableFuture<Map<String, Map<String, Object>>> resultFuture,
                                                                                            ConcurrentHashMap<String, Map<String, Object>> timingsByNodeId,
                                                                                            AtomicInteger remainingOperations,
                                                                                            String nodeId) {
        return (map, throwable) -> {
            if (throwable == null) {
                timingsByNodeId.put(nodeId, map);
                if (remainingOperations.decrementAndGet() == 0) {
                    resultFuture.complete(timingsByNodeId);
                }
            } else {
                resultFuture.completeExceptionally(throwable);
            }
        };
    }

    @VisibleForTesting
    public boolean doAnalyze() {
        return context != null;
    }
}
