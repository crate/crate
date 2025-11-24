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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.jetbrains.annotations.Nullable;

import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.common.collections.MapBuilder;
import io.crate.common.concurrent.CompletableFutures;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingRowConsumer;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.data.SentinelRow;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.MultiPhaseExecutor;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.NodeOperationGrouper;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.engine.profile.CollectProfileNodeAction;
import io.crate.execution.engine.profile.CollectProfileRequest;
import io.crate.execution.engine.profile.NodeCollectProfileResponse;
import io.crate.execution.support.NodeRequest;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.management.ExplainPlan.Phase;
import io.crate.planner.node.management.ExplainPlan.SubQueryResultAndExplain;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;
import io.crate.profile.ProfilingContext;
import io.crate.profile.Timer;

public class ExplainProfilePlan implements Plan {

    private final ProfilingContext context;
    private final LogicalPlan plan;

    public ExplainProfilePlan(LogicalPlan plan, ProfilingContext context) {
        this.plan = plan;
        this.context = context;
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
                              SubQueryResults subQueryResults) throws Exception {
        var ramAccounting = ConcurrentRamAccounting.forCircuitBreaker(
            "multi-phase",
            dependencies.circuitBreaker(CircuitBreaker.QUERY),
            plannerContext.transactionContext().sessionSettings().memoryLimitInBytes()
        );
        consumer.completionFuture().whenComplete((_, _) -> ramAccounting.close());
        executePlan(
            plan,
            dependencies,
            plannerContext,
            consumer,
            params,
            subQueryResults,
            new IdentityHashMap<>(),
            new ArrayList<>(plan.dependencies().size()),
            ramAccounting,
            null
        );
    }

    private CompletableFuture<?> executePlan(LogicalPlan plan,
                                             DependencyCarrier executor,
                                             PlannerContext plannerContext,
                                             RowConsumer consumer,
                                             Row params,
                                             SubQueryResults subQueryResults,
                                             Map<SelectSymbol, Object> valuesBySubQuery,
                                             List<Map<String, Object>> explainResults,
                                             RamAccounting ramAccounting,
                                             @Nullable SelectSymbol selectSymbol) {
        boolean isTopLevel = selectSymbol == null;
        IdentityHashMap<SelectSymbol, Object> subPlanValueBySubQuery = new IdentityHashMap<>();
        ArrayList<Map<String, Object>> subPlansExplainResults = new ArrayList<>(plan.dependencies().size());
        List<CompletableFuture<?>> subPlansFutures = new ArrayList<>(plan.dependencies().size());

        for (Map.Entry<LogicalPlan, SelectSymbol> entry : plan.dependencies().entrySet()) {
            SelectSymbol subPlanSelectSymbol = entry.getValue();

            // Some optimizers may have created new sub plans which aren't optimized by itself yet.
            final LogicalPlan subPlan = plannerContext.optimize().apply(entry.getKey(), plannerContext);

            subPlansFutures.add(executePlan(
                subPlan,
                executor,
                PlannerContext.forSubPlan(plannerContext),
                consumer,
                params,
                subQueryResults,
                subPlanValueBySubQuery,
                subPlansExplainResults,
                ramAccounting,
                subPlanSelectSymbol
            ));
        }
        var subPlansFuture = CompletableFuture
            .allOf(subPlansFutures.toArray(new CompletableFuture[0]));

        if (isTopLevel) {
            return subPlansFuture.thenCompose(_ ->
                executeTopLevelPlan(
                    plan,
                    executor,
                    plannerContext,
                    consumer,
                    params,
                    SubQueryResults.merge(subQueryResults, new SubQueryResults(subPlanValueBySubQuery)),
                    subPlansExplainResults
                ));
        } else {
            final var ramAccountingFinal = ramAccounting;
            return subPlansFuture.thenCompose(_ ->
                executeSingleSubPlan(
                    plan,
                    selectSymbol,
                    executor,
                    plannerContext,
                    ramAccountingFinal,
                    params,
                    SubQueryResults.merge(subQueryResults, new SubQueryResults(subPlanValueBySubQuery)),
                    subPlansExplainResults
                ).thenCompose(subQueryResultAndExplain -> {
                    synchronized (valuesBySubQuery) {
                        valuesBySubQuery.put(selectSymbol, subQueryResultAndExplain.value());
                    }

                    synchronized (explainResults) {
                        explainResults.add(subQueryResultAndExplain.explainResult());
                    }
                    return CompletableFuture.completedFuture(null);
                }));
        }
    }

    private CompletableFuture<?> executeTopLevelPlan(LogicalPlan plan,
                                                     DependencyCarrier executor,
                                                     PlannerContext plannerContext,
                                                     RowConsumer consumer,
                                                     Row params,
                                                     SubQueryResults subQueryResults,
                                                     List<Map<String, Object>> subQueryExplainResults) {

        Timer timer = context.createTimer(Phase.Execute.name());
        timer.start();

        NodeOperationTree operationTree;
        try {
            operationTree = LogicalPlanner.getNodeOperationTree(plan, executor, plannerContext, params, subQueryResults);
        } catch (Throwable e) {
            consumer.accept(null, e);
            return consumer.completionFuture();
        }

        // Dummy collecting consumer.
        // The result is discarded. Instead fetch the profile timings from all nodes and output that
        CollectingRowConsumer<?, Long> discardingConsumer = new CollectingRowConsumer<>(Collectors.counting());
        discardingConsumer.completionFuture()
            .whenComplete((_, _) -> context.stopTimerAndStoreDuration(timer))
            .thenCompose(_ -> fetchTimingResults(plannerContext.jobId(), executor, operationTree.nodeOperations()))
            .whenComplete((timingResults, t) -> {
                if (t == null) {
                    Map<String, Object> response = buildResponse(
                        context.getDurationInMSByTimer(),
                        timingResults,
                        operationTree,
                        subQueryExplainResults,
                        false
                    );
                    BatchIterator<Row> it = InMemoryBatchIterator.of(new Row1(response), SentinelRow.SENTINEL);
                    consumer.accept(it, null);
                } else {
                    consumer.accept(null, t);
                }
            });

        LogicalPlanner.executeNodeOpTree(
            executor,
            plannerContext.transactionContext(),
            plannerContext.jobId(),
            discardingConsumer,
            true,
            operationTree
        );
        return consumer.completionFuture();
    }

    private CompletableFuture<SubQueryResultAndExplain> executeSingleSubPlan(LogicalPlan plan,
                                                                             SelectSymbol selectSymbol,
                                                                             DependencyCarrier executor,
                                                                             PlannerContext plannerContext,
                                                                             RamAccounting ramAccounting,
                                                                             Row params,
                                                                             SubQueryResults subQueryResults,
                                                                             List<Map<String, Object>> explainResults) {
        RowConsumer rowConsumer = MultiPhaseExecutor.getConsumer(selectSymbol, ramAccounting);

        var subPlanContext = new ProfilingContext(Map.of(), executor.clusterService().state());
        Timer subPlanTimer = subPlanContext.createTimer(Phase.Execute.name());
        subPlanTimer.start();

        NodeOperationTree operationTree = LogicalPlanner.getNodeOperationTree(
            plan, executor, plannerContext, params, subQueryResults);
        LogicalPlanner.executeNodeOpTree(
            executor,
            plannerContext.transactionContext(),
            plannerContext.jobId(),
            rowConsumer,
            true,
            operationTree
        );

        return rowConsumer.completionFuture()
            .thenCompose(val -> {
                subPlanContext.stopTimerAndStoreDuration(subPlanTimer);
                return fetchTimingResults(plannerContext.jobId(), executor, operationTree.nodeOperations())
                    .thenApply((timingResults) -> {
                        var explainOutput = buildResponse(
                            subPlanContext.getDurationInMSByTimer(),
                            timingResults,
                            operationTree,
                            explainResults,
                            true
                        );
                        return new SubQueryResultAndExplain(val, explainOutput);
                    });
            });
    }

    private CompletableFuture<Map<String, Map<String, Object>>> fetchTimingResults(UUID jobId,
                                                                                   DependencyCarrier dependencies,
                                                                                   Collection<NodeOperation> nodeOperations) {
        Set<String> uniqueNodeIds = new HashSet<>(NodeOperationGrouper.groupByServer(nodeOperations).keySet());
        uniqueNodeIds.add(dependencies.localNodeId());
        List<String> nodeIds = List.copyOf(uniqueNodeIds);
        Client client = dependencies.client();

        ArrayList<CompletableFuture<NodeCollectProfileResponse>> futures = new ArrayList<>(uniqueNodeIds.size());
        for (String nodeId : nodeIds) {
            NodeRequest<CollectProfileRequest> request = CollectProfileRequest.of(nodeId, jobId);
            futures.add(client.execute(CollectProfileNodeAction.INSTANCE, request));
        }

        return CompletableFutures.allAsList(futures).thenApply(responses -> {
            HashMap<String, Map<String, Object>> result = HashMap.newHashMap(responses.size());
            for (int i = 0; i < responses.size(); i++) {
                NodeCollectProfileResponse resp = responses.get(i);
                String nodeId = nodeIds.get(i);
                result.put(nodeId, resp.durationByContextIdent());
            }
            return result;
        });
    }

    private Map<String, Object> buildResponse(Map<String, Object> apeTimings,
                                              Map<String, Map<String, Object>> timingsByNodeId,
                                              NodeOperationTree operationTree,
                                              List<Map<String, Object>> subQueryExplainResults,
                                              boolean isSubQuery) {
        MapBuilder<String, Object> mapBuilder = MapBuilder.newMapBuilder();

        if (isSubQuery == false) {
            apeTimings.forEach(mapBuilder::put);
        }

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
        //
        // Additionally, execution timings for sub-queries are stored under a "Sub-Queries" key in the same structure as above.
        //
        // "Execute": {
        //     "Sub-Queries": {
        //          "Phases": {
        //              "0-collect": {
        //                  "nodes": {"nodeId1": 23, "nodeId2": 12}
        //              },
        //              ...
        //          "nodeId1": {"QueryBreakDown": {...}}
        //      }
        //      "Phases": {
        //         "1-collect": {
        //              "nodes": {"nodeId1": 23, "nodeId2": 12}
        //          },
        //          ...
        //      }
        //      "nodeId1": {"QueryBreakDown": {...}}
        //      "nodeId2": {"QueryBreakDown": {...}}
        //  }
        //

        Map<String, Object> phasesTimings = new TreeMap<>();
        for (NodeOperation operation : operationTree.nodeOperations()) {
            ExecutionPhase phase = operation.executionPhase();
            addPhaseTimings(phase, timingsByNodeId, phasesTimings);
        }
        ExecutionPhase leafExecutionPhase = operationTree.leaf();
        addPhaseTimings(leafExecutionPhase, timingsByNodeId, phasesTimings);

        Map<String, Map<String, Object>> nodeTimingsWithoutPhases = HashMap.newHashMap(timingsByNodeId.size());
        for (Map.Entry<String, Map<String, Object>> nodeToTimingsEntry : timingsByNodeId.entrySet()) {
            nodeTimingsWithoutPhases.put(nodeToTimingsEntry.getKey(), new HashMap<>(nodeToTimingsEntry.getValue()));
        }
        for (Map<String, Object> timings : nodeTimingsWithoutPhases.values()) {
            for (String phaseToRemove : phasesTimings.keySet()) {
                timings.remove(phaseToRemove);
            }
        }
        MapBuilder<String, Object> executionTimingsMap = MapBuilder.newMapBuilder();
        executionTimingsMap.put("Phases", phasesTimings);
        nodeTimingsWithoutPhases.forEach(executionTimingsMap::put);
        executionTimingsMap.put("Total", apeTimings.get(Phase.Execute.name()));

        if (subQueryExplainResults.isEmpty() == false) {
            ArrayList<Object> subQueryExplainResultsList = new ArrayList<>(subQueryExplainResults.size());
            for (var subQueryExplainResult : subQueryExplainResults) {
                subQueryExplainResultsList.add(subQueryExplainResult.get(Phase.Execute.name()));
            }
            executionTimingsMap.put("Sub-Queries", subQueryExplainResultsList);
        }

        mapBuilder.put(Phase.Execute.name(), executionTimingsMap.immutableMap());
        return mapBuilder.immutableMap();
    }


    private static void addPhaseTimings(ExecutionPhase leafExecutionPhase,
                                        Map<String, Map<String, Object>> timingsByNodeId,
                                        Map<String, Object> allPhases) {
        String phaseName = ProfilingContext.generateProfilingKey(leafExecutionPhase.phaseId(), leafExecutionPhase.name());
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
        Map<String, Object> phaseTimingsAcrossNodes = Collections.unmodifiableMap(timingsForPhaseAcrossNodes);

        if (!phaseTimingsAcrossNodes.isEmpty()) {
            allPhases.put(phaseName, Map.of("nodes", phaseTimingsAcrossNodes));
        }
    }
}
