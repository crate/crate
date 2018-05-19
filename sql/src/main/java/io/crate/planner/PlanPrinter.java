/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner;

import com.google.common.collect.ImmutableMap;
import io.crate.execution.dsl.phases.AbstractProjectionsPhase;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhaseVisitor;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.JoinPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.phases.UpstreamPhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.Join;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class PlanPrinter {

    private PlanPrinter() {
    }

    public static Map<String, Object> objectMap(ExecutionPlan executionPlan) {
        return ExecutionPlan2MapVisitor.createMap(executionPlan);
    }

    private static class ExecutionPhase2MapVisitor extends ExecutionPhaseVisitor<Void, ImmutableMap.Builder<String, Object>> {

        public static final ExecutionPhase2MapVisitor INSTANCE = new ExecutionPhase2MapVisitor();

        private static ImmutableMap.Builder<String, Object> createMap(ExecutionPhase executionPhase,
                                                                      ImmutableMap.Builder<String, Object> subMap) {
            return ImmutableMap.<String, Object>builder()
                .put(executionPhase.type().toString(), subMap.build());
        }

        private static ImmutableMap.Builder<String, Object> createSubMap(ExecutionPhase phase) {
            return ImmutableMap.<String, Object>builder()
                .put("type", "executionPhase")
                .put("id", phase.phaseId())
                // Converting TreeMap.keySet() to be able to stream
                .put("executionNodes", new ArrayList<>(phase.nodeIds()));
        }

        static ImmutableMap.Builder<String, Object> toBuilder(ExecutionPhase executionPhase) {
            assert executionPhase != null : "executionPhase must not be null";
            return INSTANCE.process(executionPhase, null);
        }

        private static List<Map<String, Object>> projections(Iterable<Projection> projections) {
            // need to return a List because this is part of a map which is streamed to the client.
            // Map streaming uses StreamOutput#writeGenericValue which can't handle Iterable
            ArrayList<Map<String, Object>> result = new ArrayList<>();
            for (Projection projection : projections) {
                result.add(projection.mapRepresentation());
            }
            return result;
        }


        private ExecutionPhase2MapVisitor() {
        }

        @Override
        protected ImmutableMap.Builder<String, Object> visitExecutionPhase(ExecutionPhase phase, Void context) {
            return createMap(phase, createSubMap(phase));
        }

        private ImmutableMap.Builder<String, Object> process(DistributionInfo info) {
            return ImmutableMap.<String, Object>builder()
                .put("distributedByColumn", info.distributeByColumn())
                .put("type", info.distributionType().toString());
        }

        private ImmutableMap.Builder<String, Object> upstreamPhase(UpstreamPhase phase, ImmutableMap.Builder<String, Object> b) {
            return b.put("distribution", process(phase.distributionInfo()).build());
        }

        private ImmutableMap.Builder<String, Object> dqlPlanNode(AbstractProjectionsPhase phase, ImmutableMap.Builder<String, Object> b) {
            if (phase.hasProjections()) {
                b.put("projections", projections(phase.projections()));
            }
            return b;
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitRoutedCollectPhase(RoutedCollectPhase phase, Void context) {
            ImmutableMap.Builder<String, Object> builder = upstreamPhase(phase, createSubMap(phase));
            builder.put("toCollect", ExplainLeaf.printList(phase.toCollect()));
            builder = dqlPlanNode(phase, builder);
            builder.put("routing", phase.routing().locations());
            builder.put("where", phase.where().representation());
            return createMap(phase, builder);
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCollectPhase(CollectPhase phase, Void context) {
            ImmutableMap.Builder<String, Object> builder = upstreamPhase(phase, createSubMap(phase));
            builder.put("toCollect", ExplainLeaf.printList(phase.toCollect()));
            return createMap(phase, builder);
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCountPhase(CountPhase phase, Void context) {
            ImmutableMap.Builder<String, Object> builder = upstreamPhase(phase, visitExecutionPhase(phase, context));
            builder.put("routing", phase.routing().locations());
            builder.put("where", phase.where().representation());
            return builder;
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitFetchPhase(FetchPhase phase, Void context) {
            return createMap(phase, createSubMap(phase)
                .put("fetchRefs", ExplainLeaf.printList(phase.fetchRefs())));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitMergePhase(MergePhase phase, Void context) {
            ImmutableMap.Builder<String, Object> b = upstreamPhase(phase, createSubMap(phase));
            return createMap(phase, dqlPlanNode(phase, b));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitNestedLoopPhase(NestedLoopPhase phase, Void context) {
            return getBuilderForJoinPhase(phase);
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitHashJoinPhase(HashJoinPhase phase, Void context) {
            return getBuilderForJoinPhase(phase);
        }

        private ImmutableMap.Builder<String, Object> getBuilderForJoinPhase(JoinPhase phase) {
            ImmutableMap.Builder<String, Object> b = upstreamPhase(
                phase,
                createSubMap(phase).put("joinType", phase.joinType()));
            return createMap(phase, dqlPlanNode(phase, b));
        }
    }

    private static class ExecutionPlan2MapVisitor extends ExecutionPlanVisitor<Void, ImmutableMap.Builder<String, Object>> {

        private static final ExecutionPlan2MapVisitor INSTANCE = new ExecutionPlan2MapVisitor();

        private static ImmutableMap.Builder<String, Object> createMap(ExecutionPlan executionPlan,
                                                                      ImmutableMap.Builder<String, Object> subMap) {
            return ImmutableMap.<String, Object>builder()
                .put(executionPlan.getClass().getSimpleName(), subMap.build());
        }

        private static ImmutableMap.Builder<String, Object> createSubMap() {
            return ImmutableMap.<String, Object>builder().put("type", "executionPlan");
        }

        @Override
        protected ImmutableMap.Builder<String, Object> visitPlan(ExecutionPlan executionPlan, Void context) {
            return createMap(executionPlan, createSubMap());
        }

        private static Map<String, Object> phaseMap(@Nullable ExecutionPhase node) {
            if (node == null) {
                return null;
            } else {
                return ExecutionPhase2MapVisitor.toBuilder(node).build();
            }
        }

        static Map<String, Object> createMap(ExecutionPlan executionPlan) {
            assert executionPlan != null : "plan must not be null";
            return INSTANCE.process(executionPlan, null).build();
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCollect(Collect plan, Void context) {
            return createMap(plan, createSubMap()
                .put("collectPhase", phaseMap(plan.collectPhase())));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitJoin(Join plan, Void context) {
            return createMap(plan, createSubMap()
                .put("left", process(plan.left(), context).build())
                .put("right", process(plan.right(), context).build())
                .put("joinPhase", phaseMap(plan.joinPhase())));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitQueryThenFetch(QueryThenFetch plan, Void context) {
            return createMap(plan, createSubMap()
                .put("subPlan", createMap(plan.subPlan()))
                .put("fetchPhase", phaseMap(plan.fetchPhase())));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitMerge(Merge merge, Void context) {
            return createMap(merge, createSubMap()
                .put("subPlan", createMap(merge.subPlan()))
                .put("mergePhase", phaseMap(merge.mergePhase())));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitUnionPlan(UnionExecutionPlan unionExecutionPlan, Void context) {
            return createMap(unionExecutionPlan, createSubMap()
                .put("left", createMap(unionExecutionPlan.left()))
                .put("right", createMap(unionExecutionPlan.right()))
                .put("mergePhase", phaseMap(unionExecutionPlan.mergePhase())));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCountPlan(CountPlan countPlan, Void context) {
            return visitPlan(countPlan, context)
                .put("countPhase", phaseMap(countPlan.countPhase()))
                .put("mergePhase", phaseMap(countPlan.mergePhase()));
        }
    }
}
