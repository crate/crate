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
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PlanPrinter {

    private PlanPrinter() {
    }

    public static Map<String, Object> objectMap(Plan plan) {
        return Plan2MapVisitor.toMap(plan);
    }

    private static List<Object> refs(Collection<? extends Symbol> symbols) {
        List<Object> refs = new ArrayList<>(symbols.size());
        for (Symbol s : symbols) {
            refs.add(SymbolPrinter.INSTANCE.print(s, SymbolPrinter.Style.FULL_QUALIFIED));
        }
        return refs;
    }

    private static class ExecutionPhase2MapVisitor extends ExecutionPhaseVisitor<Void, ImmutableMap.Builder<String, Object>> {

        public static final ExecutionPhase2MapVisitor INSTANCE = new ExecutionPhase2MapVisitor();

        private ExecutionPhase2MapVisitor() {
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

        private static ImmutableMap.Builder<String, Object> newBuilder() {
            return ImmutableMap.builder();
        }

        @Override
        protected ImmutableMap.Builder<String, Object> visitExecutionPhase(ExecutionPhase phase, Void context) {
            return newBuilder()
                .put("phaseType", phase.type().toString())
                .put("id", phase.phaseId())
                // Converting TreeMap.keySet() to be able to stream
                .put("executionNodes", new ArrayList<>(phase.nodeIds())
                );
        }

        private ImmutableMap.Builder<String, Object> process(DistributionInfo info) {
            return newBuilder()
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
            ImmutableMap.Builder<String, Object> builder = visitCollectPhase(phase, context);
            builder = dqlPlanNode(phase, builder);
            builder.put("routing", phase.routing().locations());
            return builder;
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCollectPhase(CollectPhase phase, Void context) {
            ImmutableMap.Builder<String, Object> builder = upstreamPhase(phase, visitExecutionPhase(phase, context));
            builder.put("toCollect", refs(phase.toCollect()));
            return builder;
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCountPhase(CountPhase phase, Void context) {
            return upstreamPhase(phase, visitExecutionPhase(phase, context));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitFetchPhase(FetchPhase phase, Void context) {
            return visitExecutionPhase(phase, context)
                .put("fetchRefs", refs(phase.fetchRefs()));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitMergePhase(MergePhase phase, Void context) {
            ImmutableMap.Builder<String, Object> b = upstreamPhase(phase, visitExecutionPhase(phase, context));
            return dqlPlanNode(phase, b);
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitNestedLoopPhase(NestedLoopPhase phase, Void context) {
            ImmutableMap.Builder<String, Object> b = upstreamPhase(phase, visitExecutionPhase(phase, context));
            return dqlPlanNode(phase, b);
        }
    }

    private static class Plan2MapVisitor extends PlanVisitor<Void, ImmutableMap.Builder<String, Object>> {

        private static final Plan2MapVisitor INSTANCE = new Plan2MapVisitor();

        private static ImmutableMap.Builder<String, Object> newBuilder() {
            return ImmutableMap.builder();
        }

        @Override
        protected ImmutableMap.Builder<String, Object> visitPlan(Plan plan, Void context) {
            return newBuilder()
                .put("planType", plan.getClass().getSimpleName());
        }

        private static Map<String, Object> phaseMap(@Nullable ExecutionPhase node) {
            if (node == null) {
                return null;
            } else {
                return ExecutionPhase2MapVisitor.toBuilder(node).build();
            }
        }

        static Map<String, Object> toMap(Plan plan) {
            assert plan != null : "plan must not be null";
            return INSTANCE.process(plan, null).build();
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCollect(Collect plan, Void context) {
            ImmutableMap.Builder<String, Object> b = visitPlan(plan, context)
                .put("collectPhase", phaseMap(plan.collectPhase()));
            return b;
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitNestedLoop(NestedLoop plan, Void context) {
            return newBuilder()
                .put("planType", plan.getClass().getSimpleName())
                .put("left", process(plan.left(), context).build())
                .put("right", process(plan.right(), context).build())
                .put("nestedLoopPhase", phaseMap(plan.nestedLoopPhase()));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitQueryThenFetch(QueryThenFetch plan, Void context) {
            return visitPlan(plan, context)
                .put("subPlan", toMap(plan.subPlan()))
                .put("fetchPhase", phaseMap(plan.fetchPhase()));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, Void context) {
            List<Map<String, Object>> dependencies = new ArrayList<>(multiPhasePlan.dependencies().size());
            for (Plan dependency : multiPhasePlan.dependencies().keySet()) {
                dependencies.add(toMap(dependency));
            }
            return visitPlan(multiPhasePlan, context)
                .put("rootPlan", toMap(multiPhasePlan.rootPlan()))
                .put("dependencies", dependencies);
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitMerge(Merge merge, Void context) {
            return visitPlan(merge, context)
                .put("subPlan", toMap(merge.subPlan()))
                .put("mergePhase", phaseMap(merge.mergePhase()));
        }
    }
}
