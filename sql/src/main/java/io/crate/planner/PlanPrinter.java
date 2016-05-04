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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.dml.InsertFromSubQuery;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.ProjectionVisitor;
import io.crate.planner.projection.TopNProjection;

import javax.annotation.Nullable;
import java.util.Map;

public class PlanPrinter {

    private PlanPrinter() {
    }

    public static Map<String, Object> objectMap(Plan plan) {
        return Plan2MapVisitor.toMap(plan);
    }

    private static Iterable<Object> refs(Iterable<? extends Symbol> symbols) {
        return FluentIterable.from(symbols).transform(new Function<Symbol, Object>() {
            @Nullable
            @Override
            public Object apply(@Nullable Symbol input) {
                return SymbolPrinter.INSTANCE.print(input, SymbolPrinter.Style.FULL_QUALIFIED);
            }
        });
    }

    static class ExecutionPhase2MapVisitor extends ExecutionPhaseVisitor<Void, ImmutableMap.Builder<String, Object>> {

        public static final ExecutionPhase2MapVisitor INSTANCE = new ExecutionPhase2MapVisitor();

        private ExecutionPhase2MapVisitor() {
        }

        public static ImmutableMap.Builder<String, Object> toBuilder(ExecutionPhase node) {
            assert node != null;
            return INSTANCE.process(node, null);
        }

        private static Iterable<Map<String, Object>> projections(Iterable<Projection> projections) {
            return FluentIterable.from(projections).transform(new Function<Projection, Map<String, Object>>() {
                @Nullable
                @Override
                public Map<String, Object> apply(@Nullable Projection input) {
                    assert input != null;
                    return Projection2MapVisitor.toBuilder(input).build();
                }
            });
        }

        private static ImmutableMap.Builder<String, Object> newBuilder() {
            return ImmutableMap.builder();
        }

        @Override
        protected ImmutableMap.Builder<String, Object> visitExecutionPhase(ExecutionPhase phase, Void context) {
            return newBuilder()
                    .put("phaseType", phase.type())
                    .put("id", phase.executionPhaseId())
                    .put("executionNodes", phase.executionNodes()
                    );
        }

        private ImmutableMap.Builder<String, Object> process(DistributionInfo info) {
            return newBuilder()
                    .put("distributedByColumn", info.distributeByColumn())
                    .put("type", info.distributionType());
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


    static class Projection2MapVisitor extends ProjectionVisitor<Void, ImmutableMap.Builder<String, Object>> {

        private static final Projection2MapVisitor INSTANCE = new Projection2MapVisitor();

        private static ImmutableMap.Builder<String, Object> newBuilder() {
            return ImmutableMap.builder();
        }

        public static ImmutableMap.Builder<String, Object> toBuilder(Projection projection) {
            assert projection != null;
            return INSTANCE.process(projection, null);
        }

        @Override
        protected ImmutableMap.Builder<String, Object> visitProjection(Projection projection, Void context) {
            return newBuilder().put("type", projection.projectionType());
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitTopNProjection(TopNProjection projection, Void context) {
            return visitProjection(projection, context)
                    .put("rows", projection.offset() + "-" + projection.limit());
        }
    }

    static class Plan2MapVisitor extends PlanVisitor<Void, ImmutableMap.Builder<String, Object>> {

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

        public static Map<String, Object> toMap(Plan plan) {
            assert plan != null;
            return INSTANCE.process(plan, null).build();
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCollectAndMerge(CollectAndMerge plan, Void context) {
            ImmutableMap.Builder<String, Object> b = visitPlan(plan, context)
                    .put("collectPhase", phaseMap(plan.collectPhase()));
            if (plan.localMerge() != null) {
                b.put("localMerge", phaseMap(plan.localMerge()));
            }
            return b;
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitInsertByQuery(InsertFromSubQuery node, Void context) {
            ImmutableMap.Builder<String, Object> builder = visitPlan(node, context);
            builder.put("innerPlan", process(node.innerPlan(), context).build());

            if (node.handlerMergeNode().isPresent()) {
                builder.put("localMerge", phaseMap(node.handlerMergeNode().get()));
            }
            return builder;
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitNestedLoop(NestedLoop plan, Void context) {
            ImmutableMap.Builder<String, Object> builder = newBuilder()
                    .put("planType", plan.getClass().getSimpleName())
                    .put("left", process(plan.left().plan(), context).build())
                    .put("right", process(plan.right().plan(), context).build())
                    .put("nestedLoopPhase", phaseMap(plan.nestedLoopPhase()));

            MergePhase mergePhase = plan.localMerge();
            if (mergePhase != null) {
                builder.put("localMerge", phaseMap(mergePhase));
            }
            return builder;
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitQueryThenFetch(QueryThenFetch plan, Void context) {
            ImmutableMap.Builder<String, Object> b = visitPlan(plan, context)
                    .put("subPlan", toMap(plan.subPlan()));
            if (plan.localMerge() != null) {
                b.put("localMerge", phaseMap(plan.localMerge()));
            }
            b.put("fetchPhase", phaseMap(plan.fetchPhase()));
            return b;
        }
    }
}
