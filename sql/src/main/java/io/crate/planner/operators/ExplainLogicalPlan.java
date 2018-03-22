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

package io.crate.planner.operators;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.OrderBy;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExplainLeaf;
import io.crate.planner.PlanPrinter;
import io.crate.planner.PlannerContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ExplainLogicalPlan {

    private static final Visitor VISITOR = new Visitor();

    /**
     * Tries to build and create an explain map out an {@link ExecutionPlan} out of the given {@link LogicalPlan}.
     * If it fails, fallback to create the map out of the {@link LogicalPlan}.
     */
    public static Map<String, Object> explainMap(LogicalPlan logicalPlan,
                                                PlannerContext plannerContext,
                                                ProjectionBuilder projectionBuilder) {
        try {
            ExecutionPlan executionPlan = logicalPlan.build(
                plannerContext,
                projectionBuilder,
                TopN.NO_LIMIT,
                0,
                null,
                null,
                Row.EMPTY,
                Collections.emptyMap());
            return PlanPrinter.objectMap(executionPlan);
        } catch (Exception e) {
            return VISITOR.process(logicalPlan, new Context(plannerContext, projectionBuilder)).build();
        }
    }

    private static Map<String, Object> explainMap(LogicalPlan logicalPlan, Context context) {
        return explainMap(logicalPlan, context.plannerContext, context.projectionBuilder);
    }

    private ExplainLogicalPlan() {
    }

    private static class Context {
        private final PlannerContext plannerContext;
        private final ProjectionBuilder projectionBuilder;

        public Context(PlannerContext plannerContext, ProjectionBuilder projectionBuilder) {
            this.plannerContext = plannerContext;
            this.projectionBuilder = projectionBuilder;
        }
    }

    private static class Visitor extends LogicalPlanVisitor<Context, ImmutableMap.Builder<String, Object>> {

        private static ImmutableMap.Builder<String, Object> createMap(LogicalPlan logicalPlan,
                                                                      ImmutableMap.Builder<String, Object> subMap) {
            return ImmutableMap.<String, Object>builder()
                .put(logicalPlan.getClass().getSimpleName(), subMap.build());
        }

        private static ImmutableMap.Builder<String, Object> createSubMap() {
            return ImmutableMap.<String, Object>builder().put("type", "logicalOperator");
        }

        @Override
        protected ImmutableMap.Builder<String, Object> visitPlan(LogicalPlan logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap());
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitMultiPhase(MultiPhase logicalPlan, Context context) {
            Map<String, Map<String, Object>> dependencies = new HashMap<>(logicalPlan.dependencies().size());
            for (Map.Entry<LogicalPlan, SelectSymbol> entry : logicalPlan.dependencies().entrySet()) {
                dependencies.put(entry.getValue().representation(), explainMap(entry.getKey(), context));
            }
            return createMap(logicalPlan, createSubMap()
                .put("source", explainMap(logicalPlan.source, context))
                .put("dependencies", dependencies));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitFetchOrEval(FetchOrEval logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("fetchRefs", ExplainLeaf.printList(logicalPlan.outputs()))
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCollect(Collect logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("toCollect", ExplainLeaf.printList(logicalPlan.outputs()))
                .put("where", logicalPlan.where.query().representation()));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitLimit(Limit logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("limit", SymbolPrinter.INSTANCE.printQualified(logicalPlan.limit))
                .put("offset", SymbolPrinter.INSTANCE.printQualified(logicalPlan.offset))
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCount(Count logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("where", logicalPlan.where.query().representation()));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitGet(Get logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("toCollect", ExplainLeaf.printList(logicalPlan.outputs()))
                .put("docKeys", logicalPlan.docKeys.toString()));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitFilter(Filter logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("filter", logicalPlan.queryClause.query().representation())
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitOrder(Order logicalPlan, Context context) {
            OrderBy orderBy = logicalPlan.orderBy;
            StringBuilder sb = new StringBuilder();
            OrderBy.explainRepresentation(sb, orderBy.orderBySymbols(), orderBy.reverseFlags(), orderBy.nullsFirst());
            return createMap(logicalPlan, createSubMap()
                .put("orderBy", sb.toString())
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitUnion(Union logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("left", explainMap(logicalPlan.lhs, context))
                .put("right", explainMap(logicalPlan.rhs, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitNestedLoopJoin(NestedLoopJoin logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("left", explainMap(logicalPlan.lhs, context))
                .put("right", explainMap(logicalPlan.rhs, context))
                .put("joinType", logicalPlan.joinType())
                .put("joinCondition",
                    SymbolPrinter.INSTANCE.printQualified(logicalPlan.joinCondition())));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitHashJoin(HashJoin logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("left", explainMap(logicalPlan.lhs, context))
                .put("right", explainMap(logicalPlan.rhs, context))
                .put("joinType", logicalPlan.joinType())
                .put("joinCondition",
                    SymbolPrinter.INSTANCE.printQualified(logicalPlan.joinCondition())));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitGroupHashAggregate(GroupHashAggregate logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("aggregates", ExplainLeaf.printList(logicalPlan.aggregates))
                .put("groupKeys", ExplainLeaf.printList(logicalPlan.groupKeys))
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitHashAggregate(HashAggregate logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("aggregates", ExplainLeaf.printList(logicalPlan.aggregates))
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitInsert(Insert logicalPlan, Context context) {
            return createMap(logicalPlan, createSubMap()
                .put("projections", logicalPlan.projections().stream().map(Projection::mapRepresentation))
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitRelationBoundary(RelationBoundary logicalPlan, Context context) {
            return ImmutableMap.<String, Object>builder().putAll(explainMap(logicalPlan.source, context));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitRootRelationBoundary(RootRelationBoundary logicalPlan, Context context) {
            return ImmutableMap.<String, Object>builder().putAll(explainMap(logicalPlan.source, context));
        }
    }
}
