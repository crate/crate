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
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.planner.PlannerContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.planner.PlanPrinter.refs;

public class ExplainLogicalPlan {

    private static final Visitor VISITOR = new Visitor();

    public static Map<String, Object> objectMap(LogicalPlan logicalPlan,
                                                PlannerContext plannerContext,
                                                ProjectionBuilder projectionBuilder) {
        return VISITOR.process(logicalPlan, new Context(plannerContext, projectionBuilder)).build();
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

        private static ImmutableMap.Builder<String, Object> newBuilder() {
            return ImmutableMap.builder();
        }

        private static ImmutableMap.Builder<String, Object> subMap() {
            return newBuilder().put("type", "logicalOperator");
        }

        private static ImmutableMap.Builder<String, Object> toMap(LogicalPlan logicalPlan,
                                                                  ImmutableMap.Builder<String, Object> subMap) {
            return newBuilder()
                .put(logicalPlan.getClass().getSimpleName(), subMap.build());
        }

        private static Map<String, Object> explainMap(LogicalPlan logicalPlan, Context context) {
            return logicalPlan.explainMap(context.plannerContext, context.projectionBuilder);
        }

        @Override
        protected ImmutableMap.Builder<String, Object> visitPlan(LogicalPlan logicalPlan, Context context) {
            return toMap(logicalPlan, subMap());
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitMultiPhase(MultiPhase logicalPlan, Context context) {
            Map<String, Map<String, Object>> dependencies = new HashMap<>(logicalPlan.dependencies().size());
            for (Map.Entry<LogicalPlan, SelectSymbol> entry : logicalPlan.dependencies().entrySet()) {
                dependencies.put(entry.getValue().representation(), explainMap(entry.getKey(), context));
            }
            return toMap(logicalPlan, subMap()
                .put("source", explainMap(logicalPlan.source, context))
                .put("dependencies", dependencies));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitFetchOrEval(FetchOrEval logicalPlan, Context context) {
            return toMap(logicalPlan, subMap()
                .put("fetchRefs", refs(logicalPlan.outputs()))
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCollect(Collect logicalPlan, Context context) {
            return toMap(logicalPlan, subMap()
                .put("toCollect", refs(logicalPlan.outputs()))
                .put("where", logicalPlan.where.query().representation()));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitLimit(Limit logicalPlan, Context context) {
            return toMap(logicalPlan, subMap()
                .put("limit", SymbolPrinter.INSTANCE.print(logicalPlan.limit, SymbolPrinter.Style.FULL_QUALIFIED))
                .put("offset", SymbolPrinter.INSTANCE.print(logicalPlan.offset, SymbolPrinter.Style.FULL_QUALIFIED))
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitCount(Count logicalPlan, Context context) {
            return toMap(logicalPlan, subMap()
                .put("where", logicalPlan.where.query().representation()));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitGet(Get logicalPlan, Context context) {
            return toMap(logicalPlan, subMap()
                .put("toCollect", refs(logicalPlan.outputs()))
                .put("docKeys", logicalPlan.docKeys.toString()));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitFilter(Filter logicalPlan, Context context) {
            return toMap(logicalPlan, subMap()
                .put("filter", logicalPlan.queryClause.query().representation())
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitOrder(Order logicalPlan, Context context) {
            OrderBy orderBy = logicalPlan.orderBy;
            StringBuilder sb = new StringBuilder();
            OrderBy.explainRepresentation(sb, orderBy.orderBySymbols(), orderBy.reverseFlags(), orderBy.nullsFirst());
            return toMap(logicalPlan, subMap()
                .put("orderBy", sb.toString())
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitUnion(Union logicalPlan, Context context) {
            return toMap(logicalPlan, subMap()
                .put("left", explainMap(logicalPlan.lhs, context))
                .put("right", explainMap(logicalPlan.rhs, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitJoin(Join logicalPlan, Context context) {
            return toMap(logicalPlan, subMap()
                .put("left", explainMap(logicalPlan.lhs, context))
                .put("right", explainMap(logicalPlan.rhs, context))
                .put("joinType", logicalPlan.joinType)
                .put("joinCondition", SymbolPrinter.INSTANCE.print(logicalPlan.joinCondition, SymbolPrinter.Style.FULL_QUALIFIED)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitGroupHashAggregate(GroupHashAggregate logicalPlan, Context context) {
            List<Object> aggregates = new ArrayList<>(logicalPlan.aggregates);
            for (Function function : logicalPlan.aggregates) {
                aggregates.add(SymbolPrinter.INSTANCE.print(function, SymbolPrinter.Style.FULL_QUALIFIED));
            }
            List<Object> groupKeys = new ArrayList<>(logicalPlan.groupKeys);
            for (Symbol symbol : logicalPlan.groupKeys) {
                aggregates.add(SymbolPrinter.INSTANCE.print(symbol, SymbolPrinter.Style.FULL_QUALIFIED));
            }
            return toMap(logicalPlan, subMap()
                .put("aggregates", aggregates)
                .put("groupKeys", groupKeys)
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitHashAggregate(HashAggregate logicalPlan, Context context) {
            List<Object> aggregates = new ArrayList<>(logicalPlan.aggregates);
            for (Function function : logicalPlan.aggregates) {
                aggregates.add(SymbolPrinter.INSTANCE.print(function, SymbolPrinter.Style.FULL_QUALIFIED));
            }
            return toMap(logicalPlan, subMap()
                .put("aggregates", aggregates)
                .put("source", explainMap(logicalPlan.source, context)));
        }

        @Override
        public ImmutableMap.Builder<String, Object> visitInsert(Insert logicalPlan, Context context) {
            return toMap(logicalPlan, subMap()
                .put("projection", logicalPlan.projection.mapRepresentation())
                .put("source", explainMap(logicalPlan.source, context)));
        }
    }
}
