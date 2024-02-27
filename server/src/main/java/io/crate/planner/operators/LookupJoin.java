/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.operators;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.operator.any.AnyEqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.types.DataTypes;

public class LookupJoin extends ForwardingLogicalPlan {

    private final AbstractTableRelation<?> relation;

    public static LookupJoin create(AbstractTableRelation<?> smallerRelation,
                                    Symbol smallerRelationColumn,
                                    AbstractTableRelation<?> largerRelation,
                                    Symbol largerRelationColumn,
                                    NodeContext nodeCtx,
                                    TransactionContext txnCtx) {

        var lookUpQuery = new SelectSymbol(
            new QueriedSelectRelation(
                false,
                List.of(smallerRelation),
                List.of(),
                List.of(smallerRelationColumn),
                Literal.BOOLEAN_TRUE,
                List.of(),
                null,
                null,
                null,
                null
            ),
            DataTypes.getArrayType(smallerRelationColumn.valueType()),
            SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES,
            true
        );

        var functionImplementation = nodeCtx.functions().get(
            null,
            AnyEqOperator.NAME,
            List.of(largerRelationColumn, lookUpQuery),
            txnCtx.sessionSettings().searchPath()
        );

        var function = new Function(
            functionImplementation.signature(),
            List.of(largerRelationColumn, lookUpQuery),
            DataTypes.BOOLEAN
        );

        WhereClause whereClause = new WhereClause(function);
        LogicalPlan largerSideWithLookup = new Collect(largerRelation, largerRelation.outputs(), whereClause);
        LogicalPlan smallerSideIdLookup = new Collect(smallerRelation, List.of(smallerRelationColumn), WhereClause.MATCH_ALL);
        smallerSideIdLookup = new RootRelationBoundary(smallerSideIdLookup);

        MultiPhase multiPhase = new MultiPhase(largerSideWithLookup, Map.of(smallerSideIdLookup, lookUpQuery));
        return new LookupJoin(largerRelation, multiPhase);
    }

    private LookupJoin(AbstractTableRelation<?> largerRelation, LogicalPlan multiPhase) {
        super(multiPhase);
        this.relation = largerRelation;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return source.build(
            executor, plannerContext, planHints, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new LookupJoin(relation, Lists.getOnlyElement(sources));
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitLookupJoin(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("LookupJoin[")
            .text(relation.relationName().toString())
            .text("]");
        printStats(printContext);
        printContext.nest(source::print);
    }
}
