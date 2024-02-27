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
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.NodeContext;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.TransactionContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.types.DataTypes;

public class LookupJoin extends ForwardingLogicalPlan {

    private AbstractTableRelation<?> largerRelation;
    private AbstractTableRelation<?> smallerRelation;
    private Symbol joinCondition;

    public static LookupJoin create(AbstractTableRelation<?> largerRelation,
                             AbstractTableRelation<?> smallerRelation,
                             Symbol joinCondition,
                             NodeContext nodeCtx,
                             TransactionContext txnCtx) {

        Symbol smallerOutput;
        Symbol largerOutput;

        if (joinCondition instanceof Function joinConditionFunction) {
            var arguments = joinConditionFunction.arguments();
            if (arguments.size() == 2) {
                Symbol a = arguments.get(0);
                Symbol b = arguments.get(1);
                if (a instanceof ScopedSymbol aScopedSymbol) {
                    if (aScopedSymbol.relation().equals(smallerRelation.relationName())) {
                        smallerOutput = a;
                        largerOutput = b;
                    } else {
                        largerOutput = a;
                        smallerOutput = b;
                    }
                } else if (a instanceof SimpleReference aref) {
                    if (aref.ident().tableIdent().equals(smallerRelation.relationName())) {
                        smallerOutput = a;
                        largerOutput = b;
                    } else {
                        largerOutput = a;
                        smallerOutput = b;
                    }
                } else {
                    throw new IllegalArgumentException("Error building lookup-join");
                }
            } else {
                throw new IllegalArgumentException("Error building lookup-join");
            }
        } else {
            throw new IllegalArgumentException("Error building lookup-join");
        }

        var queriedSelectRelation = new QueriedSelectRelation(
            false,
            List.of(smallerRelation),
            List.of(),
            List.of(smallerOutput),
            Literal.BOOLEAN_TRUE,
            List.of(),
            null,
            null,
            null,
            null
        );

        var selectSymbol = new SelectSymbol(
            queriedSelectRelation,
            DataTypes.getArrayTypeByInnerType(smallerOutput.valueType()),
            SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES,
            false);

        var functionImplementation = nodeCtx.functions().get(
            null,
            AnyEqOperator.NAME,
            List.of(largerOutput, selectSymbol),
            txnCtx.sessionSettings().searchPath()
        );

        var function = new Function(
            functionImplementation.signature(),
            List.of(largerOutput, selectSymbol),
            DataTypes.BOOLEAN
        );

        WhereClause whereClause = new WhereClause(function);
        var largerSideWithLookup = new Collect(largerRelation, largerRelation.outputs(), whereClause);
        var smallerSideIdLookup = new Collect(smallerRelation, smallerRelation.outputs(), WhereClause.MATCH_ALL);
        var multiPhase = new MultiPhase(largerSideWithLookup, Map.of(smallerSideIdLookup, selectSymbol));
        return new LookupJoin(largerRelation, smallerRelation, joinCondition, multiPhase);
    }

    private LookupJoin(AbstractTableRelation<?> largerRelation,
                       AbstractTableRelation<?> smallerRelation,
                       Symbol joinCondition,
                       LogicalPlan multiPhase) {
        super(multiPhase);
        this.largerRelation = largerRelation;
        this.smallerRelation = smallerRelation;
        this.joinCondition = joinCondition;
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
        return new LookupJoin(largerRelation, smallerRelation, joinCondition, Lists.getOnlyElement(sources));
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitLookupJoin(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("LookupJoin[")
            .text(largerRelation.relationName().toString())
            .text(" | ")
            .text(joinCondition.toString(Style.QUALIFIED))
            .text("]");
        printStats(printContext);
        printContext.nest(source::print);
    }
}
