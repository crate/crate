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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.FieldResolver;
import io.crate.expression.operator.any.AnyEqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.LookupJoin;
import io.crate.planner.operators.MultiPhase;
import io.crate.planner.operators.Rename;
import io.crate.planner.operators.RootRelationBoundary;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.types.DataTypes;

public class UseLookupJoin implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern = typeOf(JoinPlan.class).with(j -> j.isLookUpJoinRuleApplied() == false);

    @Override
    public Pattern<JoinPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(JoinPlan joinPlan,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {

        JoinPlan plan = (JoinPlan) planStats.memo().extract(joinPlan);
        var lhs = plan.lhs();
        var rhs = plan.rhs();

        if (lhs.getRelationNames().size() > 1 || rhs.getRelationNames().size() > 1) {
            return null;
        }

        var lhsStats = planStats.get(lhs).numDocs();
        var rhsStats = planStats.get(rhs).numDocs();

        if (lhsStats == -1 || rhsStats == -1) {
            return null;
        }

        LogicalPlan largerSide;
        LogicalPlan smallerSide;

        if (lhsStats > rhsStats) {
            largerSide = lhs;
            smallerSide = rhs;
        } else {
            largerSide = rhs;
            smallerSide = lhs;
        }
        Map<Symbol, FieldResolver> fieldResolverMap = new HashMap<>();
        if (largerSide instanceof Rename r1) {
            for (Symbol output : r1.outputs()) {
                fieldResolverMap.put(output, r1);
            }
        }
        if (smallerSide instanceof Rename r2) {
            for (Symbol output : r2.outputs()) {
                fieldResolverMap.put(output, r2);
            }
        }

        AbstractTableRelation<?> largerRelation = largerSide.baseTables().get(0);
        AbstractTableRelation<?> smallerRelation = smallerSide.baseTables().get(0);

        LookupJoin lookupJoin = create(
            largerRelation,
            smallerRelation,
            plan.joinCondition(),
            nodeCtx,
            txnCtx,
            fieldResolverMap);

        LogicalPlan newLargerSide;

        if (largerSide instanceof Rename rename) {
            newLargerSide = rename.replaceSources(List.of(lookupJoin));
        } else {
            newLargerSide = lookupJoin;
        }

        return new JoinPlan(
            newLargerSide,
            smallerSide,
            plan.joinType(),
            plan.joinCondition(),
            plan.isFiltered(),
            plan.isRewriteFilterOnOuterJoinToInnerJoinDone(),
            true);
    }

    public static LookupJoin create(AbstractTableRelation<?> largerRelation,
                                    AbstractTableRelation<?> smallerRelation,
                                    Symbol joinCondition,
                                    NodeContext nodeCtx,
                                    TransactionContext txnCtx,
                                    Map<Symbol, FieldResolver> fieldResolverMap) {

        Symbol smallerOutput;
        Symbol largerOutput;

        if (joinCondition instanceof Function joinConditionFunction) {
            var arguments = joinConditionFunction.arguments();
            if (arguments.size() == 2) {
                Symbol a = arguments.get(0);
                Symbol b = arguments.get(1);
                if (a instanceof ScopedSymbol aScopedSymbol && b instanceof ScopedSymbol bScopedSymbol) {
                    if (aScopedSymbol.relation().equals(smallerRelation.relationName())) {
                        smallerOutput = fieldResolverMap.get(aScopedSymbol).resolveField(aScopedSymbol);
                        largerOutput = fieldResolverMap.get(bScopedSymbol).resolveField(bScopedSymbol);
                    } else {
                        largerOutput = fieldResolverMap.get(aScopedSymbol).resolveField(aScopedSymbol);
                        smallerOutput = fieldResolverMap.get(bScopedSymbol).resolveField(bScopedSymbol);
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
            true);

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
        LogicalPlan largerSideWithLookup = new Collect(largerRelation, largerRelation.outputs(), whereClause);
        LogicalPlan smallerSideIdLookup = new RootRelationBoundary(new Collect(smallerRelation, smallerRelation.outputs(), WhereClause.MATCH_ALL));

        var multiPhase = new MultiPhase(largerSideWithLookup, Map.of(smallerSideIdLookup, selectSymbol));
        return new LookupJoin(largerRelation, smallerRelation, smallerOutput, largerOutput, multiPhase);
    }

    static class Visitor extends LogicalPlanVisitor<Void, Void> {

        final UnaryOperator<LogicalPlan> resolvePlan;
        final List<AbstractTableRelation> relations = new ArrayList<>();
        final Map<Symbol, FieldResolver> scopedSymbols = new HashMap<>();

        Visitor(UnaryOperator<LogicalPlan> resolvePlan) {
            this.resolvePlan = resolvePlan;
        }

        @Override
        public Void visitGroupReference(GroupReference apply, Void context) {
            return resolvePlan.apply(apply).accept(this, context);
        }

        @Override
        public Void visitRename(Rename rename, Void context) {
            for (Symbol output : rename.outputs()) {
                scopedSymbols.put(output, rename);
            }
            return null;
        }

        @Override
        public Void visitCollect(Collect logicalPlan, Void context) {
            relations.add(logicalPlan.relation());
            return null;
        }
    }
}
