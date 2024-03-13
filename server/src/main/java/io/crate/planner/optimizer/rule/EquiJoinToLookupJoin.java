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

import static io.crate.common.collections.Iterables.getOnlyElement;
import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.expression.operator.any.AnyEqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.EquiJoinDetector;
import io.crate.planner.operators.JoinConditionSymbolsExtractor;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.MultiPhase;
import io.crate.planner.operators.RootRelationBoundary;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

/**
 * This rule optimizes equi-inner-joins with a lookup-join if there is
 * a large imbalance between the two relations joined.
 */
public class EquiJoinToLookupJoin implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern = typeOf(JoinPlan.class).with(j ->
        j.isLookUpJoinRuleApplied() == false &&
            // only inner-equi-joins
            j.joinType() == JoinType.INNER &&
            j.joinCondition() != null &&
            EquiJoinDetector.isEquiJoin(j.joinCondition())
    );

    @Override
    public Pattern<JoinPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(JoinPlan plan,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {

        LogicalPlan lhs = resolvePlan.apply(plan.lhs());
        LogicalPlan rhs = resolvePlan.apply(plan.rhs());

        if (lhs instanceof Collect == false || rhs instanceof Collect == false) {
            return null;
        }

        long lhsNumDocs = planStats.get(lhs).numDocs();
        long rhsNumDocs = planStats.get(rhs).numDocs();

        if (applyOptimization(lhsNumDocs, rhsNumDocs) == false) {
            return null;
        }

        Collect smallerSide;
        Collect largerSide;

        boolean rhsIsLarger = rhsNumDocs > lhsNumDocs;

        if (rhsIsLarger) {
            smallerSide = (Collect) lhs;
            largerSide = (Collect) rhs;
        } else {
            smallerSide = (Collect) rhs;
            largerSide = (Collect) lhs;
        }

        Map<RelationName, List<Symbol>> equiJoinCondition = JoinConditionSymbolsExtractor.extract(plan.joinCondition());
        Symbol smallerRelationColumn = getOnlyElement(equiJoinCondition.get(smallerSide.relation().relationName()));
        Symbol largerRelationColumn = getOnlyElement(equiJoinCondition.get(largerSide.relation().relationName()));

        LogicalPlan lookupJoin = createLookup(
            smallerSide,
            smallerRelationColumn,
            largerSide,
            largerRelationColumn,
            nodeCtx,
            txnCtx
        );

        LogicalPlan newLhs;
        LogicalPlan newRhs;

        if (rhsIsLarger) {
            newLhs = lhs;
            newRhs = lookupJoin;
        } else {
            newLhs = lookupJoin;
            newRhs = rhs;
        }

        return new JoinPlan(
            newLhs,
            newRhs,
            plan.joinType(),
            plan.joinCondition(),
            plan.isFiltered(),
            plan.isRewriteFilterOnOuterJoinToInnerJoinDone(),
            true
        );
    }

    private static LogicalPlan createLookup(Collect smallerRelation,
                                            Symbol smallerRelationColumn,
                                            Collect largerRelation,
                                            Symbol largerRelationColumn,
                                            NodeContext nodeCtx,
                                            TransactionContext txnCtx) {

        var lookUpQuery = new SelectSymbol(
            new QueriedSelectRelation(
                false,
                List.of(smallerRelation.relation()),
                List.of(),
                List.of(smallerRelationColumn),
                Literal.BOOLEAN_TRUE,
                List.of(),
                null,
                null,
                null,
                null
            ),
            new ArrayType<>(smallerRelationColumn.valueType()),
            SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES,
            true
        );

        var anyEqImplementation = nodeCtx.functions().get(
            null,
            AnyEqOperator.NAME,
            List.of(largerRelationColumn, lookUpQuery),
            txnCtx.sessionSettings().searchPath()
        );

        var anyEqFunction = new Function(
            anyEqImplementation.signature(),
            List.of(largerRelationColumn, lookUpQuery),
            DataTypes.BOOLEAN
        );
        WhereClause whereClause = largerRelation.where().add(anyEqFunction);
        LogicalPlan largerSideWithLookup = new Collect(largerRelation.relation(), largerRelation.outputs(), whereClause);
        LogicalPlan smallerSideIdLookup = new Collect(smallerRelation.relation(), List.of(smallerRelationColumn), smallerRelation.where());
        smallerSideIdLookup = new RootRelationBoundary(smallerSideIdLookup);

        return MultiPhase.createIfNeeded(Map.of(smallerSideIdLookup, lookUpQuery), largerSideWithLookup);
    }

    private static boolean applyOptimization(long lhsNumDocs, long rhsNumDocs) {
        if ((lhsNumDocs == -1L || rhsNumDocs == -1L) ||
            (lhsNumDocs == 0 || rhsNumDocs == 0) ||
            lhsNumDocs == rhsNumDocs) {
            return false;
        }
        long smaller, larger;
        if (lhsNumDocs > rhsNumDocs) {
            larger = lhsNumDocs;
            smaller = rhsNumDocs;

        } else {
            larger = rhsNumDocs;
            smaller = lhsNumDocs;
        }
        // Benchmarks revealed that the imbalance should be at least factor 10 with the larger table at least 10.000 rows
        // See https://github.com/crate/crate/pull/15664 for details
        if (larger < 10_000) {
            return false;
        }
        return larger / smaller >= 10;
    }
}
