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

import io.crate.analyze.relations.PlannedRelation;
import io.crate.expression.operator.any.AnyEqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.EquiJoinDetector;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinConditionSymbolsExtractor;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.MultiPhase;
import io.crate.planner.operators.RootRelationBoundary;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.iterative.GroupReferenceResolver;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

/**
 * This rule optimizes equi-inner-joins with a lookup-join if there is
 * a large imbalance between the two relations joined. This means
 * the large table will be pre-filtered by the join condition
 * with the smaller table to reduce the number of join operations e.g.:
 * Join[INNER | (id = id)] (rows=100)
 *  ├ Collect[doc.t1 | [id] | true] (rows=100000)
 *  └ Collect[doc.t2 | [id] | true] (rows=1000)
 * to:
 * Join[INNER | (id = id)]
 *  ├ MultiPhase
 *  │  └ Collect[doc.t1 | [id] | (id = ANY((doc.t2)))]
 *  │  └ Collect[doc.t2 | [id] | true]
 *  └ Collect[doc.t2 | [id] | true]
 */
public class EquiJoinToLookupJoin implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern = typeOf(JoinPlan.class).with(j ->
            j.isLookUpJoinRuleApplied() == false &&
            // only inner-equi-joins
            j.joinType() == JoinType.INNER &&
            j.joinCondition() != null &&
            EquiJoinDetector.isEquiJoin(j.joinCondition()) &&
            // no nested subqueries
            j.lhs().relationNames().size() == 1 &&
            j.rhs().relationNames().size() == 1
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

        LogicalPlan lhs = plan.lhs();
        LogicalPlan rhs = plan.rhs();

        long lhsNumDocs = planStats.get(lhs).numDocs();
        long rhsNumDocs = planStats.get(rhs).numDocs();

        if (applyOptimization(lhsNumDocs, rhsNumDocs) == false) {
            return null;
        }

        LogicalPlan smallerSide;
        LogicalPlan largerSide;

        boolean rhsIsLarger = rhsNumDocs > lhsNumDocs;

        if (rhsIsLarger) {
            smallerSide = lhs;
            largerSide = rhs;
        } else {
            smallerSide = rhs;
            largerSide = lhs;
        }

        smallerSide = GroupReferenceResolver.resolveFully(resolvePlan, smallerSide);

        Map<RelationName, List<Symbol>> equiJoinCondition = JoinConditionSymbolsExtractor.extract(plan.joinCondition());
        Symbol smallerRelationColumn = getOnlyElement(equiJoinCondition.get(getOnlyElement(smallerSide.relationNames())));
        Symbol largerRelationColumn = getOnlyElement(equiJoinCondition.get(getOnlyElement(largerSide.relationNames())));

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
        boolean lhsIsLookup = false;
        boolean rhsIsLookup = false;

        if (rhsIsLarger) {
            newLhs = lhs;
            newRhs = lookupJoin;
            rhsIsLookup = true;
        } else {
            newLhs = lookupJoin;
            newRhs = rhs;
            lhsIsLookup = true;
        }

        return new JoinPlan(
            newLhs,
            newRhs,
            plan.joinType(),
            plan.joinCondition(),
            plan.isFiltered(),
            plan.isRewriteFilterOnOuterJoinToInnerJoinDone(),
            plan.moveConstantJoinConditionRuleApplied(),
            lhsIsLookup,
            rhsIsLookup
        );
    }

    private static LogicalPlan createLookup(LogicalPlan smallerSide,
                                            Symbol smallerRelationColumn,
                                            LogicalPlan largerSide,
                                            Symbol largerRelationColumn,
                                            NodeContext nodeCtx,
                                            TransactionContext txnCtx) {

        var lookUpQuery = new SelectSymbol(
            new PlannedRelation(smallerSide),
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
        var largerSideWithLookup = new Filter(largerSide, anyEqFunction);
        var smallerSidePruned = smallerSide.pruneOutputsExcept(List.of(smallerRelationColumn));
        var eval = Eval.create(smallerSidePruned, List.of(smallerRelationColumn));
        var smallerSideIdLookup = new RootRelationBoundary(eval);
        Map<LogicalPlan, SelectSymbol> subQueries = Map.of(smallerSideIdLookup, lookUpQuery);
        return MultiPhase.createIfNeeded(subQueries, largerSideWithLookup);
    }

    private static boolean applyOptimization(long lhsNumDocs, long rhsNumDocs) {
        if ((lhsNumDocs == -1L || rhsNumDocs == -1L) ||
            (lhsNumDocs == 0 || rhsNumDocs == 0) ||
            lhsNumDocs == rhsNumDocs) {
            return false;
        }
        double smaller, larger;
        if (lhsNumDocs > rhsNumDocs) {
            larger = lhsNumDocs;
            smaller = rhsNumDocs;

        } else {
            larger = rhsNumDocs;
            smaller = lhsNumDocs;
        }
        if (larger < 10_000) {
            return false;
        }
        double difference = larger - smaller;
        double ratio = larger / smaller;
        return difference >= 5000 && ratio > 1.0;
    }
}
