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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.FieldResolver;
import io.crate.common.collections.Maps;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.consumer.RelationNameCollector;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.EquiJoinDetector;
import io.crate.planner.operators.JoinConditionSymbolsExtractor;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.LookupJoin;
import io.crate.planner.operators.Rename;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class OptimizeJoinWithLookup implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern = typeOf(JoinPlan.class).with(j ->
        j.isLookUpJoinRuleApplied() == false &&
            // only non-nested inner-equi-joins
            j.joinType() == JoinType.INNER &&
            j.joinCondition() != null &&
            EquiJoinDetector.isEquiJoin(j.joinCondition()) &&
            j.lhs().getRelationNames().size() == 1 &&
            j.rhs().getRelationNames().size() == 1
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

        var lhs = resolvePlan.apply(plan.lhs());
        var rhs = resolvePlan.apply(plan.rhs());

        var lhsStats = planStats.get(lhs).numDocs();
        var rhsStats = planStats.get(rhs).numDocs();

        if (applyOptimization(lhsStats, rhsStats) == false) {
            return null;
        }

        var visitor = new Visitor(resolvePlan);

        var lhsContext = new Context();
        lhs.accept(visitor, lhsContext);

        var rhsContext = new Context();
        rhs.accept(visitor, rhsContext);

        Map<ScopedSymbol, FieldResolver> fieldResolvers = Maps.concat(lhsContext.fieldResolvers, rhsContext.fieldResolvers);
        Map<RelationName, Symbol> equiJoinCondition = extractEquiJoinCondition(plan.joinCondition(), fieldResolvers);

        AbstractTableRelation<?> smallerRelation;
        AbstractTableRelation<?> largerRelation;
        LogicalPlan largerSide;

        boolean rhsIsLarger = false;

        if (lhsStats < rhsStats) {
            smallerRelation = lhsContext.relation;
            largerRelation = rhsContext.relation;
            largerSide = rhs;
            rhsIsLarger = true;
        } else {
            smallerRelation = rhsContext.relation;
            largerRelation = lhsContext.relation;
            largerSide = lhs;
        }

        Symbol smallerRelationColumn = equiJoinCondition.get(smallerRelation.relationName());
        Symbol largerRelationColumn = equiJoinCondition.get(largerRelation.relationName());

        LogicalPlan lookupJoin = LookupJoin.create(
            smallerRelation,
            smallerRelationColumn,
            largerRelation,
            largerRelationColumn,
            nodeCtx,
            txnCtx
        );

        if (largerSide instanceof Rename rename) {
            lookupJoin = rename.replaceSources(List.of(lookupJoin));
        }

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

    private static boolean applyOptimization(long a, long b) {
        if ((a == -1L || b == -1L) ||
            (a == 0 || b == 0) ||
            a == b) {
            return false;
        }
        long smaller, larger;
        if (a > b) {
            larger = a;
            smaller = b;

        } else {
            larger = b;
            smaller = a;
        }
        if (larger < 10_000) {
            return false;
        }
        return larger / smaller >= 10;
    }

    private Map<RelationName, Symbol> extractEquiJoinCondition(Symbol joinCondition, Map<ScopedSymbol, FieldResolver> fieldResolvers) {
        var result = new HashMap<RelationName, Symbol>();
        for (var entry : JoinConditionSymbolsExtractor.extract(joinCondition).entrySet()) {
            var relationName = entry.getKey();
            var symbol = getOnlyElement(entry.getValue());
            if (symbol instanceof ScopedSymbol scopedSymbol) {
                FieldResolver fieldResolver = fieldResolvers.get(scopedSymbol);
                if (fieldResolver != null) {
                    symbol = fieldResolver.resolveField(scopedSymbol);
                    if (symbol != null) {
                        relationName = getOnlyElement(RelationNameCollector.collect(symbol));
                    }
                }
            }
            result.put(relationName, symbol);
        }
        return result;
    }

    private static class Context {
        AbstractTableRelation<?> relation = null;
        Map<ScopedSymbol, FieldResolver> fieldResolvers = new HashMap<>();
    }

    private static class Visitor extends LogicalPlanVisitor<Context, Void> {

        final UnaryOperator<LogicalPlan> resolvePlan;

        public Visitor(UnaryOperator<LogicalPlan> resolvePlan) {
            this.resolvePlan = resolvePlan;
        }

        @Override
        public Void visitPlan(LogicalPlan plan, Context context) {
            for (LogicalPlan source : plan.sources()) {
                source.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitGroupReference(GroupReference g, Context context) {
            return resolvePlan.apply(g).accept(this, context);
        }

        @Override
        public Void visitRename(Rename rename, Context context) {
            for (Symbol output : rename.outputs()) {
                if (output instanceof ScopedSymbol scopedSymbol) {
                    context.fieldResolvers.put(scopedSymbol, rename);
                }
            }
            rename.source().accept(this, context);
            return null;
        }

        @Override
        public Void visitCollect(Collect collect, Context context) {
            assert context.relation == null : "There is more than one relation in this Logical Plan tree";
            context.relation = collect.relation();
            return null;
        }
    }
}
