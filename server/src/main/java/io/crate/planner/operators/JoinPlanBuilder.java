/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.operators;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Lists2;
import io.crate.execution.engine.join.JoinOperations;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.OuterColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.RelationName;
import io.crate.planner.SubqueryPlanner.SubQueries;
import io.crate.planner.node.dql.join.JoinType;

/**
 * A logical plan builder for `Join` operations. It will also evaluate which `Join` operator to use and build the
 * corresponding `LogicalPlan`.
 * <p>
 * We currently support the {@link NestedLoopJoin} and {@link HashJoin} operators (the hash join operator is
 * enabled by the {@link io.crate.metadata.settings.session.SessionSettingRegistry#HASH_JOIN_KEY} setting and its
 * application is mandated by {@link EquiJoinDetector}).
 */
public class JoinPlanBuilder {

    static LogicalPlan buildJoinTree(List<AnalyzedRelation> from,
                                     Symbol whereClause,
                                     List<JoinPair> joinPairs,
                                     SubQueries subQueries,
                                     Function<AnalyzedRelation, LogicalPlan> plan,
                                     boolean hashJoinEnabled,
                                     LogicalPlanIdAllocator logicalPlanIdAllocator) {
        if (from.size() == 1) {
            LogicalPlan source = subQueries.applyCorrelatedJoin(plan.apply(from.get(0)), logicalPlanIdAllocator);
            return Filter.create(source, whereClause, logicalPlanIdAllocator.nextId());
        }
        Map<Set<RelationName>, Symbol> queryParts = QuerySplitter.split(whereClause);
        List<JoinPair> allJoinPairs = JoinOperations.convertImplicitJoinConditionsToJoinPairs(joinPairs, queryParts);
        boolean optimizeOrder = true;
        for (var joinPair : allJoinPairs) {
            if (hasAdditionalDependencies(joinPair)) {
                optimizeOrder = false;
                break;
            }
        }
        LinkedHashMap<Set<RelationName>, JoinPair> joinPairsByRelations = JoinOperations.buildRelationsToJoinPairsMap(allJoinPairs);
        Iterator<RelationName> it;
        if (optimizeOrder) {
            Collection<RelationName> orderedRelationNames = JoinOrdering.getOrderedRelationNames(
                Lists2.map(from, AnalyzedRelation::relationName),
                joinPairsByRelations.keySet(),
                queryParts.keySet()
            );
            it = orderedRelationNames.iterator();
        } else {
            it = Lists2.map(from, AnalyzedRelation::relationName).iterator();
        }

        final RelationName lhsName = it.next();
        final RelationName rhsName = it.next();
        Set<RelationName> joinNames = new HashSet<>();
        joinNames.add(lhsName);
        joinNames.add(rhsName);

        JoinPair joinLhsRhs = joinPairsByRelations.remove(joinNames);
        final JoinType joinType;
        final Symbol joinCondition;
        if (joinLhsRhs == null) {
            joinType = JoinType.CROSS;
            joinCondition = null;
        } else {
            joinType = maybeInvertPair(rhsName, joinLhsRhs);
            joinCondition = joinLhsRhs.condition();
        }

        Map<RelationName, AnalyzedRelation> sources = from.stream()
            .collect(Collectors.toMap(AnalyzedRelation::relationName, rel -> rel));
        AnalyzedRelation lhs = sources.get(lhsName);
        AnalyzedRelation rhs = sources.get(rhsName);
        LogicalPlan lhsPlan = plan.apply(lhs);
        LogicalPlan rhsPlan = plan.apply(rhs);
        Symbol query = removeParts(queryParts, lhsName, rhsName);
        LogicalPlan joinPlan;

        if (containsOuterColumnFromRelation(joinCondition, rhs)) {
            joinPlan = createCorrelatedJoinWithFilter(
                lhsPlan,
                rhsPlan,
                joinType,
                joinCondition,
                lhs,
                subQueries,
                query,
                logicalPlanIdAllocator);
        } else {
            lhsPlan = subQueries.applyCorrelatedJoin(lhsPlan, logicalPlanIdAllocator);
            joinPlan = createJoinPlan(
                lhsPlan,
                rhsPlan,
                joinType,
                joinCondition,
                lhs,
                query,
                logicalPlanIdAllocator);
        }

        joinPlan = Filter.create(joinPlan, query, logicalPlanIdAllocator.nextId());
        while (it.hasNext()) {
            AnalyzedRelation nextRel = sources.get(it.next());
            joinPlan = joinWithNext(
                plan,
                joinPlan,
                nextRel,
                joinNames,
                joinPairsByRelations,
                queryParts,
                lhs,
                hashJoinEnabled,
                logicalPlanIdAllocator
            );
            joinNames.add(nextRel.relationName());
        }
        if (!queryParts.isEmpty()) {
            joinPlan = Filter.create(joinPlan, AndOperator.join(queryParts.values()), logicalPlanIdAllocator.nextId());
            queryParts.clear();
        }
        assert joinPairsByRelations.isEmpty() : "Must've applied all joinPairs";
        return joinPlan;
    }

    private static boolean containsOuterColumnFromRelation(@Nullable Symbol joinCondition, AnalyzedRelation relation) {
        if (joinCondition == null) {
            return false;
        }
        return containsOuterColumnInCorrelatedSelect(joinCondition, relation);
    }

    private static boolean containsOuterColumnInCorrelatedSelect(Symbol symbol, AnalyzedRelation relation) {
        return SymbolVisitors.any(x -> x instanceof SelectSymbol s &&
                                       s.isCorrelated() &&
                                       containsOuterColumn(s, relation), symbol);
    }

    private static boolean containsOuterColumn(SelectSymbol select, AnalyzedRelation relation) {
        for (var output : select.relation().outputs()) {
            if (SymbolVisitors.any(x -> x instanceof OuterColumn o && o.relation().equals(relation), output)) {
                return true;
            }
        }
        return false;
    }

    private static LogicalPlan createCorrelatedJoinWithFilter(
        LogicalPlan lhsPlan,
        LogicalPlan rhsPlan,
        JoinType joinType,
        Symbol joinCondition,
        AnalyzedRelation lhs,
        SubQueries subQueries,
        Symbol query,
        LogicalPlanIdAllocator logicalPlanIdAllocator
    ) {
        var validJoinConditions = new ArrayList<Symbol>();
        var joinConditionsForFilters = new ArrayList<Symbol>();

        for (var symbol : QuerySplitter.split(joinCondition).values()) {
            if (SymbolVisitors.any(s -> s instanceof SelectSymbol x && x.isCorrelated(), symbol)) {
                joinConditionsForFilters.add(symbol);
            } else {
                validJoinConditions.add(symbol);
            }
        }

        var joinPlan = createJoinPlan(
            lhsPlan,
            rhsPlan,
            joinType,
            AndOperator.join(validJoinConditions),
            lhs,
            query,
            logicalPlanIdAllocator);

        joinPlan = subQueries.applyCorrelatedJoin(joinPlan, logicalPlanIdAllocator);
        joinPlan = Filter.create(joinPlan, AndOperator.join(joinConditionsForFilters), logicalPlanIdAllocator.nextId());;

        return joinPlan;
    }

    private static boolean hasAdditionalDependencies(JoinPair joinPair) {
        Symbol condition = joinPair.condition();
        if (condition == null) {
            return false;
        }
        boolean[] hasAdditionalDependencies = {false};
        FieldsVisitor.visitFields(condition, scopedSymbol -> {
            RelationName relation = scopedSymbol.relation();
            if (!relation.equals(joinPair.left()) && !relation.equals(joinPair.right())) {
                hasAdditionalDependencies[0] = true;
            }
        });
        return hasAdditionalDependencies[0];
    }

    private static LogicalPlan createJoinPlan(LogicalPlan lhsPlan,
                                              LogicalPlan rhsPlan,
                                              JoinType joinType,
                                              Symbol joinCondition,
                                              AnalyzedRelation lhs,
                                              Symbol query,
                                              LogicalPlanIdAllocator logicalPlanIdAllocator) {
        return new NestedLoopJoin(
            lhsPlan,
            rhsPlan,
            joinType,
            joinCondition,
            !query.symbolType().isValueSymbol(),
            lhs,
            false,
            logicalPlanIdAllocator.nextId());
    }

    private static JoinType maybeInvertPair(RelationName rhsName, JoinPair pair) {
        // A matching joinPair for two relations is retrieved using pairByQualifiedNames.remove(setOf(a, b))
        // This returns a pair for both cases: (a ⋈ b) and (b ⋈ a) -> invert joinType to execute correct join
        // Note that this can only happen if a re-ordering optimization happened, otherwise the joinPair would always
        // be in the correct format.
        if (pair.right().equals(rhsName)) {
            return pair.joinType();
        }
        return pair.joinType().invert();
    }

    private static LogicalPlan joinWithNext(Function<AnalyzedRelation, LogicalPlan> plan,
                                            LogicalPlan source,
                                            AnalyzedRelation nextRel,
                                            Set<RelationName> joinNames,
                                            Map<Set<RelationName>, JoinPair> joinPairs,
                                            Map<Set<RelationName>, Symbol> queryParts,
                                            AnalyzedRelation leftRelation,
                                            boolean hashJoinEnabled,
                                            LogicalPlanIdAllocator logicalPlanIdAllocator) {
        RelationName nextName = nextRel.relationName();

        JoinPair joinPair = removeMatch(joinPairs, joinNames, nextName);
        final JoinType type;
        final Symbol condition;
        if (joinPair == null) {
            type = JoinType.CROSS;
            condition = null;
        } else {
            type = maybeInvertPair(nextName, joinPair);
            condition = joinPair.condition();
        }

        LogicalPlan nextPlan = plan.apply(nextRel);
        Symbol query = AndOperator.join(
            Stream.of(
                removeMatch(queryParts, joinNames, nextName),
                queryParts.remove(Collections.singleton(nextName)))
                .filter(Objects::nonNull).iterator()
        );
        return Filter.create(
            createJoinPlan(
                source,
                nextPlan,
                type,
                condition,
                leftRelation,
                query,
                logicalPlanIdAllocator),
            query,
            logicalPlanIdAllocator.nextId()
        );
    }

    private static Symbol removeParts(Map<Set<RelationName>, Symbol> queryParts, RelationName lhsName, RelationName rhsName) {
        // query parts can affect a single relation without being pushed down in the outer-join case
        Symbol left = queryParts.remove(Collections.singleton(lhsName));
        Symbol right = queryParts.remove(Collections.singleton(rhsName));
        Symbol both = queryParts.remove(Set.of(lhsName, rhsName));
        return AndOperator.join(
            Stream.of(left, right, both).filter(Objects::nonNull).iterator()
        );
    }

    @Nullable
    private static <V> V removeMatch(Map<Set<RelationName>, V> valuesByNames, Set<RelationName> names, RelationName nextName) {
        for (RelationName name : names) {
            V v = valuesByNames.remove(Set.of(name, nextName));
            if (v != null) {
                return v;
            }
        }
        return null;
    }
}
