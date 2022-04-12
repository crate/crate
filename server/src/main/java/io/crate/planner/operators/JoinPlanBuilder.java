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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Lists2;
import io.crate.execution.engine.join.JoinOperations;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.node.dql.join.JoinType;
import javax.annotation.Nullable;
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

import static io.crate.planner.operators.EquiJoinDetector.isHashJoinPossible;

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
                                     Function<AnalyzedRelation, LogicalPlan> plan,
                                     boolean hashJoinEnabled) {
        if (from.size() == 1) {
            return Filter.create(plan.apply(from.get(0)), whereClause);
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
        LogicalPlan joinPlan = createJoinPlan(
            lhsPlan,
            rhsPlan,
            joinType,
            joinCondition,
            lhs,
            rhs,
            query,
            hashJoinEnabled
        );

        joinPlan = Filter.create(joinPlan, query);
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
                hashJoinEnabled
            );
            joinNames.add(nextRel.relationName());
        }
        if (!queryParts.isEmpty()) {
            joinPlan = Filter.create(joinPlan, AndOperator.join(queryParts.values()));
            queryParts.clear();
        }
        assert joinPairsByRelations.isEmpty() : "Must've applied all joinPairs";

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
                                              AnalyzedRelation rhs,
                                              Symbol query,
                                              boolean hashJoinEnabled) {
        if (hashJoinEnabled && isHashJoinPossible(joinType, joinCondition)) {
            return new HashJoin(
                lhsPlan,
                rhsPlan,
                joinCondition
            );
        } else {
            return new NestedLoopJoin(
                lhsPlan,
                rhsPlan,
                joinType,
                joinCondition,
                !query.symbolType().isValueSymbol(),
                lhs,
                false);
        }
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
                                            boolean hashJoinEnabled) {
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
                nextRel,
                query,
                hashJoinEnabled),
            query
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
