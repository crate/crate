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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.SubqueryPlanner.SubQueries;
import io.crate.sql.tree.JoinType;

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
                                     Function<AnalyzedRelation, LogicalPlan> plan) {
        if (from.size() == 1) {
            LogicalPlan source = subQueries.applyCorrelatedJoin(plan.apply(from.get(0)));
            return Filter.create(source, whereClause);
        }
        Map<Set<RelationName>, Symbol> queryParts = QuerySplitter.split(whereClause);
        List<JoinPair> allJoinPairs = convertImplicitJoinConditionsToJoinPairs(joinPairs, queryParts);
        LinkedHashMap<Set<RelationName>, JoinPair> joinPairsByRelations = buildRelationsToJoinPairsMap(allJoinPairs);

        Map<RelationName, AnalyzedRelation> relationsFromClause = new HashMap<>();
        List<RelationName> relationNamesFromClause = new ArrayList<>();

        for (AnalyzedRelation analyzedRelation : from) {
            RelationName relationName = analyzedRelation.relationName();
            relationNamesFromClause.add(relationName);
            relationsFromClause.put(relationName, analyzedRelation);
        }

        final RelationName lhsName;
        final RelationName rhsName;

        final JoinType joinType;
        final Symbol joinCondition;
        Set<RelationName> joinNames = new LinkedHashSet<>();

        // Outer-joins must be created before the implicit joins because the are
        // binding stronger since they are order-dependent.
        if (containsOuterJoins(allJoinPairs)) {
            JoinPair joinPair = allJoinPairs.removeFirst();
            lhsName = joinPair.left();
            rhsName = joinPair.right();
            relationNamesFromClause.remove(lhsName);
            relationNamesFromClause.remove(rhsName);
            joinNames.add(lhsName);
            joinNames.add(rhsName);
            joinType = joinPair.joinType();
            joinCondition = joinPair.condition();
            joinPairsByRelations.remove(joinNames);
        } else {
            // All joins are inner or cross joins sp they are not order-dependent.
            // Therefore, we build the join in the original order sequentially.
            lhsName = relationNamesFromClause.removeFirst();
            rhsName = relationNamesFromClause.removeFirst();
            joinNames.add(lhsName);
            joinNames.add(rhsName);
            JoinPair joinLhsRhs = joinPairsByRelations.remove(joinNames);
            if (joinLhsRhs == null) {
                joinType = JoinType.CROSS;
                joinCondition = null;
            } else {
                joinType = joinLhsRhs.joinType();
                joinCondition = joinLhsRhs.condition();
            }
        }

        final AnalyzedRelation lhs = relationsFromClause.get(lhsName);
        final AnalyzedRelation rhs = relationsFromClause.get(rhsName);

        var correlatedSubQueriesFromJoin = extractCorrelatedSubQueries(joinCondition);
        var validJoinConditions = AndOperator.join(correlatedSubQueriesFromJoin.remainder(), null);
        var correlatedSubQueriesFromWhereClause = extractCorrelatedSubQueries(removeParts(queryParts, lhsName, rhsName));
        var validWhereConditions = AndOperator.join(correlatedSubQueriesFromWhereClause.remainder());

        boolean isFiltered = validWhereConditions.symbolType().isValueSymbol() == false;

        LogicalPlan joinPlan = new JoinPlan(
            plan.apply(lhs),
            plan.apply(rhs),
            joinType,
            validJoinConditions,
            isFiltered,
            false,
            false,
            AbstractJoinPlan.LookUpJoin.NONE);

        joinPlan = Filter.create(joinPlan, validWhereConditions);

        for (RelationName relationName : relationNamesFromClause) {
            AnalyzedRelation nextRel = relationsFromClause.get(relationName);
            joinPlan = joinWithNext(
                plan,
                joinPlan,
                nextRel,
                joinNames,
                joinPairsByRelations,
                queryParts
            );
            joinNames.add(nextRel.relationName());
        }
        joinPlan = subQueries.applyCorrelatedJoin(joinPlan);
        if (!queryParts.isEmpty()) {
            joinPlan = Filter.create(joinPlan, AndOperator.join(queryParts.values()));
            queryParts.clear();
        }
        joinPlan = Filter.create(joinPlan, AndOperator.join(correlatedSubQueriesFromJoin.correlatedSubQueries()));
        joinPlan = Filter.create(joinPlan, AndOperator.join(correlatedSubQueriesFromWhereClause.correlatedSubQueries()));
        assert joinPairsByRelations.isEmpty() : "Must've applied all joinPairs: " + joinPairsByRelations;
        return joinPlan;
    }

    private static boolean containsOuterJoins(List<JoinPair> joinPairs) {
        for (JoinPair joinPair : joinPairs) {
            JoinType jt = joinPair.joinType();
            if (jt.isOuter()) {
                return true;
            }
        }
        return false;
    }

    private static LogicalPlan joinWithNext(Function<AnalyzedRelation, LogicalPlan> plan,
                                            LogicalPlan source,
                                            AnalyzedRelation nextRel,
                                            Set<RelationName> joinNames,
                                            Map<Set<RelationName>, JoinPair> joinPairs,
                                            Map<Set<RelationName>, Symbol> queryParts) {
        RelationName nextName = nextRel.relationName();

        JoinPair joinPair = removeMatch(joinPairs, joinNames, nextName);
        final JoinType type;
        ArrayList<Symbol> conditions = new ArrayList<>();
        if (joinPair == null) {
            type = JoinType.CROSS;
        } else {
            type = joinPair.joinType();
            if (joinPair.condition() != null) {
                conditions.add(joinPair.condition());
            }
            // There could be additional joinPairs that are not directly connected to the current joinPair,
            // when there are WHERE clauses referencing to multiple relations of a previous join which was converted
            // into a joinPair by JoinOperations.convertImplicitJoinConditionsToJoinPairs(...)
            // Example:
            //  t1. JOIN t2 ON t1.x = t2.y JOIN t3 ON t1.x = t3.z WHERE t2.y = t3.z
            //  -> JoinPair(t1, t2)     <- 1st JOIN
            //  -> JoinPair(t1, t3)     <- 2nd JOIN
            //  -> JoinPair(t2, t3)     <- additional joinPair condition, must be added to the 2nd JOIN
            JoinPair additionalJoinPair;
            while ((additionalJoinPair = removeMatch(joinPairs, joinNames, nextName)) != null) {
                var additionalCondition = additionalJoinPair.condition();
                if (additionalCondition != null) {
                    conditions.add(additionalCondition);
                }
            }
        }

        LogicalPlan nextPlan = plan.apply(nextRel);
        Symbol query = AndOperator.join(
            Stream.of(
                removeMatch(queryParts, joinNames, nextName),
                queryParts.remove(Collections.singleton(nextName)))
                .filter(Objects::nonNull).iterator()
        );
        boolean isFiltered = query.symbolType().isValueSymbol() == false;

        final LogicalPlan lhs;
        final LogicalPlan rhs;

        // For outer joins lhs/rhs order has to be defined as in the query
        // which is preserved in the join pair
        if (type.isOuter() && joinPair.left().equals(nextRel.relationName())) {
            lhs = nextPlan;
            rhs = source;
        } else {
            lhs = source;
            rhs = nextPlan;
        }

        var joinPlan = new JoinPlan(
            lhs,
            rhs,
            type,
            AndOperator.join(conditions, null),
            isFiltered,
            false,
            false,
            AbstractJoinPlan.LookUpJoin.NONE
        );
        return Filter.create(joinPlan, query);
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

    /**
     * Converts any implicit join conditions of the WHERE clause to explicit {@link JoinPair}.
     * Every join condition that gets to be converted is removed from the {@code implicitJoinConditions}
     *
     * @param explicitJoinPairs The explicitJoinPairs as originally written in the query
     * @param implicitJoinConditions      The remaining queries of the WHERE clause split by involved relations
     * @return the new list of {@link JoinPair}
     */
    static List<JoinPair> convertImplicitJoinConditionsToJoinPairs(List<JoinPair> explicitJoinPairs,
                                                                   Map<Set<RelationName>, Symbol> implicitJoinConditions) {
        ArrayList<JoinPair> newJoinPairs = new ArrayList<>(explicitJoinPairs);
        Iterator<Map.Entry<Set<RelationName>, Symbol>> queryIterator = implicitJoinConditions.entrySet().iterator();

        while (queryIterator.hasNext()) {
            Map.Entry<Set<RelationName>, Symbol> queryEntry = queryIterator.next();
            Set<RelationName> relations = queryEntry.getKey();
            // If more than 2 relations are involved it cannot be converted to a JoinPair
            if (relations.size() == 2) {
                Symbol implicitJoinCondition = queryEntry.getValue();
                boolean explicitJoinPairExists = false;
                for (JoinPair explicitJoinPair : explicitJoinPairs) {
                    if (relations.equals(Set.of(explicitJoinPair.left(), explicitJoinPair.right()))) {
                        explicitJoinPairExists = true;
                        break;
                    }
                }
                if (explicitJoinPairExists == false) {
                    Iterator<RelationName> namesIter = relations.iterator();
                    newJoinPairs.add(JoinPair.of(namesIter.next(), namesIter.next(), JoinType.INNER, implicitJoinCondition));
                    queryIterator.remove();
                }
            }
        }
        return newJoinPairs;
    }

    private static LinkedHashMap<Set<RelationName>, JoinPair> buildRelationsToJoinPairsMap(List<JoinPair> joinPairs) {
        LinkedHashMap<Set<RelationName>, JoinPair> joinPairsMap = new LinkedHashMap<>();
        for (JoinPair joinPair : joinPairs) {
            if (joinPair.condition() == null) {
                continue;
            }
            // Left/right sides of a join pair have to be consistent with the key set, we ensure that left side is always first in the set.
            JoinPair prevPair = joinPairsMap.put(new LinkedHashSet<>(List.of(joinPair.left(), joinPair.right())), joinPair);
            if (prevPair != null) {
                throw new IllegalStateException("joinPairs contains duplicate: " + joinPair + " matches " + prevPair);
            }
        }
        return joinPairsMap;
    }

    /**
     * Splits the given Symbol tree into a list of correlated sub-queries and a list of remaining symbols.
     */
    public static CorrelatedSubQueries extractCorrelatedSubQueries(@Nullable Symbol from) {
        if (from == null) {
            return new CorrelatedSubQueries(List.of(), List.of());
        }
        var values = QuerySplitter.split(from).values();
        var remainder = new ArrayList<Symbol>(values.size());
        var correlatedSubQueries = new ArrayList<Symbol>(values.size());
        for (var symbol : values) {
            if (symbol.any(s -> s instanceof SelectSymbol x && x.isCorrelated())) {
                correlatedSubQueries.add(symbol);
            } else {
                remainder.add(symbol);
            }
        }
        return new CorrelatedSubQueries(correlatedSubQueries, remainder);
    }

    public record CorrelatedSubQueries(List<Symbol> correlatedSubQueries, List<Symbol> remainder) {}
}
