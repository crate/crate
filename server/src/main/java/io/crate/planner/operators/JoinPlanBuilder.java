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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Lists;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
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
        boolean optimizeOrder = true;
        for (var joinPair : allJoinPairs) {
            if (hasAdditionalDependencies(joinPair)) {
                optimizeOrder = false;
                break;
            }
        }
        LinkedHashMap<Set<RelationName>, JoinPair> joinPairsByRelations = buildRelationsToJoinPairsMap(allJoinPairs);
        Iterator<RelationName> it;
        if (optimizeOrder) {
            Collection<RelationName> orderedRelationNames = JoinOrdering.getOrderedRelationNames(
                Lists.map(from, AnalyzedRelation::relationName),
                joinPairsByRelations.keySet(),
                queryParts.keySet()
            );
            it = orderedRelationNames.iterator();
        } else {
            it = Lists.mapLazy(from, AnalyzedRelation::relationName).iterator();
        }

        final RelationName lhsName = it.next();
        final RelationName rhsName = it.next();
        Set<RelationName> joinNames = new LinkedHashSet<>();
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

        var correlatedSubQueriesFromJoin = extractCorrelatedSubQueries(joinCondition);
        var validJoinConditions = AndOperator.join(correlatedSubQueriesFromJoin.remainder(), null);
        var correlatedSubQueriesFromWhereClause = extractCorrelatedSubQueries(removeParts(queryParts, lhsName, rhsName));
        var validWhereConditions = AndOperator.join(correlatedSubQueriesFromWhereClause.remainder());

        Map<RelationName, AnalyzedRelation> sources = from.stream()
            .collect(Collectors.toMap(AnalyzedRelation::relationName, rel -> rel));
        AnalyzedRelation lhs = sources.get(lhsName);
        AnalyzedRelation rhs = sources.get(rhsName);

        boolean isFiltered = validWhereConditions.symbolType().isValueSymbol() == false;

        LogicalPlan newLhs = plan.apply(lhs);
        LogicalPlan newRhs = plan.apply(rhs);
        LogicalPlan joinPlan = new JoinPlan(
            AbstractJoinPlan.buildOutputs(newLhs.outputs(), newRhs.outputs(), joinType),
            newLhs,
            newRhs,
            joinType,
            validJoinConditions,
            isFiltered,
            false,
            false,
            false);

        joinPlan = Filter.create(joinPlan, validWhereConditions);
        while (it.hasNext()) {
            AnalyzedRelation nextRel = sources.get(it.next());
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
        assert joinPairsByRelations.isEmpty() : "Must've applied all joinPairs";
        return joinPlan;
    }

    private static boolean hasAdditionalDependencies(JoinPair joinPair) {
        Symbol condition = joinPair.condition();
        if (condition == null) {
            return false;
        }
        boolean[] hasAdditionalDependencies = {false};

        // Un-aliased tables
        RefVisitor.visitRefs(condition, ref -> {
            RelationName relationName = ref.ident().tableIdent();
            if (!relationName.equals(joinPair.left()) && !relationName.equals(joinPair.right())) {
                hasAdditionalDependencies[0] = true;
            }
        });

        // Aliased tables
        if (hasAdditionalDependencies[0] == false) {
            FieldsVisitor.visitFields(condition, scopedSymbol -> {
                RelationName relationName = scopedSymbol.relation();
                if (!relationName.equals(joinPair.left()) && !relationName.equals(joinPair.right())) {
                    hasAdditionalDependencies[0] = true;
                }
            });
        }

        return hasAdditionalDependencies[0];
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
                                            Map<Set<RelationName>, Symbol> queryParts) {
        RelationName nextName = nextRel.relationName();

        JoinPair joinPair = removeMatch(joinPairs, joinNames, nextName);
        final JoinType type;
        ArrayList<Symbol> conditions = new ArrayList<>();
        if (joinPair == null) {
            type = JoinType.CROSS;
        } else {
            type = maybeInvertPair(nextName, joinPair);
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
        var joinPlan = new JoinPlan(
            Lists.concat(source.outputs(), nextPlan.outputs()),
            source,
            nextPlan,
            type,
            AndOperator.join(conditions, null),
            isFiltered,
            false,
            false,
            false);
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
     * Every join condition that gets to be converted is removed from the {@code splitQueries}
     *
     * @param explicitJoinPairs The explicitJoinPairs as originally written in the query
     * @param splitQueries      The remaining queries of the WHERE clause split by involved relations
     * @return the new list of {@link JoinPair}
     */
    static List<JoinPair> convertImplicitJoinConditionsToJoinPairs(List<JoinPair> explicitJoinPairs,
                                                                          Map<Set<RelationName>, Symbol> splitQueries) {
        Iterator<Map.Entry<Set<RelationName>, Symbol>> queryIterator = splitQueries.entrySet().iterator();
        ArrayList<JoinPair> newJoinPairs = new ArrayList<>(explicitJoinPairs.size() + splitQueries.size());
        newJoinPairs.addAll(explicitJoinPairs);

        while (queryIterator.hasNext()) {
            Map.Entry<Set<RelationName>, Symbol> queryEntry = queryIterator.next();
            Set<RelationName> relations = queryEntry.getKey();

            if (relations.size() == 2) { // If more than 2 relations are involved it cannot be converted to a JoinPair
                Symbol implicitJoinCondition = queryEntry.getValue();
                JoinPair newJoinPair = null;
                int existingJoinPairIdx = -1;
                for (int i = 0; i < explicitJoinPairs.size(); i++) {
                    JoinPair joinPair = explicitJoinPairs.get(i);
                    if (relations.contains(joinPair.left()) && relations.contains(joinPair.right())) {
                        existingJoinPairIdx = i;
                        // If a JoinPair with the involved relations already exists then depending on the JoinType:
                        //  - INNER JOIN:  the implicit join condition can be "AND joined" with
                        //                 the existing explicit condition of the JoinPair.
                        //
                        //  - CROSS JOIN:  the implicit join condition becomes explicit and set on the JoinPair
                        //                 which becomes an INNER JOIN
                        //
                        //  - Outer Joins: Queries in the WHERE clause must be semantically applied after the outer join
                        //                 and cannot be merged with the conditions of the ON clause.
                        //
                        //  - SEMI/ANTI:   Queries in the WHERE clause must be semantically applied after the SEMI/ANTI
                        //                 join and cannot be merged with the generated SEMI/ANTI condition.
                        //
                        if (joinPair.joinType() == JoinType.INNER || joinPair.joinType() == JoinType.CROSS) {
                            newJoinPair = JoinPair.of(
                                joinPair.left(),
                                joinPair.right(),
                                JoinType.INNER,
                                mergeJoinConditions(joinPair.condition(), implicitJoinCondition));
                            queryIterator.remove();
                        } else {
                            newJoinPair = joinPair;
                        }
                    }
                }
                if (newJoinPair == null) {
                    Iterator<RelationName> namesIter = relations.iterator();
                    newJoinPair = JoinPair.of(namesIter.next(), namesIter.next(), JoinType.INNER, implicitJoinCondition);
                    queryIterator.remove();
                    newJoinPairs.add(newJoinPair);
                } else {
                    newJoinPairs.set(existingJoinPairIdx, newJoinPair);
                }
            }
        }
        return newJoinPairs;
    }

    private static Symbol mergeJoinConditions(@Nullable Symbol condition1, Symbol condition2) {
        if (condition1 == null) {
            return condition2;
        } else {
            return AndOperator.join(List.of(condition1, condition2));
        }
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
            if (SymbolVisitors.any(s -> s instanceof SelectSymbol x && x.isCorrelated(), symbol)) {
                correlatedSubQueries.add(symbol);
            } else {
                remainder.add(symbol);
            }
        }
        return new CorrelatedSubQueries(correlatedSubQueries, remainder);
    }

    public record CorrelatedSubQueries(List<Symbol> correlatedSubQueries, List<Symbol> remainder) {}
}
