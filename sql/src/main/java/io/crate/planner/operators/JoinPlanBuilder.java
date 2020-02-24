/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.operators;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.data.Row;
import io.crate.execution.engine.join.JoinOperations;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;
import io.crate.statistics.TableStats;
import org.elasticsearch.common.util.set.Sets;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A logical plan builder for `Join` operations. It will also evaluate which `Join` operator to use and build the
 * corresponding `LogicalPlan`.
 * <p>
 * We currently support the {@link NestedLoopJoin} and {@link HashJoin} operators (the hash join operator is
 * enabled by the {@link io.crate.metadata.settings.session.SessionSettingRegistry#HASH_JOIN_KEY} setting and its
 * application is mandated by {@link EquiJoinDetector}).
 */
public class JoinPlanBuilder {

    static LogicalPlan createNodes(MultiSourceSelect mss,
                                   WhereClause where,
                                   SubqueryPlanner subqueryPlanner,
                                   Functions functions,
                                   CoordinatorTxnCtx txnCtx,
                                   Set<PlanHint> hints,
                                   TableStats tableStats,
                                   Row params) {
        Map<Set<QualifiedName>, Symbol> queryParts = QuerySplitter.split(where.queryOrFallback());
        LinkedHashMap<Set<QualifiedName>, JoinPair> joinPairs =
            JoinOperations.buildRelationsToJoinPairsMap(
                JoinOperations.convertImplicitJoinConditionsToJoinPairs(mss.joinPairs(), queryParts));

        Collection<QualifiedName> orderedRelationNames;
        if (mss.sources().size() > 2) {
            orderedRelationNames = JoinOrdering.getOrderedRelationNames(
                mss.sources().keySet(),
                joinPairs.keySet(),
                queryParts.keySet()
            );
        } else {
            orderedRelationNames = mss.sources().keySet();
        }

        Iterator<QualifiedName> it = orderedRelationNames.iterator();

        final QualifiedName lhsName = it.next();
        final QualifiedName rhsName = it.next();
        Set<QualifiedName> joinNames = new HashSet<>();
        joinNames.add(lhsName);
        joinNames.add(rhsName);

        JoinPair joinLhsRhs = joinPairs.remove(joinNames);
        final JoinType joinType;
        final Symbol joinCondition;
        if (joinLhsRhs == null) {
            joinType = JoinType.CROSS;
            joinCondition = null;
        } else {
            joinType = maybeInvertPair(rhsName, joinLhsRhs);
            joinCondition = joinLhsRhs.condition();
        }

        AnalyzedRelation lhs = mss.sources().get(lhsName);
        AnalyzedRelation rhs = mss.sources().get(rhsName);
        LogicalPlan lhsPlan = LogicalPlanner.plan(lhs, subqueryPlanner, false, functions, txnCtx, hints, tableStats, params);
        LogicalPlan rhsPlan = LogicalPlanner.plan(rhs, subqueryPlanner, false, functions, txnCtx, hints, tableStats, params);
        Symbol query = removeParts(queryParts, lhsName, rhsName);
        LogicalPlan joinPlan = createJoinPlan(
            lhsPlan,
            rhsPlan,
            joinType,
            joinCondition,
            lhs,
            rhs,
            query,
            txnCtx.sessionContext(),
            tableStats);

        joinPlan = Filter.create(joinPlan, query);
        while (it.hasNext()) {
            AnalyzedRelation nextRel = mss.sources().get(it.next());
            joinPlan = joinWithNext(
                tableStats,
                hints,
                joinPlan,
                nextRel,
                joinNames,
                joinPairs,
                queryParts,
                subqueryPlanner,
                lhs,
                functions,
                txnCtx,
                params
            );
            joinNames.add(nextRel.getQualifiedName());
        }
        if (!queryParts.isEmpty()) {
            joinPlan = Filter.create(joinPlan, AndOperator.join(queryParts.values()));
            queryParts.clear();
        }
        assert joinPairs.isEmpty() : "Must've applied all joinPairs";

        return joinPlan;
    }

    private static LogicalPlan createJoinPlan(LogicalPlan lhsPlan,
                                              LogicalPlan rhsPlan,
                                              JoinType joinType,
                                              Symbol joinCondition,
                                              AnalyzedRelation lhs,
                                              AnalyzedRelation rhs,
                                              Symbol query,
                                              SessionContext sessionContext,
                                              TableStats tableStats) {
        if (isHashJoinPossible(joinType, joinCondition, sessionContext)) {
            return new HashJoin(
                lhsPlan,
                rhsPlan,
                joinCondition,
                rhs,
                tableStats);
        } else {
            return new NestedLoopJoin(
                lhsPlan,
                rhsPlan,
                joinType,
                joinCondition,
                !query.symbolType().isValueSymbol(),
                lhs);
        }
    }

    private static boolean isHashJoinPossible(JoinType joinType, Symbol joinCondition, SessionContext sessionContext) {
        return sessionContext.isHashJoinEnabled() && EquiJoinDetector.isHashJoinPossible(joinType, joinCondition);
    }

    private static JoinType maybeInvertPair(QualifiedName rhsName, JoinPair pair) {
        // A matching joinPair for two relations is retrieved using pairByQualifiedNames.remove(setOf(a, b))
        // This returns a pair for both cases: (a ⋈ b) and (b ⋈ a) -> invert joinType to execute correct join
        // Note that this can only happen if a re-ordering optimization happened, otherwise the joinPair would always
        // be in the correct format.
        if (pair.right().equals(rhsName)) {
            return pair.joinType();
        }
        return pair.joinType().invert();
    }

    private static LogicalPlan joinWithNext(TableStats tableStats,
                                            Set<PlanHint> hints,
                                            LogicalPlan source,
                                            AnalyzedRelation nextRel,
                                            Set<QualifiedName> joinNames,
                                            Map<Set<QualifiedName>, JoinPair> joinPairs,
                                            Map<Set<QualifiedName>, Symbol> queryParts,
                                            SubqueryPlanner subqueryPlanner,
                                            AnalyzedRelation leftRelation,
                                            Functions functions,
                                            CoordinatorTxnCtx txnCtx,
                                            @Nullable Row params) {
        QualifiedName nextName = nextRel.getQualifiedName();

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

        LogicalPlan nextPlan = LogicalPlanner.plan(nextRel, subqueryPlanner, false, functions, txnCtx, hints, tableStats, params);

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
                txnCtx.sessionContext(),
                tableStats),
            query
        );
    }

    private static Symbol removeParts(Map<Set<QualifiedName>, Symbol> queryParts, QualifiedName lhsName, QualifiedName rhsName) {
        // query parts can affect a single relation without being pushed down in the outer-join case
        Symbol left = queryParts.remove(Collections.singleton(lhsName));
        Symbol right = queryParts.remove(Collections.singleton(rhsName));
        Symbol both = queryParts.remove(Sets.newHashSet(lhsName, rhsName));
        return AndOperator.join(
            Stream.of(left, right, both).filter(Objects::nonNull).iterator()
        );
    }

    @Nullable
    private static <V> V removeMatch(Map<Set<QualifiedName>, V> valuesByNames, Set<QualifiedName> names, QualifiedName nextName) {
        for (QualifiedName name : names) {
            V v = valuesByNames.remove(Sets.newHashSet(name, nextName));
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    private static void addColumnsFrom(Iterable<? extends Symbol> symbols,
                                       Consumer<? super Symbol> consumer,
                                       AnalyzedRelation rel) {

        for (Symbol symbol : symbols) {
            addColumnsFrom(symbol, consumer, rel);
        }
    }

    private static void addColumnsFrom(@Nullable Symbol symbol, Consumer<? super Symbol> consumer, AnalyzedRelation rel) {
        if (symbol == null) {
            return;
        }
        FieldsVisitor.visitFields(symbol, f -> {
            if (f.relation().getQualifiedName().equals(rel.getQualifiedName())) {
                consumer.accept(f.pointer());
            }
        });
    }
}
