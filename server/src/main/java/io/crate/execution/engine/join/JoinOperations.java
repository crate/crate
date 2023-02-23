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

package io.crate.execution.engine.join;

import io.crate.analyze.JoinRelation;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.JoinType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class JoinOperations {

    private JoinOperations() {
    }

    public static boolean isMergePhaseNeeded(Collection<String> executionNodes,
                                             ResultDescription resultDescription,
                                             boolean isDistributed) {
        return isDistributed ||
               resultDescription.hasRemainingLimitOrOffset() ||
               !resultDescription.nodeIds().equals(executionNodes);
    }

    public static MergePhase buildMergePhaseForJoin(PlannerContext plannerContext,
                                                    ResultDescription resultDescription,
                                                    Collection<String> executionNodes) {
        List<Projection> projections = Collections.emptyList();
        if (resultDescription.hasRemainingLimitOrOffset()) {
            projections = Collections.singletonList(ProjectionBuilder.limitAndOffsetOrEvalIfNeeded(
                resultDescription.limit(),
                resultDescription.offset(),
                resultDescription.numOutputs(),
                resultDescription.streamOutputs()
            ));
        }

        return new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "join-merge",
            resultDescription.nodeIds().size(),
            1,
            executionNodes,
            resultDescription.streamOutputs(),
            projections,
            DistributionInfo.DEFAULT_SAME_NODE,
            resultDescription.orderBy()
        );
    }

    /**
     * Creates an {@link EvalProjection} to ensure that the join output symbols are emitted in the original order as
     * a possible outer operator (e.g. GROUP BY) is relying on the order.
     * The order could have been changed due to the switch-table optimizations
     *
     * @param outputs       List of join output symbols in their original order.
     * @param joinOutputs   List of join output symbols after possible re-ordering due optimizations.
     */
    public static Projection createJoinProjection(List<Symbol> outputs, List<Symbol> joinOutputs) {
        List<Symbol> projectionOutputs = InputColumns.create(
            outputs,
            new InputColumns.SourceSymbols(joinOutputs));
        return new EvalProjection(projectionOutputs);
    }

    private static class Visitor extends AnalyzedRelationVisitor<List<JoinPair>, AnalyzedRelation> {

        @Override
        public AnalyzedRelation visitDocTableRelation(DocTableRelation relation, List<JoinPair> context) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitJoinRelation(JoinRelation joinRelation, List<JoinPair> context) {
            var left = joinRelation.left().accept(this, context);
            var right = joinRelation.right().accept(this, context);
            return null;
        }
    }



    public static LinkedHashMap<Set<RelationName>, JoinPair> buildRelationsToJoinPairsMap(List<JoinPair> joinPairs) {
        LinkedHashMap<Set<RelationName>, JoinPair> joinPairsMap = new LinkedHashMap<>();
        for (JoinPair joinPair : joinPairs) {
            if (joinPair.condition() == null) {
                continue;
            }
            // Left/right sides of a join pair have to be consistent with the key set, we ensure that left side is always first in the set.
            RelationName left = joinPair.left();
            RelationName right = joinPair.right();
            JoinPair prevPair = joinPairsMap.put(
                new LinkedHashSet<>(List.of(left, right)), joinPair
            );
            if (prevPair != null) {
                throw new IllegalStateException("joinPairs contains duplicate: " + joinPair + " matches " + prevPair);
            }
        }
        return joinPairsMap;
    }

    /**
     * Converts any implicit join conditions of the WHERE clause to explicit {@link JoinPair}.
     * Every join condition that gets to be converted is removed from the {@code splitQueries}
     *
     * @param explicitJoinPairs The explicitJoinPairs as originally written in the query
     * @param splitQueries      The remaining queries of the WHERE clause split by involved relations
     * @return the new list of {@link JoinPair}
     */
    public static List<JoinPair> convertImplicitJoinConditionsToJoinPairs(List<JoinPair> explicitJoinPairs,
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
}
