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

import com.carrotsearch.hppc.ObjectIntHashMap;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.metadata.RelationName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class which is used by the {@link JoinPlanBuilder} for the building of
 * the join tree, which helps to find the optimal ordering of the tables.
 */
final class JoinOrdering {

    private JoinOrdering() {
    }

    static Collection<RelationName> getOrderedRelationNames(Collection<RelationName> sourceRelations,
                                                            Set<? extends Set<RelationName>> explicitJoinConditions,
                                                            Set<? extends Set<RelationName>> implicitJoinConditions) {
        if (sourceRelations.size() == 2) {
            return sourceRelations;
        }
        if (explicitJoinConditions.isEmpty() && implicitJoinConditions.isEmpty()) {
            return sourceRelations;
        }
        return orderByJoinConditions(
            sourceRelations,
            explicitJoinConditions,
            implicitJoinConditions);
    }

    /**
     * Returns a the relation re-ordered to apply join conditions further down in the tree.
     * <p>
     * (Assuming the relations are consumed from left to right to build a tree of two-relations join nodes)
     *
     * @param relations               all relations, e.g. [t1, t2, t3, t3]
     * @param explicitJoinedRelations contains all relation pairs that have an explicit join condition
     *                                e.g. {{t1, t2}, {t2, t3}}
     * @param implicitJoinedRelations contains all relations pairs that have an implicit join condition
     *                                e.g. {{t1, t2}, {t2, t3}}
     */
    static Collection<RelationName> orderByJoinConditions(Collection<RelationName> relations,
                                                          Set<? extends Set<RelationName>> explicitJoinedRelations,
                                                          Set<? extends Set<RelationName>> implicitJoinedRelations) {
        List<Set<RelationName>> pairsWithJoinConditions = new ArrayList<>(explicitJoinedRelations);
        pairsWithJoinConditions.addAll(implicitJoinedRelations);

        ObjectIntHashMap<RelationName> occurrences =
            getOccurrencesInJoinConditions(relations.size(), explicitJoinedRelations, implicitJoinedRelations);

        Set<RelationName> firstJoinPair = findAndRemoveFirstJoinPair(occurrences, pairsWithJoinConditions);
        assert firstJoinPair != null : "firstJoinPair should not be null";
        LinkedHashSet<RelationName> bestOrder = new LinkedHashSet<>(firstJoinPair);

        buildBestOrderByJoinConditions(pairsWithJoinConditions, bestOrder);
        bestOrder.addAll(relations);
        return bestOrder;
    }

    private static ObjectIntHashMap<RelationName> getOccurrencesInJoinConditions(
        int numberOfRelations,
        Set<? extends Set<RelationName>> explicitJoinedRelations,
        Set<? extends Set<RelationName>> implicitJoinedRelations) {

        ObjectIntHashMap<RelationName> occurrences = new ObjectIntHashMap<>(numberOfRelations);
        explicitJoinedRelations.forEach(o -> o.forEach(qName -> occurrences.putOrAdd(qName, 1, 1)));
        implicitJoinedRelations.forEach(o -> o.forEach(qName -> occurrences.putOrAdd(qName, 1, 1)));
        return occurrences;
    }

    @VisibleForTesting
    static Set<RelationName> findAndRemoveFirstJoinPair(ObjectIntHashMap<RelationName> occurrences,
                                                        Collection<Set<RelationName>> joinPairs) {
        Iterator<Set<RelationName>> setsIterator = joinPairs.iterator();
        while (setsIterator.hasNext()) {
            Set<RelationName> set = setsIterator.next();
            for (RelationName name : set) {
                int count = occurrences.getOrDefault(name, 0);
                if (count > 1) {
                    setsIterator.remove();
                    return set;
                }
            }
        }
        return joinPairs.iterator().next();
    }

    private static void buildBestOrderByJoinConditions(List<Set<RelationName>> sets, LinkedHashSet<RelationName> bestOrder) {
        Iterator<Set<RelationName>> setsIterator = sets.iterator();
        while (setsIterator.hasNext()) {
            Set<RelationName> set = setsIterator.next();
            for (RelationName name : set) {
                if (bestOrder.contains(name)) {
                    bestOrder.addAll(set);
                    setsIterator.remove();
                    setsIterator = sets.iterator();
                    break;
                }
            }
        }

        // Add the rest of the relations to the end of the collection
        sets.forEach(bestOrder::addAll);
    }
}
