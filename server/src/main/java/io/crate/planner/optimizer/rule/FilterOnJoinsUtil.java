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

package io.crate.planner.optimizer.rule;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Sets;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.AbstractJoinPlan;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.sql.tree.JoinType;

final class FilterOnJoinsUtil {

    private FilterOnJoinsUtil() {
    }

    static LogicalPlan getNewSource(@Nullable Symbol splitQuery, LogicalPlan source) {
        return splitQuery == null ? source : new Filter(source, splitQuery);
    }

    static LogicalPlan moveQueryBelowJoin(Symbol query, AbstractJoinPlan join) {
        if (!WhereClause.canMatch(query)) {
            return join.replaceSources(List.of(
                getNewSource(query, join.lhs()),
                getNewSource(query, join.rhs())
            ));
        }
        Map<Set<RelationName>, Symbol> splitQueries = QuerySplitter.split(query);
        final int initialParts = splitQueries.size();
        if (splitQueries.size() == 1 && splitQueries.keySet().iterator().next().size() > 1) {
            return null;
        }
        LogicalPlan lhs = join.lhs();
        LogicalPlan rhs = join.rhs();

        HashSet<RelationName> lhsRelations = new HashSet<>(lhs.getRelationNames());
        HashSet<RelationName> rhsRelations = new HashSet<>(rhs.getRelationNames());

        Symbol leftQuery = splitQueries.remove(lhsRelations);
        Symbol rightQuery = splitQueries.remove(rhsRelations);

        if (leftQuery == null && rightQuery == null) {
            // we don't have a match for the filter on rhs/lhs yet
            // let's see if we have partial match with a subsection of the relations
            var it = splitQueries.entrySet().iterator();
            while (it.hasNext()) {
                var entry = it.next();
                var relationNames = entry.getKey();
                var splitQuery = entry.getValue();

                var matchesLhs = Sets.intersection(lhsRelations, relationNames);
                var matchesRhs = Sets.intersection(rhsRelations, relationNames);

                if (matchesRhs.isEmpty() == false && matchesLhs.isEmpty()) {
                    rightQuery = rightQuery == null ? splitQuery : AndOperator.of(rightQuery, splitQuery);
                    it.remove();
                } else if (matchesRhs.isEmpty() && matchesLhs.isEmpty() == false) {
                    leftQuery = leftQuery == null ? splitQuery : AndOperator.of(leftQuery, splitQuery);
                    it.remove();
                }
            }
        }

        LogicalPlan newLhs = lhs;
        LogicalPlan newRhs = rhs;
        var joinType = join.joinType();
        if (joinType == JoinType.INNER || joinType == JoinType.CROSS) {
            newLhs = getNewSource(leftQuery, lhs);
            newRhs = getNewSource(rightQuery, rhs);
        } else {
            if (joinType == JoinType.LEFT) {
                newLhs = getNewSource(leftQuery, lhs);
                if (rightQuery != null) {
                    splitQueries.put(rhsRelations, rightQuery);
                }
            } else if (joinType == JoinType.RIGHT) {
                newRhs = getNewSource(rightQuery, rhs);
                if (leftQuery != null) {
                    splitQueries.put(lhsRelations, leftQuery);
                }
            }
        }

        if (newLhs == lhs && newRhs == rhs) {
            return null;
        }

        var newJoin = join.replaceSources(List.of(newLhs, newRhs));

        if (splitQueries.isEmpty()) {
            return newJoin;
        } else if (initialParts == splitQueries.size()) {
            return null;
        } else {
            return new Filter(newJoin, AndOperator.join(splitQueries.values()));
        }
    }
}
