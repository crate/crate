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

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;

import org.jetbrains.annotations.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntSupplier;

final class FilterOnJoinsUtil {

    private FilterOnJoinsUtil() {
    }

    static LogicalPlan getNewSource(@Nullable Symbol splitQuery, LogicalPlan source, IntSupplier ids) {
        return splitQuery == null ? source : new Filter(ids.getAsInt(), source, splitQuery);
    }

    static LogicalPlan moveQueryBelowJoin(Symbol query, JoinPlan join, IntSupplier ids) {
        if (!WhereClause.canMatch(query)) {
            return join.replaceSources(List.of(
                getNewSource(query, join.lhs(), ids),
                getNewSource(query, join.rhs(), ids)
            ));
        }
        Map<Set<RelationName>, Symbol> splitQuery = QuerySplitter.split(query);
        int initialParts = splitQuery.size();
        if (splitQuery.size() == 1 && splitQuery.keySet().iterator().next().size() > 1) {
            return null;
        }
        LogicalPlan lhs = join.lhs();
        LogicalPlan rhs = join.rhs();
        Symbol queryForLhs = splitQuery.remove(lhs.getRelationNames());
        Symbol queryForRhs = splitQuery.remove(rhs.getRelationNames());
        LogicalPlan newLhs = getNewSource(queryForLhs, lhs, ids);
        LogicalPlan newRhs = getNewSource(queryForRhs, rhs, ids);

        LogicalPlan newJoin = join.replaceSources(List.of(newLhs, newRhs));
        if (splitQuery.isEmpty()) {
            return newJoin;
        } else if (initialParts == splitQuery.size()) {
            return null;
        } else {
            return new Filter(ids.getAsInt(), newJoin, AndOperator.join(splitQuery.values()));
        }
    }
}
