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

package io.crate.planner.optimizer.rule;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class FilterOnJoinsUtil {

    private FilterOnJoinsUtil() {
    }

    static Set<QualifiedName> getRelationNames(LogicalPlan plan) {
        return plan.baseTables().stream().map(AnalyzedRelation::getQualifiedName).collect(Collectors.toSet());
    }

    static LogicalPlan getNewSource(@Nullable Symbol splitQuery, LogicalPlan source) {
        return splitQuery == null ? source : new Filter(source, splitQuery);
    }

    static LogicalPlan moveQueryBelowJoin(Symbol query, LogicalPlan join) {
        Map<Set<QualifiedName>, Symbol> splitQuery = QuerySplitter.split(query);
        if (splitQuery.size() == 1 && splitQuery.keySet().iterator().next().size() > 1) {
            return null;
        }
        assert join.sources().size() == 2 : "Join operator must only have 2 children, LHS and RHS";
        LogicalPlan lhs = join.sources().get(0);
        LogicalPlan rhs = join.sources().get(1);
        Set<QualifiedName> leftName = getRelationNames(lhs);
        Set<QualifiedName> rightName = getRelationNames(rhs);
        Symbol queryForLhs = splitQuery.remove(leftName);
        Symbol queryForRhs = splitQuery.remove(rightName);
        LogicalPlan newLhs = getNewSource(queryForLhs, lhs);
        LogicalPlan newRhs = getNewSource(queryForRhs, rhs);
        LogicalPlan newJoin = join.replaceSources(List.of(newLhs, newRhs));
        if (splitQuery.isEmpty()) {
            return newJoin;
        } else {
            return new Filter(newJoin, AndOperator.join(splitQuery.values()));
        }
    }
}
