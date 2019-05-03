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
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.QualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

public final class MoveFilterBeneathHashJoin implements Rule<Filter> {

    private final Capture<HashJoin> joinCapture;
    private final Pattern<Filter> pattern;

    public MoveFilterBeneathHashJoin() {
        this.joinCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(HashJoin.class).capturedAs(joinCapture));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter, Captures captures) {
        HashJoin hashJoin = captures.get(joinCapture);
        Symbol query = filter.query();
        Map<Set<QualifiedName>, Symbol> splitQuery = QuerySplitter.split(query);
        if (splitQuery.size() == 1 && splitQuery.keySet().iterator().next().size() > 1) {
            return null;
        }
        Set<QualifiedName> leftName = getName(hashJoin.lhs());
        Set<QualifiedName> rightName = getName(hashJoin.rhs());
        Symbol queryForLhs = splitQuery.remove(leftName);
        Symbol queryForRhs = splitQuery.remove(rightName);
        LogicalPlan newLhs = getNewSource(queryForLhs, hashJoin.lhs());
        LogicalPlan newRhs = getNewSource(queryForRhs, hashJoin.rhs());
        LogicalPlan newJoin = hashJoin.replaceSources(List.of(newLhs, newRhs));
        if (splitQuery.isEmpty()) {
            return newJoin;
        } else {
            return new Filter(newJoin, AndOperator.join(splitQuery.values()));
        }
    }

    private static Set<QualifiedName> getName(LogicalPlan plan) {
        return plan.baseTables().stream().map(AnalyzedRelation::getQualifiedName).collect(Collectors.toSet());
    }

    private static LogicalPlan getNewSource(Symbol splitQuery, LogicalPlan source) {
        return splitQuery == null ? source : new Filter(source, splitQuery);
    }
}
