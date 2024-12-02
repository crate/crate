/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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


import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.EquiJoinDetector;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public final class RewriteFilterOnCrossJoinToInnerJoin implements Rule<Filter> {

    private final Capture<JoinPlan> joinCapture;
    private final Pattern<Filter> pattern;

    public RewriteFilterOnCrossJoinToInnerJoin() {
        this.joinCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(f -> EquiJoinDetector.isEquiJoin(f.query()))
            .with(
                source(),
                typeOf(JoinPlan.class)
                    .capturedAs(joinCapture)
                    .with(j -> j.joinType() == JoinType.CROSS)
            );
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter equiJoinConditionFilter, Captures captures, Context context) {
        JoinPlan crossJoin = captures.get(joinCapture);
        Symbol query = equiJoinConditionFilter.query();
        Map<Set<RelationName>, Symbol> filterRelations = QuerySplitter.split(query);

        List<RelationName> lhs = crossJoin.lhs().relationNames();
        List<RelationName> rhs = crossJoin.rhs().relationNames();
        // TODO this could be extended to also support nested-joins with more than 2 relations
        if (lhs.size() == 1 && rhs.size() == 1) {
            HashSet<RelationName> sources = HashSet.newHashSet(2);
            sources.addAll(lhs);
            sources.addAll(rhs);
            if (filterRelations.containsKey(sources)) {
                return new JoinPlan(
                    crossJoin.lhs(),
                    crossJoin.rhs(),
                    JoinType.INNER,
                    query
                );
            }
        }
        return null;
    }
}
