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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Sets;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.AbstractJoinPlan;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class CopyFilterToLeftOfRightJoin implements Rule<Filter> {

    private final Capture<AbstractJoinPlan> joinCapture;
    private final Pattern<Filter> pattern;

    public CopyFilterToLeftOfRightJoin() {
        this.joinCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(AbstractJoinPlan.class)
                .capturedAs(joinCapture)
                .with(join -> join.joinType() == JoinType.RIGHT)
            );
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter, Captures captures, Rule.Context context) {
        AbstractJoinPlan join = captures.get(joinCapture);
        Symbol query = filter.query();

        var splitQueries = QuerySplitter.split(query);
        var lhsRelations = new HashSet<>(join.lhs().relationNames());
        var rhsRelations = new HashSet<>(join.rhs().relationNames());
        var symbolEvaluator = new NullSymbolEvaluator(context.txnCtx(), context.nodeCtx());
        List<Symbol> pushableConjuncts = new ArrayList<>();

        for (var entry : splitQueries.entrySet()) {
            var relationNames = entry.getKey();
            var splitQuery = entry.getValue();

            if (!relationNames.isEmpty()
                && lhsRelations.containsAll(relationNames)
                && Sets.intersection(rhsRelations, relationNames).isEmpty()) {

                Symbol nullEvaluated = splitQuery.accept(symbolEvaluator, null);

                if (!WhereClause.canMatch(nullEvaluated)) {
                    pushableConjuncts.add(splitQuery);
                }
            }
        }

        if (pushableConjuncts.isEmpty()) {
            return null;
        }

        Symbol safeLeftQuery = AndOperator.join(pushableConjuncts);

        if (join.lhs() instanceof Filter lhsFilter && lhsFilter.query().equals(safeLeftQuery)) {
            return null;
        }

        LogicalPlan newLhs = new Filter(join.lhs(), safeLeftQuery);
        LogicalPlan newJoin = join.replaceSources(List.of(newLhs, join.rhs()));

        return new Filter(newJoin, query);
    }
}
