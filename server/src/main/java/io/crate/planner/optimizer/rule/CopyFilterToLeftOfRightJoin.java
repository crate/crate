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

/**
 * Copies eligible predicates from a filter above a RIGHT JOIN to its left input.
 *
 * <p>A RIGHT JOIN preserves rows from its right input and may produce rows where
 * all columns from its left input are NULL. This rule copies a predicate only if
 * it references relations from the left input and cannot evaluate to TRUE when
 * its referenced values are NULL.
 *
 * <p>This optimization is safe for null-rejecting predicates because the
 * original filter would reject any NULL-extended row produced for an unmatched
 * right-side row. Likewise, any left-side row that fails the predicate could
 * not contribute to the final result, since the original filter would reject
 * the corresponding joined rows. Filtering such rows before the join therefore
 * does not change the query result. The original filter is retained above the
 * join to preserve the semantics of the complete filter.
 *
 * <p>Predicates that reference the right input or both inputs are not copied,
 * because they cannot be evaluated solely against the left input. Predicates
 * that can evaluate to TRUE when the left-side values are NULL must also not
 * be copied. For example, {@code lhs.col IS NULL} or
 * {@code COALESCE(lhs.col, 1) = 1} can match NULL-extended rows. Filtering the
 * left input with such a predicate could create additional unmatched
 * right-side rows that pass the original filter, changing the result.
 *
 */
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

                // Only copy predicates that cannot match a NULL-extended left input.
                // Otherwise, filtering the left input could create unmatched right-side
                // rows that would pass the original filter.
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
