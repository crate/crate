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

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static io.crate.sql.tree.JoinType.CROSS;
import static io.crate.sql.tree.JoinType.INNER;
import static io.crate.sql.tree.JoinType.LEFT;
import static io.crate.sql.tree.JoinType.RIGHT;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Sets;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.AbstractJoinPlan;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

/**
 * Splits the given filter and pushes the split filters down to preserved sides of the joins.
 * Note: a side is preserved, if the join returns all or a subset of the rows from the side and
 * non-preserved, if it can provide extra null rows.
 * Preserved sides:
 *      LHS of LEFT join
 *      RHS of RIGHT join
 *      neither sides of FULL join
 *      both sides of CROSS and INNER joins.
 * <p>
 * Below example describes how this rule and {@link RewriteFilterOnOuterJoinToInnerJoin} work together:
 * cr> create table t1 (x int);
 * CREATE OK, 1 row affected  (2.083 sec)
 * cr> create table t2 (x int);
 * CREATE OK, 1 row affected  (1.785 sec)
 * cr> explain verbose select * from t1 left join t2 on t1.x = t2.x where t1.x > 1 and t2.x > 1;
 * +------------------------------------------------------+------------------------------------------------------+
 * | STEP                                                 | QUERY PLAN                                           |
 * +------------------------------------------------------+------------------------------------------------------+
 * | Initial logical plan                                 | Filter[((x > 1) AND (x > 1))] (rows=0)               |
 * |                                                      |   └ Join[LEFT | (x = x)] (rows=unknown)              |
 * |                                                      |     ├ Collect[doc.t1 | [x] | true] (rows=unknown)    |
 * |                                                      |     └ Collect[doc.t2 | [x] | true] (rows=unknown)    |
 * | optimizer_move_filter_beneath_join                   | Filter[(x > 1)] (rows=0)                             | <-- MoveFilterBeneathJoin splits the filter and pushes down to preserved sides of the joins.
 * |                                                      |   └ Join[LEFT | (x = x)] (rows=unknown)              |
 * |                                                      |     ├ Filter[(x > 1)] (rows=0)                       |
 * |                                                      |     │  └ Collect[doc.t1 | [x] | true] (rows=unknown) |
 * |                                                      |     └ Collect[doc.t2 | [x] | true] (rows=unknown)    |
 * | optimizer_rewrite_filter_on_outer_join_to_inner_join | Join[INNER | (x = x)] (rows=unknown)                 | <-- RewriteFilterOnOuterJoinToInnerJoin pushes down (t2.x > 1) and LEFT join is re-written to INNER join.
 * |                                                      |   ├ Filter[(x > 1)] (rows=0)                         |
 * |                                                      |   │  └ Collect[doc.t1 | [x] | true] (rows=unknown)   |
 * |                                                      |   └ Filter[(x > 1)] (rows=0)                         |
 * |                                                      |     └ Collect[doc.t2 | [x] | true] (rows=unknown)    |
 * <p>
 * See {@link MoveFilterBeneathJoinTest} for more examples.
 */
public final class MoveFilterBeneathJoin implements Rule<Filter> {

    private final Capture<AbstractJoinPlan> joinCapture;
    private final Pattern<Filter> pattern;
    private static final Set<JoinType> SUPPORTED_JOIN_TYPES = EnumSet.of(INNER, LEFT, RIGHT, CROSS);

    public MoveFilterBeneathJoin() {
        this.joinCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(),
                typeOf(AbstractJoinPlan.class)
                    .capturedAs(joinCapture)
                    .with(join -> SUPPORTED_JOIN_TYPES.contains(join.joinType()))
            );
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        var join = captures.get(joinCapture);
        var query = filter.query();
        if (!WhereClause.canMatch(query)) {
            return join.replaceSources(List.of(
                getNewSource(query, join.lhs()),
                getNewSource(query, join.rhs())
            ));
        }
        var splitQueries = QuerySplitter.split(query);
        final int initialParts = splitQueries.size();
        if (splitQueries.size() == 1 && splitQueries.keySet().iterator().next().size() > 1) {
            return null;
        }
        var lhs = join.lhs();
        var rhs = join.rhs();

        var lhsRelations = new HashSet<>(lhs.relationNames());
        var rhsRelations = new HashSet<>(rhs.relationNames());

        var leftQuery = splitQueries.remove(lhsRelations);
        var rightQuery = splitQueries.remove(rhsRelations);

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

        var newLhs = lhs;
        var newRhs = rhs;
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

    static LogicalPlan getNewSource(@Nullable Symbol splitQuery, LogicalPlan source) {
        return splitQuery == null ? source : new Filter(source, splitQuery);
    }
}
