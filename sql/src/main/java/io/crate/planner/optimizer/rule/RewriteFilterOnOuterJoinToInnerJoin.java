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

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static io.crate.planner.optimizer.rule.FilterOnJoinsUtil.getNewSource;

/**
 * If we can determine that a filter on an OUTER JOIN turns all NULL rows that the join could generate into a NO-MATCH
 * we can push down the filter and turn the OUTER JOIN into an inner join.
 *
 * <p>
 * So this tries to transform
 * </p>
 *
 * <pre>
 *     Filter (lhs.x = 1 AND rhs.x = 2)
 *       |
 *     NestedLoop (outerJoin)
 *       /  \
 *     LHS  RHS
 * </pre>
 *
 * into
 *
 * <pre>
 *     Filter
 *       |
 *     NestedLoop (innerJoin)
 *       /      \
 *   Filter      Filter
 * (lhs.x = 1)    (rhs.x = 2)
 *     |              |
 *    LHS            RHS
 * </pre>
 *
 * A case where this is *NOT* safe is for example:
 *
 * <pre>
 * SELECT * FROM t1
 *     LEFT JOIN t2 ON t1.t2_id = t2.id
 * WHERE
 *     coalesce(t2.x, 20) > 10   # becomes TRUE for null rows
 * </pre>
 *
 *
 * A case where the FILTER turns all null rows into no-matches:
 *
 * <pre>
 * SELECT * FROM t1
 *      LEFT JOIN t2 ON t1.t2_id = t2.id
 * WHERE
 *      t2.x = 10           # null = 10 -> null -> no match
 * </pre>
 */
public final class RewriteFilterOnOuterJoinToInnerJoin implements Rule<Filter> {

    private final Capture<NestedLoopJoin> nlCapture;
    private final Pattern<Filter> pattern;
    private final EvaluatingNormalizer normalizer;

    public RewriteFilterOnOuterJoinToInnerJoin(Functions functions) {
        this.normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        this.nlCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
                .with(source(), typeOf(NestedLoopJoin.class).capturedAs(nlCapture)
                    .with(nl -> nl.joinType().isOuter())
                );
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter, Captures captures) {
        NestedLoopJoin nl = captures.get(nlCapture);
        Symbol query = filter.query();
        Map<Set<QualifiedName>, Symbol> splitQueries = QuerySplitter.split(query);
        if (splitQueries.size() == 1 && splitQueries.keySet().iterator().next().size() > 1) {
            return null;
        }
        LogicalPlan lhs = nl.sources().get(0);
        LogicalPlan rhs = nl.sources().get(1);
        Set<QualifiedName> leftName = lhs.getRelationNames();
        Set<QualifiedName> rightName = rhs.getRelationNames();

        Symbol leftQuery = splitQueries.remove(leftName);
        Symbol rightQuery = splitQueries.remove(rightName);

        final LogicalPlan newLhs;
        final LogicalPlan newRhs;
        final boolean newJoinIsInnerJoin;
        switch (nl.joinType()) {
            case LEFT:
                /* LEFT OUTER JOIN -> NULL rows are generated for the RHS if the join-condition doesn't match
                 *
                 * cr> select t1.x as t1x, t2.x as t2x from t1 left join t2 on t1.x = t2.x;
                 * +-----+------+
                 * | t1x |  t2x |
                 * +-----+------+
                 * |   3 |    3 |
                 * |   2 |    2 |
                 * |   1 | NULL |
                 * +-----+------+
                 */
                newLhs = getNewSource(leftQuery, lhs);
                if (rightQuery == null) {
                    newRhs = rhs;
                    newJoinIsInnerJoin = false;
                } else if (couldMatchOnNull(rightQuery)) {
                    newRhs = rhs;
                    newJoinIsInnerJoin = false;
                    splitQueries.put(rightName, rightQuery);
                } else {
                    newRhs = getNewSource(rightQuery, rhs);
                    newJoinIsInnerJoin = true;
                }
                break;
            case RIGHT:
                /* RIGHT OUTER JOIN -> NULL rows are generated for the LHS if the join-condition doesn't match

                 * cr> select t1.x as t1x, t2.x as t2x from t1 right join t2 on t1.x = t2.x;
                 * +------+-----+
                 * |  t1x | t2x |
                 * +------+-----+
                 * |    3 |   3 |
                 * |    2 |   2 |
                 * | NULL |   4 |
                 * +------+-----+
                 */
                if (leftQuery == null) {
                    newLhs = lhs;
                    newJoinIsInnerJoin = false;
                } else if (couldMatchOnNull(leftQuery)) {
                    newLhs = lhs;
                    newJoinIsInnerJoin = false;
                    splitQueries.put(leftName, leftQuery);
                } else {
                    newLhs = getNewSource(leftQuery, lhs);
                    newJoinIsInnerJoin = true;
                }
                newRhs = getNewSource(rightQuery, rhs);
                break;
            case FULL:
                /*
                 * cr> select t1.x as t1x, t2.x as t2x from t1 full outer join t2 on t1.x = t2.x;
                 * +------+------+
                 * |  t1x |  t2x |
                 * +------+------+
                 * |    3 |    3 |
                 * |    2 |    2 |
                 * |    1 | NULL |
                 * | NULL |    4 |
                 * +------+------+
                 */
                if (couldMatchOnNull(leftQuery)) {
                    newLhs = lhs;
                    splitQueries.put(leftName, leftQuery);
                } else {
                    newLhs = getNewSource(leftQuery, lhs);
                }
                if (couldMatchOnNull(rightQuery)) {
                    newRhs = rhs;
                    splitQueries.put(rightName, rightQuery);
                } else {
                    newRhs = getNewSource(rightQuery, rhs);
                }
                newJoinIsInnerJoin = newLhs != lhs && newRhs != rhs;
                break;
            default:
                throw new UnsupportedOperationException(
                    "The Rule to rewrite filter+outer-joins to inner joins must not be run on joins of type=" + nl.joinType());
        }
        if (newLhs == lhs && newRhs == rhs) {
            return null;
        }
        NestedLoopJoin newJoin = new NestedLoopJoin(
            newLhs,
            newRhs,
            newJoinIsInnerJoin ? JoinType.INNER : nl.joinType(),
            nl.joinCondition(),
            nl.isFiltered(),
            nl.topMostLeftRelation(),
            nl.orderByWasPushedDown()
        );
        assert newJoin.outputs().equals(nl.outputs()) : "Outputs after rewrite must be the same as before";
        return splitQueries.isEmpty() ? newJoin : new Filter(newJoin, AndOperator.join(splitQueries.values()));
    }

    private boolean couldMatchOnNull(@Nullable Symbol query) {
        if (query == null) {
            return false;
        }
        return WhereClause.canMatch(
            normalizer.normalize(FieldReplacer.replaceFields(query, ignored -> Literal.NULL), null));
    }
}
