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
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.statistics.TableStats;

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

    public RewriteFilterOnOuterJoinToInnerJoin() {
        this.nlCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
                .with(source(), typeOf(NestedLoopJoin.class).capturedAs(nlCapture)
                    .with(nl -> nl.joinType().isOuter() && !nl.isRewriteFilterOnOuterJoinToInnerJoinDone())
                );
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx) {
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(nodeCtx);
        NestedLoopJoin nl = captures.get(nlCapture);
        Symbol query = filter.query();
        Map<Set<RelationName>, Symbol> splitQueries = QuerySplitter.split(query);
        if (splitQueries.size() == 1 && splitQueries.keySet().iterator().next().size() > 1) {
            return null;
        }
        LogicalPlan lhs = nl.sources().get(0);
        LogicalPlan rhs = nl.sources().get(1);
        Set<RelationName> leftName = lhs.getRelationNames();
        Set<RelationName> rightName = rhs.getRelationNames();

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
                } else if (couldMatchOnNull(rightQuery, normalizer, txnCtx)) {
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
                } else if (couldMatchOnNull(leftQuery, normalizer, txnCtx)) {
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

                if (couldMatchOnNull(leftQuery, normalizer, txnCtx)) {
                    newLhs = lhs;
                } else {
                    newLhs = getNewSource(leftQuery, lhs);
                    if (leftQuery != null) {
                        splitQueries.put(leftName, leftQuery);
                    }
                }
                if (couldMatchOnNull(rightQuery, normalizer, txnCtx)) {
                    newRhs = rhs;
                } else {
                    newRhs = getNewSource(rightQuery, rhs);
                }

                /*
                 * Filters on each side must be put back into the Filter as each side can generate NULL's on outer joins
                 * which must be filtered out AFTER the join operation.
                 * In case the filter is only on one side, the join could be rewritten to a LEFT/RIGHT OUTER.
                 * TODO: Create a dedicated rule RewriteFilterOnOuterJoinToLeftOrRight
                 *
                 * cr> select t1.x as t1x, t2.x as t2x, t2.y as t2y from t1 full outer join t2 on t1.x = t2.x where t2y = 1;
                 * +------+------+------+
                 * |  t1x |  t2x |  t2y |
                 * +------+------+------+
                 * |    3 |    3 |    1 |
                 * |    2 |    2 |    1 |
                 * | NULL |    4 |    1 |
                 * +------+------+------+
                 */
                if (leftQuery != null) {
                    splitQueries.put(leftName, leftQuery);
                }
                if (rightQuery != null) {
                    splitQueries.put(rightName, rightQuery);
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
            nl.orderByWasPushedDown(),
            true
        );
        assert newJoin.outputs().equals(nl.outputs()) : "Outputs after rewrite must be the same as before";
        return splitQueries.isEmpty() ? newJoin : new Filter(newJoin, AndOperator.join(splitQueries.values()));
    }

    private static boolean couldMatchOnNull(@Nullable Symbol query,
                                            EvaluatingNormalizer normalizer,
                                            TransactionContext txnCtx) {
        if (query == null) {
            return false;
        }
        return WhereClause.canMatch(
            normalizer.normalize(
                RefReplacer.replaceRefs(
                    FieldReplacer.replaceFields(query, ignored -> Literal.NULL),
                    ignored -> Literal.NULL
                ),
                txnCtx)
        );
    }
}
