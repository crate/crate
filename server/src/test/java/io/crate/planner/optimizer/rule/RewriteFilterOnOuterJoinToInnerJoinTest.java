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

import static io.crate.testing.Asserts.assertThat;

import java.util.Map;
import java.util.function.UnaryOperator;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class RewriteFilterOnOuterJoinToInnerJoinTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;
    private Map<RelationName, AnalyzedRelation> sources;
    private PlanStats planStats;
    private LogicalPlan t1;
    private LogicalPlan t2;

    @Before
    public void prepare() throws Exception {
        sources = T3.sources(clusterService);
        sqlExpressions = new SqlExpressions(sources);
        planStats = new PlanStats(sqlExpressions.nodeCtx,
            CoordinatorTxnCtx.systemTransactionContext(),
            new TableStats());

        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (a int)")
            .addTable("create table t2 (b int)")
            .build();

        t1 = e.logicalPlan("SELECT a FROM t1");
        t2 = e.logicalPlan("SELECT b FROM t2");
    }

    @Test
    public void test_push_filter_down_to_non_preserved_side_of_left_join_and_rewrites_to_inner_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.LEFT, joinCondition);
        var filter = new Filter(join, sqlExpressions.asSymbol("doc.t2.b > 1"));
        assertThat(filter).hasOperators(
            "Filter[(b > 1)]",
            "  └ Join[LEFT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[INNER | (a = b)]",
            "  ├ Collect[doc.t1 | [a] | true]",
            "  └ Filter[(b > 1)]",
            "    └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_cannot_push_filter_down_to_preserved_side_of_left_join_and_cannot_rewrite_to_inner_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.LEFT, joinCondition);
        var filter = new Filter(join, sqlExpressions.asSymbol("doc.t1.a > 1"));
        assertThat(filter).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[LEFT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).isNull();
    }

    @Test
    public void test_cannot_push_filter_down_to_non_preserved_side_of_left_join_and_cannot_rewrite_to_inner_join_if_the_filter_matches_nulls() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.LEFT, joinCondition);
        var filter = new Filter(join, sqlExpressions.asSymbol("doc.t2.b is null"));
        assertThat(filter).hasOperators(
            "Filter[(b IS NULL)]",
            "  └ Join[LEFT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).isNull();
    }

    @Test
    public void test_push_filter_down_to_non_preserved_side_of_right_join_and_rewrites_to_inner_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, sqlExpressions.asSymbol("doc.t1.a > 1"));
        assertThat(filter).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[RIGHT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[INNER | (a = b)]",
            "  ├ Filter[(a > 1)]",
            "  │  └ Collect[doc.t1 | [a] | true]",
            "  └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_cannot_push_filter_down_to_preserved_side_of_right_join_and_cannot_rewrite_to_inner_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, sqlExpressions.asSymbol("doc.t2.b > 1"));
        assertThat(filter).hasOperators(
            "Filter[(b > 1)]",
            "  └ Join[RIGHT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).isNull();
    }

    @Test
    public void test_cannot_push_filter_down_to_non_preserved_side_of_right_join_and_cannot_rewrite_to_inner_join_if_the_filter_matches_nulls() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, sqlExpressions.asSymbol("doc.t1.a is null"));
        assertThat(filter).hasOperators(
            "Filter[(a IS NULL)]",
            "  └ Join[RIGHT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).isNull();
    }

    @Test
    public void test_push_filter_down_to_preserved_sides_of_full_join_and_rewrites_to_inner_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.FULL, joinCondition);
        var filter = new Filter(join, sqlExpressions.asSymbol("doc.t1.a > 1 and doc.t2.b > 1"));
        assertThat(filter).hasOperators(
            "Filter[((a > 1) AND (b > 1))]",
            "  └ Join[FULL | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Filter[((a > 1) AND (b > 1))]",
            "  └ Join[INNER | (a = b)]",
            "    ├ Filter[(a > 1)]",
            "    │  └ Collect[doc.t1 | [a] | true]",
            "    └ Filter[(b > 1)]",
            "      └ Collect[doc.t2 | [b] | true]"
        );
    }
}
