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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.UnaryOperator;

import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class MoveFilterBeneathJoinTest extends CrateDummyClusterServiceUnitTest {

    private PlanStats planStats;
    private LogicalPlan t1;
    private LogicalPlan t2;
    private LogicalPlan t3;
    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (a int)")
            .addTable("create table t2 (b int)")
            .addTable("create table t3 (c int)");
        planStats = new PlanStats(
            e.nodeCtx,
            CoordinatorTxnCtx.systemTransactionContext(),
            new TableStats());

        t1 = e.logicalPlan("SELECT a FROM t1");
        t2 = e.logicalPlan("SELECT b FROM t2");
        t3 = e.logicalPlan("SELECT c FROM t3");
    }

    @Test
    public void test_push_filter_beyond_join() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);
        var filter = new Filter(join1, e.asSymbol("doc.t1.a > 1"));

        assertThat(filter).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[INNER | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );


        var rule = new MoveFilterBeneathJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
                                match.captures(),
                                planStats,
                                CoordinatorTxnCtx.systemTransactionContext(),
                                e.nodeCtx,
                                UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[INNER | (a = b)]",
            "  ├ Filter[(a > 1)]",
            "  │  └ Collect[doc.t1 | [a] | true]",
            "  └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_push_filter_relating_to_nested_right_relation_beyond_join() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);
        var joinCondition2 = e.asSymbol("doc.t2.b = doc.t3.c");
        var join2 = new JoinPlan(join1, t3, JoinType.INNER, joinCondition2);
        var filter = new Filter(join2, e.asSymbol("doc.t1.a > 1"));

        assertThat(filter).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[INNER | (b = c)]",
            "    ├ Join[INNER | (a = b)]",
            "    │  ├ Collect[doc.t1 | [a] | true]",
            "    │  └ Collect[doc.t2 | [b] | true]",
            "    └ Collect[doc.t3 | [c] | true]"
        );


        var rule = new MoveFilterBeneathJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
                                match.captures(),
                                planStats,
                                CoordinatorTxnCtx.systemTransactionContext(),
                                e.nodeCtx,
                                UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[INNER | (b = c)]",
            "  ├ Filter[(a > 1)]",
            "  │  └ Join[INNER | (a = b)]",
            "  │    ├ Collect[doc.t1 | [a] | true]",
            "  │    └ Collect[doc.t2 | [b] | true]",
            "  └ Collect[doc.t3 | [c] | true]"
        );
    }

    @Test
    public void test_push_filter_relating_to_nested_left_relation_beyond_join() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);
        var joinCondition2 = e.asSymbol("doc.t2.b = doc.t3.c");
        var join2 = new JoinPlan(t3, join1, JoinType.INNER, joinCondition2);
        var filter = new Filter(join2, e.asSymbol("doc.t1.a > 1"));

        assertThat(filter).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[INNER | (b = c)]",
            "    ├ Collect[doc.t3 | [c] | true]",
            "    └ Join[INNER | (a = b)]",
            "      ├ Collect[doc.t1 | [a] | true]",
            "      └ Collect[doc.t2 | [b] | true]"
        );


        var rule = new MoveFilterBeneathJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
                                match.captures(),
                                planStats,
                                CoordinatorTxnCtx.systemTransactionContext(),
                                e.nodeCtx,
                                UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[INNER | (b = c)]",
            "  ├ Collect[doc.t3 | [c] | true]",
            "  └ Filter[(a > 1)]",
            "    └ Join[INNER | (a = b)]",
            "      ├ Collect[doc.t1 | [a] | true]",
            "      └ Collect[doc.t2 | [b] | true]"
        );
    }

    public void test_do_not_push_filter_when_both_sides_match() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);

        var joinCondition2 = e.asSymbol("doc.t1.a = doc.t3.c");
        var join2 = new JoinPlan(t1, t3, JoinType.INNER, joinCondition2);

        var joinCondition3 = e.asSymbol("doc.t2.b = doc.t1.a");
        var join3 = new JoinPlan(join1, join2, JoinType.INNER, joinCondition3);

        var filter = new Filter(join3, e.asSymbol("doc.t1.a > 1"));

        assertThat(filter).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[INNER | (b = a)]",
            "    ├ Join[INNER | (a = b)]",
            "    │  ├ Collect[doc.t1 | [a] | true]",
            "    │  └ Collect[doc.t2 | [b] | true]",
            "    └ Join[INNER | (a = c)]",
            "      ├ Collect[doc.t1 | [a] | true]",
            "      └ Collect[doc.t3 | [c] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
                                match.captures(),
                                planStats,
                                CoordinatorTxnCtx.systemTransactionContext(),
                                e.nodeCtx,
                                UnaryOperator.identity());

        assertThat(result).isNull();
    }

    public void test_push_and_split_filter_to_both_sides() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);

        var joinCondition2 = e.asSymbol("doc.t1.a = doc.t3.c");
        var join2 = new JoinPlan(t1, t3, JoinType.INNER, joinCondition2);

        var joinCondition3 = e.asSymbol("doc.t2.b = doc.t1.a");
        var join3 = new JoinPlan(join1, join2, JoinType.INNER, joinCondition3);

        var filter = new Filter(join3, e.asSymbol("doc.t1.a > 1 AND doc.t2.b < 10 AND doc.t3.c = 1"));

        assertThat(filter).hasOperators(
            "Filter[(((a > 1) AND (b < 10)) AND (c = 1))]",
            "  └ Join[INNER | (b = a)]",
            "    ├ Join[INNER | (a = b)]",
            "    │  ├ Collect[doc.t1 | [a] | true]",
            "    │  └ Collect[doc.t2 | [b] | true]",
            "    └ Join[INNER | (a = c)]",
            "      ├ Collect[doc.t1 | [a] | true]",
            "      └ Collect[doc.t3 | [c] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
                                match.captures(),
                                planStats,
                                CoordinatorTxnCtx.systemTransactionContext(),
                                e.nodeCtx,
                                UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[INNER | (b = a)]",
            "    ├ Filter[(b < 10)]",
            "    │  └ Join[INNER | (a = b)]",
            "    │    ├ Collect[doc.t1 | [a] | true]",
            "    │    └ Collect[doc.t2 | [b] | true]",
            "    └ Filter[(c = 1)]",
            "      └ Join[INNER | (a = c)]",
            "        ├ Collect[doc.t1 | [a] | true]",
            "        └ Collect[doc.t3 | [c] | true]"
        );
    }

    @Test
    public void test_push_multiple_filters_to_the_same_side() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);

        var joinCondition2 = e.asSymbol("doc.t1.a = doc.t3.c");
        var join2 = new JoinPlan(join1, t3, JoinType.INNER, joinCondition2);

        var filter = new Filter(join2, e.asSymbol("doc.t1.a > 1 AND doc.t2.b < 2"));

        assertThat(filter).hasOperators(
            "Filter[((a > 1) AND (b < 2))]",
            "  └ Join[INNER | (a = c)]",
            "    ├ Join[INNER | (a = b)]",
            "    │  ├ Collect[doc.t1 | [a] | true]",
            "    │  └ Collect[doc.t2 | [b] | true]",
            "    └ Collect[doc.t3 | [c] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[INNER | (a = c)]",
            "  ├ Filter[((a > 1) AND (b < 2))]",
            "  │  └ Join[INNER | (a = b)]",
            "  │    ├ Collect[doc.t1 | [a] | true]",
            "  │    └ Collect[doc.t2 | [b] | true]",
            "  └ Collect[doc.t3 | [c] | true]"
        );
    }

    @Test
    public void test_push_filter_down_to_preserved_side_of_left_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.LEFT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t1.a > 1"));
        assertThat(filter).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[LEFT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[LEFT | (a = b)]",
            "  ├ Filter[(a > 1)]",
            "  │  └ Collect[doc.t1 | [a] | true]",
            "  └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_cannot_push_filter_down_to_non_preserved_side_of_left_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.LEFT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t2.b > 1"));
        assertThat(filter).hasOperators(
            "Filter[(b > 1)]",
            "  └ Join[LEFT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).isNull();
    }

    @Test
    public void test_push_filter_down_to_preserved_of_right_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t2.b > 1"));
        assertThat(filter).hasOperators(
            "Filter[(b > 1)]",
            "  └ Join[RIGHT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[RIGHT | (a = b)]",
            "  ├ Collect[doc.t1 | [a] | true]",
            "  └ Filter[(b > 1)]",
            "    └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_cannot_push_filter_down_to_non_preserved_side_of_right_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t1.a > 1"));
        assertThat(filter).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[RIGHT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).isNull();
    }

    @Test
    public void test_cannot_push_filter_down_to_full_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.FULL, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t1.a > 1 and doc.t2.b > 1"));
        assertThat(filter).hasOperators(
            "Filter[((a > 1) AND (b > 1))]",
            "  └ Join[FULL | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isFalse();
    }

    @Test
    public void test_push_filter_down_to_cross_join() {
        var join = new JoinPlan(t1, t2, JoinType.CROSS, null);
        var filter = new Filter(join, e.asSymbol("doc.t2.b > 1"));
        assertThat(filter).hasOperators(
            "Filter[(b > 1)]",
            "  └ Join[CROSS]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[CROSS]",
            "  ├ Collect[doc.t1 | [a] | true]",
            "  └ Filter[(b > 1)]",
            "    └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_push_filter_down_to_preserved_side_of_left_nested_join() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.LEFT, joinCondition1);
        var joinCondition2 = e.asSymbol("doc.t1.a = doc.t3.c");
        var join2 = new JoinPlan(join1, t3, JoinType.LEFT, joinCondition2);
        var filter = new Filter(join2, e.asSymbol("doc.t1.a > 1"));
        assertThat(filter).hasOperators(
            "Filter[(a > 1)]",
            "  └ Join[LEFT | (a = c)]",
            "    ├ Join[LEFT | (a = b)]",
            "    │  ├ Collect[doc.t1 | [a] | true]",
            "    │  └ Collect[doc.t2 | [b] | true]",
            "    └ Collect[doc.t3 | [c] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[LEFT | (a = c)]",
            "  ├ Filter[(a > 1)]",
            "  │  └ Join[LEFT | (a = b)]",
            "  │    ├ Collect[doc.t1 | [a] | true]",
            "  │    └ Collect[doc.t2 | [b] | true]",
            "  └ Collect[doc.t3 | [c] | true]"
        );
    }

    @Test
    public void test_push_filter_down_to_preserved_side_of_right_nested_join() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition1);
        var joinCondition2 = e.asSymbol("doc.t2.b = doc.t3.c");
        var join2 = new JoinPlan(t3, join1, JoinType.RIGHT, joinCondition2);
        var filter = new Filter(join2, e.asSymbol("doc.t2.b > 1"));
        assertThat(filter).hasOperators(
            "Filter[(b > 1)]",
            "  └ Join[RIGHT | (b = c)]",
            "    ├ Collect[doc.t3 | [c] | true]",
            "    └ Join[RIGHT | (a = b)]",
            "      ├ Collect[doc.t1 | [a] | true]",
            "      └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveFilterBeneathJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[RIGHT | (b = c)]",
            "  ├ Collect[doc.t3 | [c] | true]",
            "  └ Filter[(b > 1)]",
            "    └ Join[RIGHT | (a = b)]",
            "      ├ Collect[doc.t1 | [a] | true]",
            "      └ Collect[doc.t2 | [b] | true]"
        );
    }
}
