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

public class RewriteFilterOnOuterJoinToInnerJoinTest extends CrateDummyClusterServiceUnitTest {

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
        planStats = new PlanStats(e.nodeCtx,
            CoordinatorTxnCtx.systemTransactionContext(),
            new TableStats());


        t1 = e.logicalPlan("SELECT a FROM t1");
        t2 = e.logicalPlan("SELECT b FROM t2");
        t3 = e.logicalPlan("SELECT c FROM t3");
    }

    @Test
    public void test_push_filter_down_to_non_preserved_side_of_left_join_and_rewrites_to_inner_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.LEFT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t2.b > 1"));
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
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (a = b)]",
            "  ├ Collect[doc.t1 | [a] | true]",
            "  └ Filter[(b > 1)]",
            "    └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_cannot_push_filter_down_to_preserved_side_of_left_join_and_cannot_rewrite_to_inner_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.LEFT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t1.a > 1"));
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
            e.ruleContext());

        assertThat(result).isNull();
    }

    @Test
    public void test_cannot_push_filter_down_to_non_preserved_side_of_left_join_and_cannot_rewrite_to_inner_join_if_the_filter_matches_nulls() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.LEFT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t2.b is null"));
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
            e.ruleContext());

        assertThat(result).isNull();
    }

    @Test
    public void test_push_filter_down_to_non_preserved_side_of_right_join_and_rewrites_to_inner_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t1.a > 1"));
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
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (a = b)]",
            "  ├ Filter[(a > 1)]",
            "  │  └ Collect[doc.t1 | [a] | true]",
            "  └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_cannot_push_filter_down_to_preserved_side_of_right_join_and_cannot_rewrite_to_inner_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t2.b > 1"));
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
            e.ruleContext());

        assertThat(result).isNull();
    }

    @Test
    public void test_cannot_push_filter_down_to_non_preserved_side_of_right_join_and_cannot_rewrite_to_inner_join_if_the_filter_matches_nulls() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t1.a is null"));
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
            e.ruleContext());

        assertThat(result).isNull();
    }

    @Test
    public void test_push_filter_down_to_preserved_sides_of_full_join_and_rewrites_to_inner_join() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.FULL, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t1.a > 1 and doc.t2.b > 1"));
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
            e.ruleContext());

        assertThat(result).hasOperators(
            "Filter[((a > 1) AND (b > 1))]",
            "  └ Join[INNER | (a = b)]",
            "    ├ Filter[(a > 1)]",
            "    │  └ Collect[doc.t1 | [a] | true]",
            "    └ Filter[(b > 1)]",
            "      └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_cannot_rewrite_full_join_to_inner_join_if_filter_contains_a_sub_query() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.FULL, joinCondition);
        var filter = new Filter(join, e.asSymbol("coalesce(doc.t1.a, 15) not in (select b from t2 where b = 2)"));

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).isNull();
    }

    @Test
    public void test_rewrite_right_join_to_inner_join_for_nested_left_relation() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);

        var joinCondition2 = e.asSymbol("doc.t2.b = doc.t3.c");
        var join2 = new JoinPlan(join1, t3, JoinType.RIGHT, joinCondition2);

        var filter = new Filter(join2, e.asSymbol("doc.t2.b > 1"));

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).hasOperators(
            "Join[INNER | (b = c)]",
            "  ├ Filter[(b > 1)]",
            "  │  └ Join[INNER | (a = b)]",
            "  │    ├ Collect[doc.t1 | [a] | true]",
            "  │    └ Collect[doc.t2 | [b] | true]",
            "  └ Collect[doc.t3 | [c] | true]"
        );
    }

    @Test
    public void test_rewrite_right_join_to_inner_join_when_left_contains_multiple_relations() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);

        var joinCondition2 = e.asSymbol("doc.t1.a = doc.t3.c");
        var join2 = new JoinPlan(join1, t3, JoinType.RIGHT, joinCondition2);

        var filter = new Filter(
            join2,
            e.asSymbol("doc.t1.a > 1 AND doc.t2.b < 10")
        );

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).hasOperators(
            "Join[INNER | (a = c)]",
            "  ├ Filter[((a > 1) AND (b < 10))]",
            "  │  └ Join[INNER | (a = b)]",
            "  │    ├ Collect[doc.t1 | [a] | true]",
            "  │    └ Collect[doc.t2 | [b] | true]",
            "  └ Collect[doc.t3 | [c] | true]"
        );
    }

    @Test
    public void test_do_not_rewrite_when_filter_references_both_sides() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);

        var joinCondition2 = e.asSymbol("doc.t2.b = doc.t3.c");
        var join2 = new JoinPlan(join1, t3, JoinType.RIGHT, joinCondition2);

        var filter = new Filter(
            join2,
            e.asSymbol("doc.t2.b = doc.t3.c")
        );

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).isNull();
    }

    @Test
    public void test_do_not_rewrite_when_filter_references_only_preserved_side() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);

        var joinCondition2 = e.asSymbol("doc.t2.b = doc.t3.c");
        var join2 = new JoinPlan(join1, t3, JoinType.RIGHT, joinCondition2);

        var filter = new Filter(join2, e.asSymbol("doc.t3.c > 1"));

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).isNull();
    }

    @Test
    public void test_sql_integration_rewrite_right_join_to_inner_join_and_push_filter_down() {
        LogicalPlan plan = e.logicalPlan(
            "SELECT count(*) FROM t1 JOIN t2 ON t2.b > 1 RIGHT JOIN t3 ON t2.b = t3.c WHERE t2.b > 1"
        );

        assertThat(plan).hasOperators(
            "HashAggregate[count(*)]",
            "  └ HashJoin[INNER | (b = c)]",
            "    ├ NestedLoopJoin[INNER | (b > 1)]",
            "    │  ├ Collect[doc.t1 | [] | true]",
            "    │  └ Collect[doc.t2 | [b] | (b > 1)]",
            "    └ Collect[doc.t3 | [c] | true]"
        );
    }

    @Test
    public void test_do_not_rewrite_when_filter_matches_nulls() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, e.asSymbol("doc.t1.a IS NULL"));

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).isNull();
    }

    @Test
    public void test_do_not_rewrite_when_filter_matches_nulls_semantically() {
        var joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);
        var filter = new Filter(join, e.asSymbol("coalesce(doc.t1.a, 1) = 1"));
        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).isNull();
    }

    @Test
    public void test_mixed_conjuncts_are_split_correctly_on_outer_join() {
        var joinCondition = e.asSymbol("doc.t1.a = 10");
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, joinCondition);

        var filter = new Filter(join, e.asSymbol("doc.t1.a > 1 AND doc.t1.a = doc.t2.b"));

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).hasOperators(
            "Filter[(a = b)]",
            "  └ Join[INNER | (a = 10)]",
            "    ├ Filter[(a > 1)]",
            "    │  └ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_rewrite_full_join_to_inner_join_with_nested_left_relation() {
        var joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        var join1 = new JoinPlan(t1, t2, JoinType.INNER, joinCondition1);

        var joinCondition2 = e.asSymbol("doc.t1.a = doc.t3.c");
        var join2 = new JoinPlan(join1, t3, JoinType.FULL, joinCondition2);

        var filter = new Filter(join2, e.asSymbol("doc.t1.a > 1 AND doc.t3.c > 1"));

        var rule = new RewriteFilterOnOuterJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).hasOperators(
            "Filter[((a > 1) AND (c > 1))]",
            "  └ Join[INNER | (a = c)]",
            "    ├ Filter[(a > 1)]",
            "    │  └ Join[INNER | (a = b)]",
            "    │    ├ Collect[doc.t1 | [a] | true]",
            "    │    └ Collect[doc.t2 | [b] | true]",
            "    └ Filter[(c > 1)]",
            "      └ Collect[doc.t3 | [c] | true]"
        );
    }
}
