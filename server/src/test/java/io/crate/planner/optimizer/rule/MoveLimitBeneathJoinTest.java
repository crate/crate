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
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class MoveLimitBeneathJoinTest extends CrateDummyClusterServiceUnitTest {

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
    public void test_move_filter_beneath_left_join() {
        var join = new JoinPlan(t1, t2, JoinType.LEFT, sqlExpressions.asSymbol("a = b"));
        var limit = new Limit(join, sqlExpressions.asSymbol("10"), sqlExpressions.asSymbol("0"), false, false);

        assertThat(limit).hasOperators(
            "Limit[10;0]",
            "  └ Join[LEFT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveLimitBeneathJoin();
        Match<Limit> match = rule.pattern().accept(limit, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(limit);

        Limit result = (Limit) rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            Function.identity());

        assertThat(result).hasOperators(
            "Limit[10;0]",
            "  └ Join[LEFT | (a = b)]",
            "    ├ Limit[10;0]",
            "    │  └ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        assertThat(result.isPushedBeneathJoin()).isTrue();
    }

    @Test
    public void test_move_filter_beneath_right_join() {
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, sqlExpressions.asSymbol("a = b"));
        var limit = new Limit(join, sqlExpressions.asSymbol("10"), sqlExpressions.asSymbol("0"), false, false);

        assertThat(limit).hasOperators(
            "Limit[10;0]",
            "  └ Join[RIGHT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveLimitBeneathJoin();
        Match<Limit> match = rule.pattern().accept(limit, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(limit);

        Limit result = (Limit) rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            Function.identity());

        assertThat(result).hasOperators(
            "Limit[10;0]",
            "  └ Join[RIGHT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Limit[10;0]",
            "      └ Collect[doc.t2 | [b] | true]"
        );

        assertThat(result.isPushedBeneathJoin()).isTrue();
    }

    @Test
    public void test_move_filter_beneath_cross_join() {
        var join = new JoinPlan(t1, t2, JoinType.CROSS, null);
        var limit = new Limit(join, sqlExpressions.asSymbol("10"), sqlExpressions.asSymbol("0"), false, false);

        assertThat(limit).hasOperators(
            "Limit[10;0]",
            "  └ Join[CROSS]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveLimitBeneathJoin();
        Match<Limit> match = rule.pattern().accept(limit, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(limit);

        Limit result = (Limit) rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            Function.identity());

        assertThat(result).hasOperators(
            "Limit[10;0]",
            "  └ Join[CROSS]",
            "    ├ Limit[10;0]",
            "    │  └ Collect[doc.t1 | [a] | true]",
            "    └ Limit[10;0]",
            "      └ Collect[doc.t2 | [b] | true]"
        );

        assertThat(result.isPushedBeneathJoin()).isTrue();
    }

    @Test
    public void test_do_not_move_filter_beneath_full_join() {
        var join = new JoinPlan(t1, t2, JoinType.FULL, sqlExpressions.asSymbol("a = b"));
        var limit = new Limit(join, sqlExpressions.asSymbol("10"), sqlExpressions.asSymbol("0"), false, false);

        assertThat(limit).hasOperators(
            "Limit[10;0]",
            "  └ Join[FULL | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveLimitBeneathJoin();
        Match<Limit> match = rule.pattern().accept(limit, Captures.empty());
        assertThat(match.isPresent()).isFalse();
    }

    @Test
    public void test_do_not_move_filter_beneath_inner_join() {
        var join = new JoinPlan(t1, t2, JoinType.INNER, sqlExpressions.asSymbol("a = b"));
        var limit = new Limit(join, sqlExpressions.asSymbol("10"), sqlExpressions.asSymbol("0"), false, false);

        assertThat(limit).hasOperators(
            "Limit[10;0]",
            "  └ Join[INNER | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveLimitBeneathJoin();
        Match<Limit> match = rule.pattern().accept(limit, Captures.empty());
        Assertions.assertThat(match.isPresent()).isFalse();
    }

    @Test
    public void test_limit_offset_is_normalized_to_global_limit() {
        var join = new JoinPlan(t1, t2, JoinType.RIGHT, sqlExpressions.asSymbol("a = b"));
        var limit = new Limit(join, sqlExpressions.asSymbol("10"), sqlExpressions.asSymbol("5"), false, false);

        assertThat(limit).hasOperators(
            "Limit[10;5]",
            "  └ Join[RIGHT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MoveLimitBeneathJoin();
        Match<Limit> match = rule.pattern().accept(limit, Captures.empty());

        Limit result = (Limit) rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            Function.identity());

        assertThat(match.isPresent()).isTrue();
        assertThat(result).hasOperators(
            "Limit[10;5]",
            "  └ Join[RIGHT | (a = b)]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Limit[15;0]",
            "      └ Collect[doc.t2 | [b] | true]"
        );
    }
}
