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

import java.util.Collections;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.sql.tree.JoinType;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class MoveConstantJoinConditionsBeneathJoinTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;
    private Map<RelationName, AnalyzedRelation> sources;
    private PlanStats planStats;
    private Collect c1;
    private Collect c2;

    @Before
    public void prepare() throws Exception {
        sources = T3.sources(clusterService);
        sqlExpressions = new SqlExpressions(sources);
        planStats = new PlanStats(sqlExpressions.nodeCtx, CoordinatorTxnCtx.systemTransactionContext(), new TableStats());
        c1 = new Collect((AbstractTableRelation<?>) sources.get(T3.T1), Collections.emptyList(), WhereClause.MATCH_ALL);
        c2 = new Collect((AbstractTableRelation<?>) sources.get(T3.T2), Collections.emptyList(), WhereClause.MATCH_ALL);
    }

    @Test
    public void test_extract_constant_join_condition_into_filter_on_inner_join() {
        // This condition has a non-constant part `doc.t1.x = doc.t2.y` and a constant part `doc.t2.b = 'abc'`
        var joinCondition = sqlExpressions.asSymbol("doc.t1.x = doc.t2.y and doc.t2.b = 'abc'");

        JoinPlan jp = new JoinPlan(c1, c2, JoinType.INNER, joinCondition);
        var rule = new MoveConstantJoinConditionsBeneathJoin();
        Match<JoinPlan> match = rule.pattern().accept(jp, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(jp);

        LogicalPlan result = rule.apply(
            match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[INNER | (x = y)]",
            "  ├ Collect[doc.t1 | [] | true]",
            "  └ Filter[(b = 'abc')]",
            "    └ Collect[doc.t2 | [] | true]"
        );
    }

    @Test
    public void test_extract_constant_join_condition_into_filter_on_cross_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.x = doc.t2.y and doc.t2.b = 'abc'");

        JoinPlan jp = new JoinPlan(c1, c2, JoinType.CROSS, joinCondition);
        var rule = new MoveConstantJoinConditionsBeneathJoin();
        Match<JoinPlan> match = rule.pattern().accept(jp, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(jp);

        LogicalPlan result = rule.apply(
            match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[CROSS | (x = y)]",
            "  ├ Collect[doc.t1 | [] | true]",
            "  └ Filter[(b = 'abc')]",
            "    └ Collect[doc.t2 | [] | true]"
        );
    }

    @Test
    public void test_filter_on_lhs_on_right_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.x = doc.t2.y and doc.t1.x > 1");

        JoinPlan jp = new JoinPlan(c1, c2, JoinType.RIGHT, joinCondition);
        var rule = new MoveConstantJoinConditionsBeneathJoin();
        Match<JoinPlan> match = rule.pattern().accept(jp, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(jp);

        LogicalPlan result = rule.apply(
            match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[RIGHT | (x = y)]",
            "  ├ Filter[(x > 1)]",
            "  │  └ Collect[doc.t1 | [] | true]",
            "  └ Collect[doc.t2 | [] | true]"
        );
    }

    @Test
    public void test_filter_on_rhs_on_right_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.x = doc.t2.y and doc.t2.y > 1");

        JoinPlan jp = new JoinPlan(c1, c2, JoinType.RIGHT, joinCondition);
        var rule = new MoveConstantJoinConditionsBeneathJoin();
        Match<JoinPlan> match = rule.pattern().accept(jp, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(jp);

        LogicalPlan result = rule.apply(
            match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[RIGHT | ((x = y) AND (y > 1))]",
            "  ├ Collect[doc.t1 | [] | true]",
            "  └ Collect[doc.t2 | [] | true]"
        );
    }

    @Test
    public void test_filter_on_rhs_on_left_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.x = doc.t2.y and doc.t2.y > 1");

        JoinPlan jp = new JoinPlan(c1, c2, JoinType.LEFT, joinCondition);
        var rule = new MoveConstantJoinConditionsBeneathJoin();
        Match<JoinPlan> match = rule.pattern().accept(jp, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(jp);

        LogicalPlan result = rule.apply(
            match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[LEFT | (x = y)]",
            "  ├ Collect[doc.t1 | [] | true]",
            "  └ Filter[(y > 1)]",
            "    └ Collect[doc.t2 | [] | true]"
        );
    }

    @Test
    public void test_filter_on_lhs_on_left_join() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.x = doc.t2.y and doc.t1.x > 1");

        JoinPlan jp = new JoinPlan(c1, c2, JoinType.LEFT, joinCondition);
        var rule = new MoveConstantJoinConditionsBeneathJoin();
        Match<JoinPlan> match = rule.pattern().accept(jp, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(jp);

        LogicalPlan result = rule.apply(
            match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[LEFT | ((x = y) AND (x > 1))]",
            "  ├ Collect[doc.t1 | [] | true]",
            "  └ Collect[doc.t2 | [] | true]"
        );
    }

    @Test
    public void test_filter_on_lhs_on_left_join_with_remaining_filter() {
        var joinCondition = sqlExpressions.asSymbol("doc.t1.x = doc.t2.y and doc.t1.x = 1 and t2.y > 5");

        JoinPlan jp = new JoinPlan(c1, c2, JoinType.LEFT, joinCondition);
        var rule = new MoveConstantJoinConditionsBeneathJoin();
        Match<JoinPlan> match = rule.pattern().accept(jp, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(jp);

        LogicalPlan result = rule.apply(
            match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).hasOperators(
            "Join[LEFT | ((x = y) AND (x = 1))]",
            "  ├ Collect[doc.t1 | [] | true]",
            "  └ Filter[(y > 5)]",
            "    └ Collect[doc.t2 | [] | true]"
        );

    }
}
