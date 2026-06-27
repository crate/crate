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

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.expression.symbol.Function;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.symbol.Symbol;
import io.crate.testing.SQLExecutor;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.sql.tree.JoinType;

public class DeduplicateAndJoinConditionsTest extends CrateDummyClusterServiceUnitTest {

    private LogicalPlan t1;
    private LogicalPlan t2;
    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService)
                .addTable("create table t1 (a int)")
                .addTable("create table t2 (b int, c int)");

        t1 = e.logicalPlan("SELECT a FROM t1");
        t2 = e.logicalPlan("SELECT b FROM t2");
    }

    @Test
    public void test_does_not_match_if_no_join_conditions() {
        JoinPlan plan = new JoinPlan(t1, t2, JoinType.CROSS, null);

        assertThat(plan).hasOperators(
                "Join[CROSS]",
                "  ├ Collect[doc.t1 | [a] | true]",
                "  └ Collect[doc.t2 | [b] | true]");

        Rule<JoinPlan> rule = new DeduplicateAndJoinConditions();
        Match<JoinPlan> match = rule.pattern().accept(plan, Captures.empty());
        assertThat(match.isPresent()).isFalse();
    }

    @Test
    public void test_deduplicates_identical_join_conditions() {
        Symbol joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        Symbol joinCondition2 = e.asSymbol("doc.t1.a > 20");
        JoinPlan plan = new JoinPlan(t1, t2, JoinType.INNER,
                AndOperator.join(List.of(joinCondition1, joinCondition1, joinCondition2)));

        assertThat(plan).hasOperators(
                "Join[INNER | (((a = b) AND (a = b)) AND (a > 20))]",
                "  ├ Collect[doc.t1 | [a] | true]",
                "  └ Collect[doc.t2 | [b] | true]");

        Rule<JoinPlan> rule = new DeduplicateAndJoinConditions();
        Match<JoinPlan> match = rule.pattern().accept(plan, Captures.empty());
        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(plan);

        LogicalPlan result = rule.apply(plan, match.captures(), e.ruleContext());
        assertThat(result).hasOperators(
                "Join[INNER | ((a = b) AND (a > 20))]",
                "  ├ Collect[doc.t1 | [a] | true]",
                "  └ Collect[doc.t2 | [b] | true]");
    }

    @Test
    public void test_does_nothing_if_no_duplicates() {
        Symbol joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        Symbol joinCondition2 = e.asSymbol("doc.t1.a > 20");
        JoinPlan plan = new JoinPlan(t1, t2, JoinType.INNER, AndOperator.join(List.of(joinCondition1, joinCondition2)));

        assertThat(plan).hasOperators(
                "Join[INNER | ((a = b) AND (a > 20))]",
                "  ├ Collect[doc.t1 | [a] | true]",
                "  └ Collect[doc.t2 | [b] | true]");

        Rule<JoinPlan> rule = new DeduplicateAndJoinConditions();
        Match<JoinPlan> match = rule.pattern().accept(plan, Captures.empty());
        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(plan);

        LogicalPlan result = rule.apply(plan, match.captures(), e.ruleContext());
        assertThat(result).isNull();
    }

    @Test
    public void test_does_nothing_if_no_and_operator() {
        Symbol joinCondition = e.asSymbol("doc.t1.a = doc.t2.b");
        Symbol orConjunction = new Function(OrOperator.SIGNATURE, List.of(joinCondition, joinCondition),
                Operator.RETURN_TYPE);
        JoinPlan plan = new JoinPlan(t1, t2, JoinType.INNER, orConjunction);

        assertThat(plan).hasOperators(
                "Join[INNER | ((a = b) OR (a = b))]",
                "  ├ Collect[doc.t1 | [a] | true]",
                "  └ Collect[doc.t2 | [b] | true]");

        Rule<JoinPlan> rule = new DeduplicateAndJoinConditions();
        Match<JoinPlan> match = rule.pattern().accept(plan, Captures.empty());
        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(plan);

        LogicalPlan result = rule.apply(plan, match.captures(), e.ruleContext());
        assertThat(result).isNull();
    }

    @Test
    public void test_deduplicates_and_preserves_other_conjunctions() {
        Symbol joinCondition1 = e.asSymbol("doc.t1.a = doc.t2.b");
        Symbol joinCondition2 = e.asSymbol("doc.t2.c > doc.t1.a");
        Symbol andConjunction = AndOperator.join(List.of(joinCondition1, joinCondition1));
        Symbol orConjunction = new Function(OrOperator.SIGNATURE, List.of(andConjunction, joinCondition2),
                Operator.RETURN_TYPE);
        JoinPlan plan = new JoinPlan(t1, t2, JoinType.INNER, orConjunction);

        assertThat(plan).hasOperators(
                "Join[INNER | (((a = b) AND (a = b)) OR (c > a))]",
                "  ├ Collect[doc.t1 | [a] | true]",
                "  └ Collect[doc.t2 | [b] | true]");

        Rule<JoinPlan> rule = new DeduplicateAndJoinConditions();
        Match<JoinPlan> match = rule.pattern().accept(plan, Captures.empty());
        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(plan);

        LogicalPlan result = rule.apply(plan, match.captures(), e.ruleContext());
        assertThat(result).hasOperators(
                "Join[INNER | ((a = b) OR (c > a))]",
                "  ├ Collect[doc.t1 | [a] | true]",
                "  └ Collect[doc.t2 | [b] | true]");
    }

}
