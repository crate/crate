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

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.sql.tree.JoinType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RewriteFilterOnCrossJoinToInnerJoinTest extends CrateDummyClusterServiceUnitTest {

    private LogicalPlan t1;
    private LogicalPlan t2;
    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (a int)")
            .addTable("create table t2 (b int)");

        t1 = e.logicalPlan("SELECT a FROM t1");
        t2 = e.logicalPlan("SELECT b FROM t2");
    }

    @Test
    public void test_rewrite_equ_join_filter_on_top_of_cross_join_to_inner_join() {
        var join1 = new JoinPlan(t1, t2, JoinType.CROSS, null);
        var filter = new Filter(join1, e.asSymbol("doc.t1.a = doc.t2.b"));

        assertThat(filter).hasOperators(
            "Filter[(a = b)]",
            "  └ Join[CROSS]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnCrossJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        Assertions.assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | (a = b)]",
            "  ├ Collect[doc.t1 | [a] | true]",
            "  └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_rewrite_equ_join_filter_on_top_of_cross_join_to_inner_join_on_split_query() {
        var join1 = new JoinPlan(t1, t2, JoinType.CROSS, null);
        var filter = new Filter(join1, e.asSymbol("doc.t1.a = doc.t2.b and doc.t1.a = 10"));

        assertThat(filter).hasOperators(
            "Filter[((a = b) AND (a = 10))]",
            "  └ Join[CROSS]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnCrossJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        Assertions.assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            e.ruleContext());

        assertThat(result).hasOperators(
            "Join[INNER | ((a = b) AND (a = 10))]",
            "  ├ Collect[doc.t1 | [a] | true]",
            "  └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_do_not_rewrite_non_equi_join_filter_on_top_of_cross_join_when_filter_does_not_match() {
        var join1 = new JoinPlan(t1, t2, JoinType.CROSS, null);
        var filter = new Filter(join1, e.asSymbol("doc.t1.a = 1"));

        assertThat(filter).hasOperators(
            "Filter[(a = 1)]",
            "  └ Join[CROSS]",
            "    ├ Collect[doc.t1 | [a] | true]",
            "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new RewriteFilterOnCrossJoinToInnerJoin();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        Assertions.assertThat(match.isPresent()).isFalse();
    }
}
