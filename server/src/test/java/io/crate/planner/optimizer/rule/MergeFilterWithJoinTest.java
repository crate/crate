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

public class MergeFilterWithJoinTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;
    private Map<RelationName, AnalyzedRelation> sources;
    private PlanStats planStats;
    private LogicalPlan t1;
    private LogicalPlan t2;
    private LogicalPlan t3;

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
            .addTable("create table t3 (c int)")
            .build();

        t1 = e.logicalPlan("SELECT a FROM t1");
        t2 = e.logicalPlan("SELECT b FROM t2");
        t3 = e.logicalPlan("SELECT c FROM t3");
    }

    @Test
    public void test_push_equi_joins() {
        var join = new JoinPlan(t1, t2, JoinType.CROSS, null);
        var filter = new Filter(join, sqlExpressions.asSymbol("doc.t1.a = doc.t2.b"));

        assertThat(filter).isEqualTo(
            "Filter[(a = b)]\n" +
                "  └ Join[CROSS]\n" +
                "    ├ Collect[doc.t1 | [a] | true]\n" +
                "    └ Collect[doc.t2 | [b] | true]"
        );

        var rule = new MergeFilterWithJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).isEqualTo(
            "Join[INNER | (a = b)]\n" +
                "  ├ Collect[doc.t1 | [a] | true]\n" +
                "  └ Collect[doc.t2 | [b] | true]"
        );
    }

    @Test
    public void test_push_equi_joins_on_nested_joins_1() {
        var join1 = new JoinPlan(t1, t2, JoinType.CROSS, null);
        var join2 = new JoinPlan(join1, t3, JoinType.CROSS, null);
        var filter = new Filter(join2, sqlExpressions.asSymbol("doc.t1.a = doc.t2.b"));

        assertThat(filter).isEqualTo(
            "Filter[(a = b)]\n" +
                "  └ Join[CROSS]\n" +
                "    ├ Join[CROSS]\n" +
                "    │  ├ Collect[doc.t1 | [a] | true]\n" +
                "    │  └ Collect[doc.t2 | [b] | true]\n" +
                "    └ Collect[doc.t3 | [c] | true]"
        );

        var rule = new MergeFilterWithJoin();
        var match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(filter);

        var result = rule.apply(match.value(),
            match.captures(),
            planStats,
            CoordinatorTxnCtx.systemTransactionContext(),
            sqlExpressions.nodeCtx,
            UnaryOperator.identity());

        assertThat(result).isEqualTo(
                " Join[INNER | (a = b)]\n" +
                "  ├ Join[CROSS]\n" +
                "  │  ├ Collect[doc.t1 | [a] | true]\n" +
                "  │  └ Collect[doc.t2 | [b] | true]\n" +
                "  └ Collect[doc.t3 | [c] | true]"
        );
    }

    @Test
    public void test_push_equi_joins_on_nested_joins_2() {
        var join1 = new JoinPlan(t1, t2, JoinType.CROSS, null);
        var join2 = new JoinPlan(join1, t3, JoinType.CROSS, null);
        var filter = new Filter(join2, sqlExpressions.asSymbol("doc.t1.a = doc.t2.b AND doc.t2.b = doc.t3.c"));

        assertThat(filter).isEqualTo(
            "Filter[((a = b) AND (b = c))]\n" +
                "  └ Join[CROSS]\n" +
                "    ├ Join[CROSS]\n" +
                "    │  ├ Collect[doc.t1 | [a] | true]\n" +
                "    │  └ Collect[doc.t2 | [b] | true]\n" +
                "    └ Collect[doc.t3 | [c] | true]"
        );

        var rule = new MergeFilterWithJoin();
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
}
