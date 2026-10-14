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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.symbol.Function;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.statistics.Stats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RewriteDistinctAggToGroupByTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private LogicalPlan collectA;
    private LogicalPlan collectAX;
    private LogicalPlan collectAB;
    private final RewriteDistinctAggToGroupBy rule = new RewriteDistinctAggToGroupBy();

    @Before
    public void setup() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("""
                create table doc.t1 (
                  a text,
                  b text,
                  x int,
                  i int
                )"""
            );

        collectA = e.logicalPlan("SELECT a FROM t1");
        collectAX = e.logicalPlan("SELECT a, x FROM t1");
        collectAB = e.logicalPlan("SELECT a, b FROM t1");
    }

    private void assertApplied(LogicalPlan plan, String expectedPlan) {
        var match = rule.pattern().accept(plan, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(plan);

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).isEqualTo(expectedPlan);
    }

    private void assertNotMatched(LogicalPlan plan) {
        var match = rule.pattern().accept(plan, Captures.empty());

        assertThat(match.isPresent()).isFalse();
    }

    /// Pattern matches structurally, but `apply` bails out (returns null), leaving `plan` untouched.
    private void assertNotApplied(LogicalPlan plan) {
        var match = rule.pattern().accept(plan, Captures.empty());
        assertThat(match.isPresent()).isTrue();

        var result = rule.apply(match.value(), match.captures(), e.ruleContext());
        assertThat(result).isNull();
    }

    private void setTableStats(long numDocs) {
        e.updateTableStats(Map.of(
            e.resolveTableInfo("t1").ident(),
            new Stats(numDocs, 0L, Map.of())
        ));
    }

    @Test
    public void test_count_distinct_is_grouped_by_the_counted_column() {
        // SELECT count(distinct a) FROM t1
        var countFn = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(collectA, List.of(countFn));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[count(DISTINCT a)]",
            "  └ Collect[doc.t1 | [a] | true]"
        );

        assertApplied(
            hashAgg,
            """
            HashAggregate[count(DISTINCT a)]
              └ GroupHashAggregate[a]
                └ Collect[doc.t1 | [a] | true]
            """
        );
    }

    @Test
    public void test_where_clause_stays_below_the_group_by() {
        // SELECT count(distinct a) FROM t1 WHERE x > 1
        var filteredCollect = e.logicalPlan("SELECT a FROM t1 WHERE x > 1");
        var countFn = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(filteredCollect, List.of(countFn));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[count(DISTINCT a)]",
            "  └ Collect[doc.t1 | [a] | (x > 1)]"
        );

        assertApplied(
            hashAgg,
            """
            HashAggregate[count(DISTINCT a)]
              └ GroupHashAggregate[a]
                └ Collect[doc.t1 | [a] | (x > 1)]
            """
        );
    }

    @Test
    public void test_cannot_apply_for_distinct_aggregate_with_filter() {
        // SELECT count(distinct a) FILTER (WHERE x > 1) FROM t1
        var countFn = (Function) e.asSymbol("count(distinct a) FILTER (WHERE x > 1)");
        var hashAgg = new HashAggregate(collectAX, List.of(countFn));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[count(DISTINCT a) FILTER (WHERE (x > 1))]",
            "  └ Collect[doc.t1 | [a, x] | true]"
        );

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_distinct_aggregate_another_aggregate_different_column() {
        // SELECT count(distinct a), sum(x) FROM t1
        var countFn = (Function) e.asSymbol("count(distinct a)");
        var sumFn = (Function) e.asSymbol("sum(x)");
        var hashAgg = new HashAggregate(collectAX, List.of(countFn, sumFn));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[count(DISTINCT a), sum(x)]",
            "  └ Collect[doc.t1 | [a, x] | true]"
        );

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_distinct_aggregate_over_scalar() {
        // SELECT count(distinct upper(a)) FROM t1
        var countFn = (Function) e.asSymbol("count(distinct upper(a))");
        var hashAgg = new HashAggregate(collectA, List.of(countFn));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[count(DISTINCT upper(a))]",
            "  └ Collect[doc.t1 | [a] | true]"
        );

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_distinct_aggregate_in_group_by() {
        // SELECT count(distinct a) FROM t1 GROUP BY a
        var countFn = (Function) e.asSymbol("count(distinct a)");
        var groupAgg = new GroupHashAggregate(collectA, List.of(e.asSymbol("a")), List.of(countFn));
        var eval = Eval.create(groupAgg, List.of(countFn));

        assertThat(eval).hasOperators(
            "Eval[count(DISTINCT a)]",
            "  └ GroupHashAggregate[a | count(DISTINCT a)]",
            "    └ Collect[doc.t1 | [a] | true]"
        );

        assertNotMatched(eval);
    }

    @Test
    public void test_cannot_apply_source_already_grouped() {
        // SELECT count(distinct a) FROM (SELECT a FROM t1 GROUP BY a) t
        var countFn = (Function) e.asSymbol("count(distinct a)");
        var groupAgg = new GroupHashAggregate(collectA, List.of(e.asSymbol("a")), List.of());
        var hashAgg = new HashAggregate(groupAgg, List.of(countFn));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[count(DISTINCT a)]",
            "  └ GroupHashAggregate[a]",
            "    └ Collect[doc.t1 | [a] | true]"
        );

        var match = rule.pattern().accept(hashAgg, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isEqualTo(hashAgg);

        var result = rule.apply(
            match.value(),
            match.captures(),
            e.ruleContext()
        );

        assertThat(result).isNull();
    }

    @Test
    public void test_reapply_rule() {
        // SELECT count(distinct a) FROM t1
        var countFn = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(collectA, List.of(countFn));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[count(DISTINCT a)]",
            "  └ Collect[doc.t1 | [a] | true]"
        );

        assertApplied(
            hashAgg,
            """
            HashAggregate[count(DISTINCT a)]
              └ GroupHashAggregate[a]
                └ Collect[doc.t1 | [a] | true]
            """
        );
        assertApplied(
            hashAgg,
            """
            HashAggregate[count(DISTINCT a)]
              └ GroupHashAggregate[a]
                └ Collect[doc.t1 | [a] | true]
            """
        );
    }

    @Test
    public void test_distinct_aggregates_on_different_columns_are_split_and_joined() {
        // SELECT count(distinct a), count(distinct x) FROM t1
        var countA = (Function) e.asSymbol("count(distinct a)");
        var countB = (Function) e.asSymbol("count(distinct b)");
        var hashAgg = new HashAggregate(collectAB, List.of(countA, countB));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[count(DISTINCT a), count(DISTINCT b)]",
            "  └ Collect[doc.t1 | [a, b] | true]"
        );

        setTableStats(1000);
        // No Eval here, because the join's natural output order already matches the original aggregate order.
        assertApplied(
            hashAgg,
            """
            Join[CROSS]
              ├ HashAggregate[count(DISTINCT a)]
              │  └ Collect[doc.t1 | [a, b] | true]
              └ HashAggregate[count(DISTINCT b)]
                └ Collect[doc.t1 | [a, b] | true]
            """
        );

        // Each join side is a single-column distinct aggregate. On a later optimizer
        // iteration, the same rule dedups it via GROUP BY instead of splitting it again.
        var match = rule.pattern().accept(hashAgg, Captures.empty());
        var result = rule.apply(match.value(), match.captures(), e.ruleContext());
        assertApplied(
            result.sources().get(0),
            """
            HashAggregate[count(DISTINCT a)]
              └ GroupHashAggregate[a]
                └ Collect[doc.t1 | [a, b] | true]
            """
        );
        assertApplied(
            result.sources().get(1),
            """
            HashAggregate[count(DISTINCT b)]
              └ GroupHashAggregate[b]
                └ Collect[doc.t1 | [a, b] | true]
            """
        );
    }

    @Test
    public void test_not_applied_when_table_is_too_large() {
        // SELECT count(distinct a), count(distinct x) FROM t1
        setTableStats((long) (RewriteDistinctAggToGroupBy.MAX_TABLE_SIZE + 1));

        var countA = (Function) e.asSymbol("count(distinct a)");
        var countB = (Function) e.asSymbol("count(distinct b)");
        var hashAgg = new HashAggregate(collectAB, List.of(countA, countB));

        assertNotApplied(hashAgg);
    }

    @Test
    public void test_where_clause_kept_on_different_columns_split() {
        // SELECT count(distinct a), count(distinct x) FROM t1 WHERE i > 1
        var filteredCollect = e.logicalPlan("SELECT a, b FROM t1 WHERE i > 1");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var countB = (Function) e.asSymbol("count(distinct b)");
        var hashAgg = new HashAggregate(filteredCollect, List.of(countA, countB));

        setTableStats(1000);
        assertApplied(
            hashAgg,
            """
            Join[CROSS]
              ├ HashAggregate[count(DISTINCT a)]
              │  └ Collect[doc.t1 | [a, b] | (i > 1)]
              └ HashAggregate[count(DISTINCT b)]
                └ Collect[doc.t1 | [a, b] | (i > 1)]
            """
        );
    }

    @Test
    public void test_cannot_apply_for_distinct_aggregate_with_filter_on_different_columns() {
        // SELECT count(distinct a), count(distinct x) FILTER (WHERE x > 1) FROM t1
        var countA = (Function) e.asSymbol("count(distinct a)");
        var countX = (Function) e.asSymbol("count(distinct x) FILTER (WHERE x > 1)");
        var hashAgg = new HashAggregate(collectAX, List.of(countA, countX));

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_for_distinct_aggregate_over_scalar_on_multiple_columns() {
        // SELECT count(distinct upper(a)), count(distinct x) FROM t1
        var countUpperA = (Function) e.asSymbol("count(distinct upper(a))");
        var countX = (Function) e.asSymbol("count(distinct x)");
        var hashAgg = new HashAggregate(collectAX, List.of(countUpperA, countX));

        assertNotMatched(hashAgg);
    }
}
