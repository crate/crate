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

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.symbol.Function;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class RewriteDistinctAggToGroupByTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private LogicalPlan collectA;
    private LogicalPlan collectAX;
    private final RewriteDistinctAggToGroupBy rule = new RewriteDistinctAggToGroupBy();

    @Before
    public void setup() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(T3.T1_DEFINITION);

        collectA = e.logicalPlan("SELECT a FROM t1");
        collectAX = e.logicalPlan("SELECT a, x FROM t1");
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
    public void test_avg_distinct_is_grouped_by_the_averaged_column() {
        // SELECT avg(distinct x) FROM t1
        var collectX = e.logicalPlan("SELECT x FROM t1");
        var avgFn = (Function) e.asSymbol("avg(distinct x)");
        var hashAgg = new HashAggregate(collectX, List.of(avgFn));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[avg(DISTINCT x)]",
            "  └ Collect[doc.t1 | [x] | true]"
        );

        assertApplied(
            hashAgg,
            """
            HashAggregate[avg(DISTINCT x)]
              └ GroupHashAggregate[x]
                └ Collect[doc.t1 | [x] | true]
            """
        );
    }

    @Test
    public void test_multiple_functions_with_same_column() {
        // SELECT avg(distinct x) FROM t1
        var collectX = e.logicalPlan("SELECT x FROM t1");
        var avgFn = (Function) e.asSymbol("avg(distinct x)");
        var countFn = (Function) e.asSymbol("count(distinct x)");
        var hashAgg = new HashAggregate(collectX, List.of(avgFn, countFn));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[avg(DISTINCT x), count(DISTINCT x)]",
            "  └ Collect[doc.t1 | [x] | true]"
        );

        assertApplied(
            hashAgg,
            """
            HashAggregate[avg(DISTINCT x), count(DISTINCT x)]
              └ GroupHashAggregate[x]
                └ Collect[doc.t1 | [x] | true]
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
        var countX = (Function) e.asSymbol("count(distinct x)");
        var hashAgg = new HashAggregate(collectAX, List.of(countA, countX));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[count(DISTINCT a), count(DISTINCT x)]",
            "  └ Collect[doc.t1 | [a, x] | true]"
        );

        // No Eval here, because the join's natural output order already matches the original aggregate order.
        assertApplied(
            hashAgg,
            """
            Join[CROSS]
              ├ HashAggregate[count(DISTINCT a)]
              │  └ Collect[doc.t1 | [a, x] | true]
              └ HashAggregate[count(DISTINCT x)]
                └ Collect[doc.t1 | [a, x] | true]
            """
        );
    }

    @Test
    public void test_output_order_is_preserved_when_columns_interleave() {
        // SELECT count(distinct x), count(distinct i), avg(distinct x) FROM t1
        var collectXI = e.logicalPlan("SELECT x, i FROM t1");
        var countX = (Function) e.asSymbol("count(distinct x)");
        var countI = (Function) e.asSymbol("count(distinct i)");
        var avgX = (Function) e.asSymbol("avg(distinct x)");
        var hashAgg = new HashAggregate(collectXI, List.of(countX, countI, avgX));

        // Eval needed because the order in HashAggregates is different.
        assertApplied(
            hashAgg,
            """
            Eval[count(DISTINCT x), count(DISTINCT i), avg(DISTINCT x)]
              └ Join[CROSS]
                ├ HashAggregate[count(DISTINCT x), avg(DISTINCT x)]
                │  └ Collect[doc.t1 | [x, i] | true]
                └ HashAggregate[count(DISTINCT i)]
                  └ Collect[doc.t1 | [x, i] | true]
            """
        );
    }

    @Test
    public void test_where_clause_kept_on_different_columns_split() {
        // SELECT count(distinct a), count(distinct x) FROM t1 WHERE i > 1
        var filteredCollect = e.logicalPlan("SELECT a, x FROM t1 WHERE i > 1");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var countX = (Function) e.asSymbol("count(distinct x)");
        var hashAgg = new HashAggregate(filteredCollect, List.of(countA, countX));

        assertApplied(
            hashAgg,
            """
            Join[CROSS]
              ├ HashAggregate[count(DISTINCT a)]
              │  └ Collect[doc.t1 | [a, x] | (i > 1)]
              └ HashAggregate[count(DISTINCT x)]
                └ Collect[doc.t1 | [a, x] | (i > 1)]
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

    @Test
    public void test_join_branches_are_deduplicated_by_the_same_rule() {
        // SELECT count(distinct a), count(distinct x) FROM t1
        var countA = (Function) e.asSymbol("count(distinct a)");
        var countX = (Function) e.asSymbol("count(distinct x)");
        var hashAgg = new HashAggregate(collectAX, List.of(countA, countX));

        var match = rule.pattern().accept(hashAgg, Captures.empty());
        assertThat(match.isPresent()).isTrue();
        var result = rule.apply(match.value(), match.captures(), e.ruleContext());

        // Each join branch is a single-column distinct aggregate. On a later optimizer
        // iteration, the same rule dedups it via GROUP BY instead of splitting it again.
        assertApplied(
            result.sources().get(0),
            """
            HashAggregate[count(DISTINCT a)]
              └ GroupHashAggregate[a]
                └ Collect[doc.t1 | [a, x] | true]
            """
        );
        assertApplied(
            result.sources().get(1),
            """
            HashAggregate[count(DISTINCT x)]
              └ GroupHashAggregate[x]
                └ Collect[doc.t1 | [a, x] | true]
            """
        );
    }
}
