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
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class RewriteMixedDistinctAggToGroupByTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private LogicalPlan collectAX;
    private final RewriteMixedDistinctAggToGroupBy rule = new RewriteMixedDistinctAggToGroupBy();

    @Before
    public void setup() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(T3.T1_DEFINITION);

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
    public void test_sum_and_count_distinct_are_grouped_by_the_distinct_column() {
        // SELECT sum(x), count(distinct a) FROM t1
        var sumX = (Function) e.asSymbol("sum(x)");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(collectAX, List.of(sumX, countA));

        assertThat(hashAgg).hasOperators(
            "HashAggregate[sum(x), count(DISTINCT a)]",
            "  └ Collect[doc.t1 | [a, x] | true]"
        );

        assertApplied(
            hashAgg,
            """
            HashAggregate[sum(x), count(DISTINCT a)]
              └ GroupHashAggregate[a | sum(x)]
                └ Collect[doc.t1 | [a, x] | true]
            """
        );
    }

    @Test
    public void test_count_star_can_be_mixed_with_a_distinct_aggregate() {
        // SELECT count(*), count(distinct a) FROM t1
        var countStar = (Function) e.asSymbol("count(*)");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(collectAX, List.of(countStar, countA));

        assertApplied(
            hashAgg,
            """
            HashAggregate[count(*), count(DISTINCT a)]
              └ GroupHashAggregate[a | count(*)]
                └ Collect[doc.t1 | [a, x] | true]
            """
        );
    }

    @Test
    public void test_min_and_max_are_grouped_by_the_distinct_column() {
        // SELECT min(x), max(i), count(distinct a) FROM t1
        var collectAXI = e.logicalPlan("SELECT a, x, i FROM t1");
        var minX = (Function) e.asSymbol("min(x)");
        var maxI = (Function) e.asSymbol("max(i)");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(collectAXI, List.of(minX, maxI, countA));

        assertApplied(
            hashAgg,
            """
            HashAggregate[min(x), max(i), count(DISTINCT a)]
              └ GroupHashAggregate[a | min(x), max(i)]
                └ Collect[doc.t1 | [a, x, i] | true]
            """
        );
    }

    @Test
    public void test_avg_non_distinct_is_split_into_sum_and_count_partials() {
        // SELECT avg(x), count(distinct a) FROM t1
        var avgX = (Function) e.asSymbol("avg(x)");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(collectAX, List.of(avgX, countA));

        assertApplied(
            hashAgg,
            """
            HashAggregate[avg(x), count(DISTINCT a)]
              └ GroupHashAggregate[a | sum(x), count(x)]
                └ Collect[doc.t1 | [a, x] | true]
            """
        );
    }

    @Test
    public void test_distinct_avg_is_not_split_while_non_distinct_column_is_split() {
        // SELECT count(x), avg(distinct a) FROM t1
        var countX = (Function) e.asSymbol("count(x)");
        var avgDistinctI = (Function) e.asSymbol("avg(distinct i)");
        var hashAgg = new HashAggregate(e.logicalPlan("SELECT i, x FROM t1"), List.of(countX, avgDistinctI));

        assertApplied(
            hashAgg,
            """
            HashAggregate[count(x), avg(DISTINCT i)]
              └ GroupHashAggregate[i | count(x)]
                └ Collect[doc.t1 | [i, x] | true]
            """
        );
    }

    @Test
    public void test_where_clause_stays_below_the_group_by() {
        // SELECT sum(x), count(distinct a) FROM t1 WHERE i > 1
        var filteredCollect = e.logicalPlan("SELECT a, x FROM t1 WHERE i > 1");
        var sumX = (Function) e.asSymbol("sum(x)");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(filteredCollect, List.of(sumX, countA));

        assertApplied(
            hashAgg,
            """
            HashAggregate[sum(x), count(DISTINCT a)]
              └ GroupHashAggregate[a | sum(x)]
                └ Collect[doc.t1 | [a, x] | (i > 1)]
            """
        );
    }

    @Test
    public void test_cannot_apply_again() {
        // SELECT sum(x), count(distinct a) FROM t1
        var sumX = (Function) e.asSymbol("sum(x)");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(collectAX, List.of(sumX, countA));

        var match = rule.pattern().accept(hashAgg, Captures.empty());
        var result = rule.apply(match.value(), match.captures(), e.ruleContext());

        // distinctMode is now SPLIT_AND_MERGE, so the pattern no longer matches.
        assertNotMatched(result);
    }

    @Test
    public void test_cannot_apply_without_a_distinct_aggregate() {
        // SELECT sum(x), count(a) FROM t1
        var sumX = (Function) e.asSymbol("sum(x)");
        var countA = (Function) e.asSymbol("count(a)");
        var hashAgg = new HashAggregate(collectAX, List.of(sumX, countA));

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_for_distinct_aggregates_on_different_columns() {
        // SELECT sum(x), count(distinct a), count(distinct i) FROM t1
        var collectAXI = e.logicalPlan("SELECT a, x, i FROM t1");
        var sumX = (Function) e.asSymbol("sum(x)");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var countI = (Function) e.asSymbol("count(distinct i)");
        var hashAgg = new HashAggregate(collectAXI, List.of(sumX, countA, countI));

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_for_unsupported_distinct_aggregate() {
        // SELECT sum(x), min(distinct a) FROM t1 (min/max distinct not supported by RewriteDistinctAggToGroupBy)
        var sumX = (Function) e.asSymbol("sum(x)");
        var minDistinctA = (Function) e.asSymbol("min(distinct a)");
        var hashAgg = new HashAggregate(collectAX, List.of(sumX, minDistinctA));

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_for_unsupported_non_distinct_aggregate() {
        // SELECT variance(x), count(distinct a) FROM t1
        var varianceX = (Function) e.asSymbol("variance(x)");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(collectAX, List.of(varianceX, countA));

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_for_distinct_aggregate_over_scalar() {
        // SELECT sum(x), count(distinct upper(a)) FROM t1
        var sumX = (Function) e.asSymbol("sum(x)");
        var countUpperA = (Function) e.asSymbol("count(distinct upper(a))");
        var hashAgg = new HashAggregate(collectAX, List.of(sumX, countUpperA));

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_for_non_distinct_aggregate_over_scalar() {
        // SELECT count(upper(a)), count(distinct x) FROM t1
        var countUpperA = (Function) e.asSymbol("count(upper(a))");
        var countX = (Function) e.asSymbol("count(distinct x)");
        var hashAgg = new HashAggregate(collectAX, List.of(countUpperA, countX));

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_for_distinct_aggregate_with_filter() {
        // SELECT sum(x), count(distinct a) FILTER (WHERE x > 1) FROM t1
        var sumX = (Function) e.asSymbol("sum(x)");
        var countA = (Function) e.asSymbol("count(distinct a) FILTER (WHERE x > 1)");
        var hashAgg = new HashAggregate(collectAX, List.of(sumX, countA));

        assertNotMatched(hashAgg);
    }

    @Test
    public void test_cannot_apply_for_non_distinct_aggregate_with_filter() {
        // SELECT sum(x) FILTER (WHERE x > 1), count(distinct a) FROM t1
        var sumX = (Function) e.asSymbol("sum(x) FILTER (WHERE x > 1)");
        var countA = (Function) e.asSymbol("count(distinct a)");
        var hashAgg = new HashAggregate(collectAX, List.of(sumX, countA));

        assertNotMatched(hashAgg);
    }
}
