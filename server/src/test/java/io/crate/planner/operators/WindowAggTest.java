/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.operators;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isReference;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class WindowAggTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void init() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE t1 (x int, y int)");
    }

    private LogicalPlan plan(String statement) {
        return e.logicalPlan(statement);
    }

    @Test
    public void testTwoWindowFunctionsWithDifferentWindowDefinitionResultsInTwoOperators() {
        LogicalPlan plan = plan("SELECT avg(x) OVER (PARTITION BY x), avg(x) OVER (PARTITION BY y) FROM t1");
        var expectedPlan =
            """
            Eval[avg(x) OVER (PARTITION BY x), avg(x) OVER (PARTITION BY y)]
              └ WindowAgg[x, y, avg(x) OVER (PARTITION BY x), avg(x) OVER (PARTITION BY y)]
                └ WindowAgg[x, y, avg(x) OVER (PARTITION BY x)]
                  └ Collect[doc.t1 | [x, y] | true]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_window_agg_output_for_select_with_standalone_ref_and_window_func_with_filter() {
        var plan = plan("SELECT y, AVG(x) FILTER (WHERE x > 1) OVER() FROM t1");
        var expectedPlan =
            """
            Eval[y, avg(x) FILTER (WHERE (x > 1)) OVER ()]
              └ WindowAgg[x, (x > 1), y, avg(x) FILTER (WHERE (x > 1)) OVER ()]
                └ Collect[doc.t1 | [x, (x > 1), y] | true]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_window_agg_with_filter_that_contains_column_that_is_not_in_outputs() {
        var plan = plan("SELECT x, COUNT(*) FILTER (WHERE y > 1) OVER() FROM t1");
        var expectedPlan =
            """
            Eval[x, count(*) FILTER (WHERE (y > 1)) OVER ()]
              └ WindowAgg[(y > 1), x, count(*) FILTER (WHERE (y > 1)) OVER ()]
                └ Collect[doc.t1 | [(y > 1), x] | true]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_window_agg_is_removed_if_unused_in_upper_select() {
        var plan = plan("SELECT x FROM (SELECT x, ROW_NUMBER() OVER (PARTITION BY y) FROM t1) t");
        var expectedPlan =
            "Rename[x] AS t\n" +
            "  └ Collect[doc.t1 | [x] | true]";
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void testNoOrderByIfNoPartitionsAndNoOrderBy() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER ()"));
        assertThat(orderBy).isNull();
    }

    @Test
    public void testOrderByIsOverOrderByWithoutPartitions() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (ORDER BY x)"));
        assertThat(orderBy).isNotNull();
        assertThat(orderBy.orderBySymbols()).satisfiesExactly(isReference("x"));
    }

    @Test
    public void testOrderByIsPartitionByWithoutExplicitOrderBy() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY x)"));
        assertThat(orderBy).isNotNull();
        assertThat(orderBy.orderBySymbols()).satisfiesExactly(isReference("x"));
    }

    @Test
    public void testOrderByIsMergedWithPartitionByWithFullColumnOverlap() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY x ORDER BY x)"));
        assertThat(orderBy).isNotNull();
        assertThat(orderBy.orderBySymbols()).satisfiesExactly(isReference("x"));
    }

    @Test
    public void testOrderByIsMergedWithPartitionByWithPartialColumnOverlap() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY x, y ORDER BY x)"));
        assertThat(orderBy).isNotNull();
        assertThat(orderBy.orderBySymbols()).satisfiesExactly(isReference("x"), isReference("y"));
    }

    @Test
    public void testOrderByIsMergedWithPartitionByWithPartialColumnOverlapButReverseOrder() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY y, x ORDER BY x)"));
        assertThat(orderBy).isNotNull();
        assertThat(orderBy.orderBySymbols()).satisfiesExactly(isReference("y"), isReference("x"));
    }

    @Test
    public void testOrderByIsMergedWithPartitionByWithNoOverlap() {
        OrderBy orderBy = WindowAgg.createOrderByInclPartitionBy(wd("avg(x) OVER (PARTITION BY y ORDER BY x)"));
        assertThat(orderBy).isNotNull();
        assertThat(orderBy.orderBySymbols()).satisfiesExactly(isReference("y"), isReference("x"));
    }

    private WindowDefinition wd(String expression) {
        Symbol symbol = e.asSymbol(expression);
        assertThat(symbol).isExactlyInstanceOf(WindowFunction.class);
        return ((WindowFunction) symbol).windowDefinition();
    }
}
