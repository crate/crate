/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.optimizer.rule;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.WindowAgg;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class MoveFilterBeneathWindowAggTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (id int, x int)");
    }

    @Test
    public void test_filter_on_containing_windows_function_is_not_moved() {
        var collect = e.logicalPlan("SELECT id FROM t1");

        WindowFunction windowFunction = (WindowFunction) e.asSymbol("ROW_NUMBER() OVER(PARTITION by id)");
        WindowAgg windowAgg = (WindowAgg) WindowAgg.create(collect, List.of(windowFunction));

        Symbol query = e.asSymbol("ROW_NUMBER() OVER(PARTITION by id) = 2");
        Filter filter = new Filter(windowAgg, query);

        var rule = new MoveFilterBeneathWindowAgg();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(filter);

        LogicalPlan newPlan = rule.apply(
            match.value(),
            match.captures(),
            e.planStats(),
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity(),
            mock(PlannerContext.class)
        );

        assertThat(newPlan).isNull();
    }

    @Test
    public void test_filter_containing_columns_not_part_of_the_window_partition_cannot_be_moved() {
        var collect = e.logicalPlan("SELECT id, x FROM t1");

        WindowFunction windowFunction = (WindowFunction) e.asSymbol("ROW_NUMBER() OVER(PARTITION by id)");
        WindowAgg windowAgg = (WindowAgg) WindowAgg.create(collect, List.of(windowFunction));

        Symbol query = e.asSymbol("x = 1");
        Filter filter = new Filter(windowAgg, query);

        var rule = new MoveFilterBeneathWindowAgg();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(filter);

        LogicalPlan newPlan = rule.apply(
            match.value(),
            match.captures(),
            e.planStats(),
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity(),
            mock(PlannerContext.class)
        );

        assertThat(newPlan).isNull();
    }

    @Test
    public void test_filter_on_windows_function_partition_columns_is_moved() {
        var collect = e.logicalPlan("SELECT id FROM t1");

        WindowFunction windowFunction = (WindowFunction) e.asSymbol("ROW_NUMBER() OVER(PARTITION BY id)");
        WindowAgg windowAgg = (WindowAgg) WindowAgg.create(collect, List.of(windowFunction));

        Symbol query = e.asSymbol("id = 10");
        Filter filter = new Filter(windowAgg, query);

        var rule = new MoveFilterBeneathWindowAgg();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(filter);
        LogicalPlan newPlan = rule.apply(
            match.value(),
            match.captures(),
            e.planStats(),
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity(),
            mock(PlannerContext.class)
        );
        var expectedPlan =
            """
            WindowAgg[id, row_number() OVER (PARTITION BY id)]
              └ Filter[(id = 10)]
                └ Collect[doc.t1 | [id] | true]
            """;

        assertThat(newPlan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_filter_part_on_windows_function_partition_columns_is_moved() {
        var collect = e.logicalPlan("SELECT id FROM t1");

        WindowFunction windowFunction = (WindowFunction) e.asSymbol("ROW_NUMBER() OVER(PARTITION BY id)");
        WindowAgg windowAgg = (WindowAgg) WindowAgg.create(collect, List.of(windowFunction));

        Symbol query = e.asSymbol("ROW_NUMBER() OVER(PARTITION BY id) = 2 AND id = 10 AND x = 1");
        Filter filter = new Filter(windowAgg, query);

        var rule = new MoveFilterBeneathWindowAgg();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(filter);

        LogicalPlan newPlan = rule.apply(
            match.value(),
            match.captures(),
            e.planStats(),
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity(),
            mock(PlannerContext.class)
        );
        var expectedPlan =
            """
            Filter[((row_number() OVER (PARTITION BY id) = 2) AND (x = 1))]
              └ WindowAgg[id, row_number() OVER (PARTITION BY id)]
                └ Filter[(id = 10)]
                  └ Collect[doc.t1 | [id] | true]
            """;

        assertThat(newPlan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_filters_combined_by_OR_cannot_be_moved() {
        var collect = e.logicalPlan("SELECT id FROM t1");

        WindowFunction windowFunction = (WindowFunction) e.asSymbol("ROW_NUMBER() OVER(PARTITION BY id)");
        WindowAgg windowAgg = (WindowAgg) WindowAgg.create(collect, List.of(windowFunction));

        Symbol query = e.asSymbol("ROW_NUMBER() OVER(PARTITION BY id) = 1 OR id = 10");
        Filter filter = new Filter(windowAgg, query);

        var rule = new MoveFilterBeneathWindowAgg();
        Match<Filter> match = rule.pattern().accept(filter, Captures.empty());

        assertThat(match.isPresent()).isTrue();
        assertThat(match.value()).isSameAs(filter);

        LogicalPlan newPlan = rule.apply(
            match.value(),
            match.captures(),
            e.planStats(),
            CoordinatorTxnCtx.systemTransactionContext(),
            e.nodeCtx,
            UnaryOperator.identity(),
            mock(PlannerContext.class)
        );
        assertThat(newPlan).isNull();
    }
}
