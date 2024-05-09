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

package io.crate.planner.optimizer.iterative;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.createNodeContext;

import java.util.List;

import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.Order;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.rule.DeduplicateOrder;
import io.crate.planner.optimizer.rule.MergeFilters;
import io.crate.planner.optimizer.rule.MoveFilterBeneathOrder;
import io.crate.planner.optimizer.tracer.OptimizerTracer;
import io.crate.statistics.TableStats;

public class IterativeOptimizerTest {

    private final NodeContext nodeCtx = createNodeContext(null);
    private final CoordinatorTxnCtx ctx = CoordinatorTxnCtx.systemTransactionContext();
    private final PlanStats planStats = new PlanStats(nodeCtx, CoordinatorTxnCtx.systemTransactionContext(), new TableStats());

    @Test
    public void test_match_single_rule_merge_filters() {
        var source = new MemoTest.TestPlan(1, List.of());
        var filter1 = new Filter(source, Literal.BOOLEAN_TRUE);
        var filter2 = new Filter(filter1, Literal.BOOLEAN_TRUE);

        IterativeOptimizer optimizer = new IterativeOptimizer(nodeCtx,
                                                              () -> Version.CURRENT,
                                                              List.of(new MergeFilters()));

        var result = optimizer.optimize(filter2, planStats, ctx, OptimizerTracer.NOOP);
        assertThat(result).isEqualTo("Filter[(true AND true)]\n" +
                                             "  └ TestPlan[]");
    }

    @Test
    public void test_match_multiple_rules_merge_filters_and_orders() {
        var source = new MemoTest.TestPlan(1, List.of());
        var filter1 = new Filter(source, Literal.BOOLEAN_TRUE);
        var filter2 = new Filter(filter1, Literal.BOOLEAN_TRUE);
        var order1 = new Order(filter2, new OrderBy(List.of()));
        var order2 = new Order(order1, new OrderBy(List.of()));

        IterativeOptimizer optimizer = new IterativeOptimizer(nodeCtx,
                                                              () -> Version.CURRENT,
                                                              List.of(new MergeFilters(), new DeduplicateOrder()));

        var result = optimizer.optimize(order2, planStats, ctx, OptimizerTracer.NOOP);
        assertThat(result).isEqualTo(
            """
            OrderBy[]
              └ Filter[(true AND true)]
                └ TestPlan[]
            """
        );
    }

    @Test
    public void test_reapply_rules_on_changing_nodes() {
        // This tests the following scenario:
        //  OrderBy[]
        //    └ Filter[true]
        //      └ OrderBy[]
        //        └ TestPlan[]
        // 1. Move the filter beneath order1
        //   OrderBy[]
        //    └ OrderBy[]
        //      └ Filter[true]
        //        └ TestPlan[]
        // 2. Now match the next rule and dedup the orders
        //   OrderBy[]
        //     └ Filter[true]
        //       └ TestPlan[]
        var source = new MemoTest.TestPlan(1, List.of());
        var order1 = new Order(source, new OrderBy(List.of()));
        var filter = new Filter(order1, Literal.BOOLEAN_TRUE);
        var order2 = new Order(filter, new OrderBy(List.of()));

        IterativeOptimizer optimizer = new IterativeOptimizer(nodeCtx,
                                                              () -> Version.CURRENT,
                                                              List.of(new MoveFilterBeneathOrder(), new DeduplicateOrder()));

        var result = optimizer.optimize(order2, planStats, ctx, OptimizerTracer.NOOP);

        assertThat(result).isEqualTo(
            """
            OrderBy[]
              └ Filter[true]
                └ TestPlan[]
            """
        );
    }

}

