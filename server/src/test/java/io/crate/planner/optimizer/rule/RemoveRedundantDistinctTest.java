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

package io.crate.planner.optimizer.rule;

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.HashAggregate;
import io.crate.planner.operators.LimitDistinct;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.RootRelationBoundary;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RemoveRedundantDistinctTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private final RemoveRedundantDistinct rule = new RemoveRedundantDistinct();

    @Before
    public void setup() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table doc.pk (id int primary key, name text)")
            .addTable("create table doc.no_pk (x int, y text)");
    }

    private LogicalPlan apply(LogicalPlan plan) {
        var match = rule.pattern().accept(plan, Captures.empty());
        assertThat(match.isPresent()).isTrue();
        return rule.apply(match.value(), match.captures(), e.ruleContext());
    }

    /// `SQLExecutor.logicalPlan` wraps the plan in a RootRelationBoundary; return the Collect beneath it.
    private LogicalPlan collect(String stmt) {
        LogicalPlan plan = e.logicalPlan(stmt);
        return plan instanceof RootRelationBoundary boundary ? boundary.source() : plan;
    }

    private static LimitDistinct limitDistinct(LogicalPlan source) {
        return new LimitDistinct(source, Literal.of(10), Literal.of(5), source.outputs());
    }

    @Test
    public void test_group_by_on_primary_key_is_removed() {
        LogicalPlan collect = collect("SELECT id, name FROM pk");
        var groupBy = new GroupHashAggregate(collect, collect.outputs(), List.of());

        assertThat(apply(groupBy)).isEqualTo("Collect[doc.pk | [id, name] | true]");
    }

    @Test
    public void test_limit_distinct_on_primary_key_is_replaced_by_limit() {
        LogicalPlan collect = collect("SELECT id, name FROM pk");

        assertThat(apply(limitDistinct(collect))).isEqualTo(
            """
            Limit[10;5]
              └ Collect[doc.pk | [id, name] | true]
            """
        );
    }

    @Test
    public void test_limit_distinct_without_primary_key_is_kept() {
        LogicalPlan collect = collect("SELECT x, y FROM no_pk");

        assertThat(apply(limitDistinct(collect))).isNull();
    }

    @Test
    public void test_limit_distinct_on_part_of_primary_key_is_kept() {
        LogicalPlan collect = collect("SELECT name FROM pk");

        assertThat(apply(limitDistinct(collect))).isNull();
    }

    @Test
    public void test_group_by_with_aggregates_does_not_match() {
        LogicalPlan collect = collect("SELECT id, name FROM pk");
        var countFn = (Function) e.asSymbol("count(name)");
        var groupBy = new GroupHashAggregate(collect, List.of(collect.outputs().getFirst()), List.of(countFn));

        assertThat(rule.pattern().accept(groupBy, Captures.empty()).isPresent()).isFalse();
    }

    @Test
    public void test_other_forwarding_plans_do_not_match() {
        LogicalPlan collect = collect("SELECT id, name FROM pk");
        var countFn = (Function) e.asSymbol("count(name)");
        var hashAgg = new HashAggregate(collect, List.of(countFn));

        assertThat(rule.pattern().accept(hashAgg, Captures.empty()).isPresent()).isFalse();
    }
}
