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

package io.crate.planner;

import static io.crate.analyze.TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT;
import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.analyze.TableDefinitions;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.metadata.RelationName;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.LogicalPlannerTest;
import io.crate.planner.operators.Union;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class UnionPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_DEFINITION)
            .build();
    }

    @Test
    public void testSimpleUnion() {
        ExecutionPlan plan = e.plan(
            "select id from users " +
            "union all " +
            "select id from locations ");
        assertThat(plan, instanceOf(UnionExecutionPlan.class));
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.orderBy(), is(nullValue()));
        assertThat(unionExecutionPlan.mergePhase().numInputs(), is(2));
        assertThat(unionExecutionPlan.left(), instanceOf(Collect.class));
        assertThat(unionExecutionPlan.right(), instanceOf(Collect.class));
    }

    @Test
    public void testUnionWithOrderByLimit() {
        ExecutionPlan plan = e.plan(
            "select id from users " +
            "union all " +
            "select id from locations " +
            "order by id limit 2");
        assertThat(plan, instanceOf(UnionExecutionPlan.class));
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.mergePhase().numInputs(), is(2));
        assertThat(unionExecutionPlan.mergePhase().orderByPositions(), instanceOf(PositionalOrderBy.class));
        assertThat(unionExecutionPlan.mergePhase().projections(), contains(
            instanceOf(LimitAndOffsetProjection.class)
        ));
        assertThat(unionExecutionPlan.left(), instanceOf(Collect.class));
        assertThat(unionExecutionPlan.right(), instanceOf(Collect.class));
    }

    @Test
    public void testUnionWithSubselects() {
        ExecutionPlan plan = e.plan(
            "select * from (select id from users order by id limit 2) a " +
            "union all " +
            "select id from locations " +
            "order by id limit 2");
        assertThat(plan, instanceOf(UnionExecutionPlan.class));
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.mergePhase().numInputs(), is(2));
        assertThat(unionExecutionPlan.orderBy(), Matchers.notNullValue());
        assertThat(unionExecutionPlan.mergePhase().projections(), contains(
            instanceOf(LimitAndOffsetProjection.class)
        ));
        assertThat(unionExecutionPlan.left(), instanceOf(Merge.class));
        Merge merge = (Merge) unionExecutionPlan.left();
        assertThat(merge.subPlan(), instanceOf(Collect.class));
        assertThat(unionExecutionPlan.right(), instanceOf(Collect.class));
    }

    @Test
    public void testUnionWithOrderByLiteralConstant() {
        String stmt = "select * from (" +
            " select 1 as x, id from users" +
            " union all" +
            " select 2, id from users" +
            ") o" +
            " order by x";
        var logicalPlan = e.logicalPlan(stmt);
        String expectedPlan =
            "Rename[x, id] AS o\n" +
            "  └ Union[x, id]\n" +
            "    ├ OrderBy[1 AS x ASC]\n" +
            "    │  └ Collect[doc.users | [1 AS x, id] | true]\n" +
            "    └ OrderBy[2 ASC]\n" +
            "      └ Collect[doc.users | [2, id] | true]";
        assertThat(logicalPlan, is(LogicalPlannerTest.isPlan(expectedPlan)));
        ExecutionPlan plan = e.plan(stmt);
        assertThat(plan, instanceOf(UnionExecutionPlan.class));
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.mergePhase().orderByPositions(), instanceOf(PositionalOrderBy.class));
        assertThat(unionExecutionPlan.mergePhase().orderByPositions().indices(), is(new int[]{0}));
    }

    @Test
    public void test_select_subset_of_outputs_from_union() {
        String stmt = "select x from (" +
                      " select 1 as x, id from users" +
                      " union all" +
                      " select 2, id from users" +
                      ") o" +
                      " order by x";
        var logicalPlan = e.logicalPlan(stmt);
        String expectedPlan =
            "Rename[x] AS o\n" +
            "  └ Union[x]\n" +
            "    ├ OrderBy[1 AS x ASC]\n" +
            "    │  └ Collect[doc.users | [1 AS x] | true]\n" +
            "    └ OrderBy[2 ASC]\n" +
            "      └ Collect[doc.users | [2] | true]";
        assertThat(logicalPlan, is(LogicalPlannerTest.isPlan(expectedPlan)));
    }

    @Test
    public void testUnionDistinct() {
        var logicalPlan = e.logicalPlan(
            "select id from users " +
            "union distinct " +
            "select id from locations ");
        String expectedPlan =
            "GroupHashAggregate[id]\n" +
            "  └ Union[id]\n" +
            "    ├ Collect[doc.users | [id] | true]\n" +
            "    └ Collect[doc.locations | [id] | true]";
        assertThat(logicalPlan, is(LogicalPlannerTest.isPlan(expectedPlan)));
    }

    @Test
    public void test_union_with_different_types_in_queries_adds_casts() {
        UnionExecutionPlan union = e.plan("select null union all select id from users");
        Collect left = (Collect) union.left();
        assertThat(left.collectPhase().projections(), contains(
            instanceOf(EvalProjection.class), // returns NULL
            instanceOf(EvalProjection.class)  // casts NULL to long to match `id`
        ));
        Collect right = (Collect) union.right();
        assertThat(left.streamOutputs(), is(right.streamOutputs()));
        assertThat(left.streamOutputs(), contains(
            is(DataTypes.LONG)
        ));


        union = e.plan("select id from users union all select null");
        left = (Collect) union.left();
        right = (Collect) union.right();
        assertThat(left.streamOutputs(), is(right.streamOutputs()));
        assertThat(right.streamOutputs(), contains(
            is(DataTypes.LONG)
        ));
        assertThat(right.collectPhase().projections(), contains(
            instanceOf(EvalProjection.class), // returns NULL
            instanceOf(EvalProjection.class)  // casts NULL to long to match `id`
        ));
    }

    @Test
    public void test_union_returns_unknown_expected_rows_unknown_on_one_source_plan() {
        String stmt = "select id from users union all select id from locations";
        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(1, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        var context = e.getPlannerContext(clusterService.state());
        var logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            tableStats,
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        var plan = logicalPlanner.plan(e.analyze(stmt), context);
        var union = (Union) plan.sources().get(0);
        assertThat(union.numExpectedRows(), is(-1L));
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(1, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);
        plan = logicalPlanner.plan(e.analyze(stmt), context);
        union = (Union) plan.sources().get(0);
        assertThat(union.numExpectedRows(), is(-1L));
    }

}
