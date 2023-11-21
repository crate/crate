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
import static io.crate.testing.Asserts.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.analyze.TableDefinitions;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.metadata.RelationName;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
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
            """
            SELECT id FROM users
            UNION ALL
            SELECT id FROM locations
            """);
        assertThat(plan).isExactlyInstanceOf(UnionExecutionPlan.class);
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.orderBy()).isNull();
        assertThat(unionExecutionPlan.mergePhase().numInputs()).isEqualTo(2);
        assertThat(unionExecutionPlan.left()).isExactlyInstanceOf(Collect.class);
        assertThat(unionExecutionPlan.right()).isExactlyInstanceOf(Collect.class);
    }

    @Test
    public void testUnionWithOrderByLimit() {
        ExecutionPlan plan = e.plan(
            """
            SELECT id FROM users
            UNION ALL
            SELECT id FROM locations
            ORDER BY id
            LIMIT 2
            """
        );
        assertThat(plan).isExactlyInstanceOf(UnionExecutionPlan.class);
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.mergePhase().numInputs()).isEqualTo(2);
        assertThat(unionExecutionPlan.mergePhase().orderByPositions()).isExactlyInstanceOf(PositionalOrderBy.class);
        assertThat(unionExecutionPlan.mergePhase().projections())
            .satisfiesExactly(p -> assertThat(p).isExactlyInstanceOf(LimitAndOffsetProjection.class));
        assertThat(unionExecutionPlan.left()).isExactlyInstanceOf(Collect.class);
        assertThat(unionExecutionPlan.right()).isExactlyInstanceOf(Collect.class);
    }

    @Test
    public void testUnionWithSubselects() {
        ExecutionPlan plan = e.plan(
            """
            SELECT * FROM (
              SELECT id FROM users
              ORDER BY id
              LIMIT 2) a
            UNION ALL
            SELECT id FROM locations
            ORDER BY id
            LIMIT 2
            """
        );
        assertThat(plan).isExactlyInstanceOf(UnionExecutionPlan.class);
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.mergePhase().numInputs()).isEqualTo(2);
        assertThat(unionExecutionPlan.orderBy()).isNotNull();
        assertThat(unionExecutionPlan.mergePhase().projections())
            .satisfiesExactly(p -> assertThat(p).isExactlyInstanceOf(LimitAndOffsetProjection.class));
        assertThat(unionExecutionPlan.left()).isExactlyInstanceOf(Merge.class);
        Merge merge = (Merge) unionExecutionPlan.left();
        assertThat(merge.subPlan()).isExactlyInstanceOf(Collect.class);
        assertThat(unionExecutionPlan.right()).isExactlyInstanceOf(Collect.class);
    }

    @Test
    public void testUnionWithOrderByLiteralConstant() {
        String stmt =
            """
            SELECT * FROM (
              SELECT 1 as x, id FROM users
              UNION ALL
              SELECT 2, id FROM users
              ) o
            ORDER BY x
            """;
        var logicalPlan = e.logicalPlan(stmt);
        String expectedPlan =
            """
            Rename[x, id] AS o
              └ Union[x, id]
                ├ OrderBy[1 AS x ASC]
                │  └ Collect[doc.users | [1 AS x, id] | true]
                └ OrderBy[2 ASC]
                  └ Collect[doc.users | [2, id] | true]
            """;
        assertThat(logicalPlan).isEqualTo(expectedPlan);
        ExecutionPlan plan = e.plan(stmt);
        assertThat(plan).isExactlyInstanceOf(UnionExecutionPlan.class);
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.mergePhase().orderByPositions()).isExactlyInstanceOf(PositionalOrderBy.class);
        assertThat(unionExecutionPlan.mergePhase().orderByPositions().indices()).isEqualTo(new int[]{0});
    }

    @Test
    public void test_select_subset_of_outputs_from_union() {
        String stmt =
            """
            SELECT x FROM(
              SELECT 1 as x, id FROM users
              UNION ALL
              SELECT 2, id FROM users
              ) o
            ORDER BY x
             """;
        var logicalPlan = e.logicalPlan(stmt);
        String expectedPlan =
            """
            Rename[x] AS o
              └ Union[x]
                ├ OrderBy[1 AS x ASC]
                │  └ Collect[doc.users | [1 AS x] | true]
                └ OrderBy[2 ASC]
                  └ Collect[doc.users | [2] | true]
            """;
        assertThat(logicalPlan).isEqualTo(expectedPlan);
    }

    @Test
    public void testUnionDistinct() {
        var logicalPlan = e.logicalPlan(
            """
            SELECT id FROM users
            UNION DISTINCT
            SELECT id FROM locations
            """
        );
        String expectedPlan =
            """
            GroupHashAggregate[id]
              └ Union[id]
                ├ Collect[doc.users | [id] | true]
                └ Collect[doc.locations | [id] | true]
            """;
        assertThat(logicalPlan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_union_with_different_types_in_queries_adds_casts() {
        UnionExecutionPlan union = e.plan("SELECT null UNION ALL SELECT id FROM users");
        Collect left = (Collect) union.left();
        assertThat(left.collectPhase().projections()).satisfiesExactly(
            p -> assertThat(p).isExactlyInstanceOf(EvalProjection.class), // returns NULL
            p -> assertThat(p).isExactlyInstanceOf(EvalProjection.class)  // casts NULL to long to match `id`
        );
        Collect right = (Collect) union.right();
        assertThat(left.streamOutputs()).isEqualTo(right.streamOutputs());
        assertThat(left.streamOutputs()).satisfiesExactly(o -> assertThat(o).isEqualTo(DataTypes.LONG));


        union = e.plan("SELECT id FROM users UNION ALL SELECT null");
        left = (Collect) union.left();
        right = (Collect) union.right();
        assertThat(left.streamOutputs()).isEqualTo(right.streamOutputs());
        assertThat(right.streamOutputs()).satisfiesExactly(o -> assertThat(o).isEqualTo(DataTypes.LONG));
        assertThat(right.collectPhase().projections()).satisfiesExactly(
            p -> assertThat(p).isExactlyInstanceOf(EvalProjection.class), // returns NULL
            p -> assertThat(p).isExactlyInstanceOf(EvalProjection.class)  // casts NULL to long to match `id`
        );
    }

    @Test
    public void test_union_returns_unknown_expected_rows_unknown_on_one_source_plan() {
        String stmt = "SELECT id FROM users UNION ALL SELECT id FROM locations";
        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(1, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        var context = e.getPlannerContext(clusterService.state());
        var logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        var plan = logicalPlanner.plan(e.analyze(stmt), context);
        var union = (Union) plan.sources().get(0);
        assertThat(e.getStats(union).numDocs()).isEqualTo(-1L);
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(1, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);
        plan = logicalPlanner.plan(e.analyze(stmt), context);
        union = (Union) plan.sources().get(0);
        assertThat(e.getStats(union).numDocs()).isEqualTo(-1L);
    }

    // tracks bugs: https://github.com/crate/crate/issues/14807, https://github.com/crate/crate/issues/14805
    @Test
    public void test_pruneOutputsExcept_can_handle_duplicates() {
        LogicalPlan plan = e.logicalPlan(
            "SELECT * FROM (SELECT x.id, y.id FROM users AS x, users AS y) z UNION SELECT 1, 1;");

        assertThat(plan).isEqualTo(
            """
                GroupHashAggregate[id, id]
                  └ Union[id, id]
                    ├ Rename[id, id] AS z
                    │  └ NestedLoopJoin[CROSS]
                    │    ├ Rename[id] AS x
                    │    │  └ Collect[doc.users | [id] | true]
                    │    └ Rename[id] AS y
                    │      └ Collect[doc.users | [id] | true]
                    └ Eval[1, 1]
                      └ TableFunction[empty_row | [] | true]
                """
        );
    }

}
