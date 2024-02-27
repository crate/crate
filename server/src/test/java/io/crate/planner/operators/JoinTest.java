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

import static io.crate.analyze.TableDefinitions.TEST_DOC_LOCATIONS_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT;
import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static io.crate.testing.Asserts.assertList;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isInputColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.join.Join;
import io.crate.sql.tree.JoinType;
import io.crate.sql.tree.QualifiedName;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class JoinTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private ProjectionBuilder projectionBuilder;

    @Before
    public void setUpExecutor() throws IOException {
        e = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable(USER_TABLE_DEFINITION)
            .addTable(TEST_DOC_LOCATIONS_TABLE_DEFINITION)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T3_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .build();
        projectionBuilder = new ProjectionBuilder(e.nodeCtx);
    }

    private LogicalPlan buildLogicalPlan(QueriedSelectRelation mss) {
        var plannerCtx = e.getPlannerContext(clusterService.state());
        return buildLogicalPlan(mss, plannerCtx);
    }

    private LogicalPlan buildLogicalPlan(QueriedSelectRelation mss, PlannerContext plannerCtx) {
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            new ForeignDataWrappers(Settings.EMPTY, clusterService, e.nodeCtx),
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, plannerCtx));
        return logicalPlanner.plan(mss, plannerCtx,subqueryPlanner, false);
    }

    private Join buildJoin(LogicalPlan operator) {
        var plannerCtx = e.getPlannerContext(clusterService.state());
        return buildJoin(operator, plannerCtx);
    }

    private Join buildJoin(LogicalPlan operator, PlannerContext plannerCtx) {
        return (Join) operator.build(mock(DependencyCarrier.class), plannerCtx, Set.of(), projectionBuilder, -1, 0, null, null, Row.EMPTY, SubQueryResults.EMPTY);
    }

    private Join plan(QueriedSelectRelation mss, PlannerContext plannerCtx) {
        return buildJoin(buildLogicalPlan(mss, plannerCtx), plannerCtx);
    }

    private Join plan(QueriedSelectRelation mss) {
        var plannerCtx = e.getPlannerContext(clusterService.state());
        return buildJoin(buildLogicalPlan(mss, plannerCtx), plannerCtx);
    }

    private static String tableName(ExecutionPlan plan) {
        return ((Reference) ((Collect) plan).collectPhase().toCollect().get(0)).ident().tableIdent().name();
    }

    @Test
    public void testNestedLoop_TablesAreSwitchedIfLeftIsSmallerThanRight() {
        QueriedSelectRelation mss = e.analyze("select * from users, locations where users.id = locations.id");

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(10, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(10_000, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        var plannerCtx = e.getPlannerContext(clusterService.state());
        plannerCtx.transactionContext().sessionSettings().setHashJoinEnabled(false);
        Join nl = plan(mss, plannerCtx);
        assertThat(tableName(nl.left())).isEqualTo("locations");
        assertThat(tableName(nl.right())).isEqualTo("users");

        rowCountByTable.put(USER_TABLE_IDENT, new Stats(10_000, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(10, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        nl = plan(mss);
        assertThat(tableName(nl.left())).isEqualTo("users");
    }

    @Test
    public void test_nestedloop_tables_are_not_switched_when_expected_numbers_of_rows_are_negative() {
        QueriedSelectRelation mss = e.analyze("select * from users left join locations on users.id = locations.id");

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(0, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        LogicalPlan operator = buildLogicalPlan(mss);
        assertThat(operator).isExactlyInstanceOf(NestedLoopJoin.class);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        context.transactionContext().sessionSettings().setHashJoinEnabled(false);
        Join nl = plan(mss, context);
        assertThat(tableName(nl.left())).isEqualTo("users");
        assertThat(tableName(nl.right())).isEqualTo("locations");
    }

    @Test
    public void test_nestedloop_tables_are_not_switched_when_rhs_expected_numbers_of_rows_are_negative_on_a_single_node() throws IOException {
        resetClusterService();
        e = SQLExecutor.builder(clusterService, 1, Randomness.get(), List.of())
            .addTable(USER_TABLE_DEFINITION)
            .addTable(TEST_DOC_LOCATIONS_TABLE_DEFINITION)
            .build();

        QueriedSelectRelation mss = e.analyze("select * from users left join locations on users.id = locations.id");

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(0, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        context.transactionContext().sessionSettings().setHashJoinEnabled(false);

        LogicalPlan operator = buildLogicalPlan(mss, context);
        assertThat(operator).isExactlyInstanceOf(NestedLoopJoin.class);

        Join nl = plan(mss, context);
        assertThat(tableName(nl.left())).isEqualTo("users");
        assertThat(tableName(nl.right())).isEqualTo("locations");
    }

    @Test
    public void test_hashjoin_tables_are_not_switched_when_expected_numbers_of_rows_are_negative() {
        QueriedSelectRelation mss = e.analyze("select users.name, locations.id " +
                                              "from users " +
                                              "join locations on users.id = locations.id");

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(0, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        LogicalPlan plan = buildLogicalPlan(mss);
        assertThat(plan).isEqualTo("Eval[name, id]\n" +
                                   "  └ HashJoin[(id = id)]\n" +
                                   "    ├ Collect[doc.users | [name, id] | true]\n" +
                                   "    └ Collect[doc.locations | [id] | true]");

        var hashjoin = plan.sources().get(0);
        assertThat(hashjoin).isExactlyInstanceOf(HashJoin.class);

        Join join = buildJoin(hashjoin);
        assertThat(tableName(join.left())).isEqualTo("users");
        assertThat(tableName(join.right())).isEqualTo("locations");
    }

    @Test
    public void testNestedLoop_TablesAreSwitchedIfBlockJoinAndRightIsSmallerThanLeft() throws IOException {
        // blockNL is only possible on single node clusters
        resetClusterService();
        e = SQLExecutor.builder(clusterService, 1, random(), List.of())
            .addTable("create table j.left_table (id int)")
            .addTable("create table j.right_table (id int)")
            .build();
        RelationName leftName = new RelationName("j", "left_table");
        RelationName rightName = new RelationName("j", "right_table");

        QueriedSelectRelation mss = e.analyze("select * from j.left_table as l left join j.right_table as r on l.id = r.id");

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(leftName, new Stats(10, 0, Map.of()));
        rowCountByTable.put(rightName, new Stats(10_000, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        Join nl = plan(mss);
        assertThat(tableName(nl.left())).isEqualTo(leftName.name());
        assertThat(nl.joinPhase().joinType()).isEqualTo(JoinType.LEFT);

        rowCountByTable.put(leftName, new Stats(10_000, 0, Map.of()));
        rowCountByTable.put(rightName, new Stats(10, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        nl = plan(mss);
        assertThat(tableName(nl.left())).isEqualTo(rightName.name());
        assertThat(nl.joinPhase().joinType()).isEqualTo(JoinType.RIGHT);  // ensure that also the join type inverted
    }

    @Test
    public void testNestedLoop_TablesAreNotSwitchedIfLeftHasAPushedDownOrderBy() {
        QueriedSelectRelation mss = e.analyze("select users.id from users, locations where users.id = locations.id order by users.id");

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(10, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(10_0000, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        context.transactionContext().sessionSettings().setHashJoinEnabled(false);

        Join nl = plan(mss, context);
        assertList(((Collect) nl.left()).collectPhase().toCollect()).isSQL("doc.users.id");
        assertThat(nl.resultDescription().orderBy()).isNotNull();
    }

    @Test
    public void testNestedLoop_TablesAreNotSwitchedAfterOrderByPushDown() {
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(60000, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(10_0000, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        QueriedSelectRelation mss = e.analyze("select users.id from users, locations " +
                  "where users.id = locations.id order by users.id");

        PlannerContext context = e.getPlannerContext(clusterService.state());
        context.transactionContext().sessionSettings().setHashJoinEnabled(false);

        LogicalPlan plan = buildLogicalPlan(mss, context);
        assertThat(plan).isEqualTo("Eval[id]\n" +
                                   "  └ NestedLoopJoin[INNER | (id = id)]\n" +
                                   "    ├ OrderBy[id ASC]\n" +
                                   "    │  └ Collect[doc.users | [id] | true]\n" +
                                   "    └ Collect[doc.locations | [id] | true]");

        var join = buildJoin(plan, context);
        assertThat(join.resultDescription().orderBy()).isNotNull();
    }

    @Test
    public void testHashJoin_TableOrderInLogicalAndExecutionPlan() {
        QueriedSelectRelation mss = e.analyze("select users.name, locations.id " +
                                              "from users " +
                                              "join locations on users.id = locations.id");

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(100, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(20, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        LogicalPlan plan = buildLogicalPlan(mss);
        assertThat(plan).isEqualTo(
            "Eval[name, id]\n" +
            "  └ HashJoin[(id = id)]\n" +
            "    ├ Collect[doc.users | [name, id] | true]\n" +
            "    └ Collect[doc.locations | [id] | true]"
        );
        var hashJoin = plan.sources().get(0);
        assertThat(hashJoin).isExactlyInstanceOf(HashJoin.class);
        assertThat(((HashJoin) hashJoin).rhs().getRelationNames())
            .as("Smaller table must be on the right-hand-side")
            .containsExactly(TEST_DOC_LOCATIONS_TABLE_IDENT);

        Join join = buildJoin(hashJoin);
        assertThat(tableName(join.left())).isEqualTo("users");
        assertThat(tableName(join.right())).isEqualTo("locations");
    }

    @Test
    public void testHashJoin_TablesSwitchWhenRightBiggerThanLeft() {
        QueriedSelectRelation mss = e.analyze("select users.name, locations.id " +
                                              "from users " +
                                              "join locations on users.id = locations.id");

        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(50, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(100, 0, Map.of()));
        e.updateTableStats(rowCountByTable);

        LogicalPlan plan = buildLogicalPlan(mss);
        assertThat(plan).isEqualTo("Eval[name, id]\n" +
                                   "  └ Eval[name, id, id]\n" +
                                   "    └ HashJoin[(id = id)]\n" +
                                   "      ├ Collect[doc.locations | [id] | true]\n" +
                                   "      └ Collect[doc.users | [name, id] | true]");

        LogicalPlan hashjoin = plan.sources().get(0).sources().get(0);
        assertThat(hashjoin).isExactlyInstanceOf(HashJoin.class);
        assertThat(((HashJoin) hashjoin).lhs().getRelationNames())
            .as("Smaller table must be on the left-hand-side")
            .containsExactly(TEST_DOC_LOCATIONS_TABLE_IDENT);

        Join join = buildJoin(hashjoin);
        assertThat(tableName(join.left())).isEqualTo("locations");
    }

    @Test
    public void testMultipleHashJoins() {
        QueriedSelectRelation mss = e.analyze("select * " +
                                              "from t1 inner join t2 on t1.a = t2.b " +
                                              "inner join t3 on t3.c = t2.b");

        LogicalPlan operator = buildLogicalPlan(mss);
        assertThat(operator).isExactlyInstanceOf(HashJoin.class);
        LogicalPlan leftPlan = ((HashJoin) operator).lhs;
        assertThat(leftPlan).isExactlyInstanceOf(HashJoin.class);

        Join join = buildJoin(operator);
        assertThat(join.joinPhase()).isExactlyInstanceOf(HashJoinPhase.class);
        assertThat(join.left()).isExactlyInstanceOf(Join.class);
        assertThat(((Join)join.left()).joinPhase()).isExactlyInstanceOf(HashJoinPhase.class);
    }

    @Test
    public void testMixedHashJoinNestedLoop() {
        QueriedSelectRelation mss = e.analyze("select * " +
                                              "from t1 inner join t2 on t1.a = t2.b " +
                                              "left join t3 on t3.c = t2.b");

        LogicalPlan operator = buildLogicalPlan(mss);
        assertThat(operator).isExactlyInstanceOf(NestedLoopJoin.class);
        LogicalPlan leftPlan = ((NestedLoopJoin) operator).lhs;
        assertThat(leftPlan).isExactlyInstanceOf(HashJoin.class);

        Join join = buildJoin(operator);
        assertThat(join.joinPhase()).isExactlyInstanceOf(NestedLoopPhase.class);
        assertThat(join.left()).isExactlyInstanceOf(Join.class);
        assertThat(((Join)join.left()).joinPhase()).isExactlyInstanceOf(HashJoinPhase.class);
    }

    @Test
    public void testBlockNestedLoopWhenTableSizeUnknownAndOneExecutionNode() throws IOException {
        // rebuild executor + cluster state with 1 node
        resetClusterService();
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .build();

        QueriedSelectRelation mss = e.analyze("select * from t1, t4");

        LogicalPlan operator = buildLogicalPlan(mss);
        assertThat(operator).isExactlyInstanceOf(NestedLoopJoin.class);

        Join join = buildJoin(operator);
        assertThat(join.joinPhase()).isExactlyInstanceOf(NestedLoopPhase.class);
        NestedLoopPhase joinPhase = (NestedLoopPhase) join.joinPhase();
        assertThat(joinPhase.blockNestedLoop).isEqualTo(true);
    }

    @Test
    public void testBlockNestedLoopWhenLeftSideIsSmallerAndOneExecutionNode() throws IOException {
        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> stats = new HashMap<>();
        stats.put(T3.T1, new Stats(23, 64, Map.of()));
        stats.put(T3.T4, new Stats(42, 64, Map.of()));
        tableStats.updateTableStats(stats);

        // rebuild executor + cluster state with 1 node
        resetClusterService();
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .build();

        QueriedSelectRelation mss = e.analyze("select * from t1, t4");

        LogicalPlan operator = buildLogicalPlan(mss);
        assertThat(operator).isExactlyInstanceOf(NestedLoopJoin.class);

        Join join = buildJoin(operator);
        assertThat(join.joinPhase()).isExactlyInstanceOf(NestedLoopPhase.class);
        NestedLoopPhase joinPhase = (NestedLoopPhase) join.joinPhase();
        assertThat(joinPhase.blockNestedLoop).isEqualTo(true);

        assertThat(join.left()).isExactlyInstanceOf(Collect.class);
        // no table switch should have been made
        assertThat(tableName(join.left())).isEqualTo(T3.T1.name());
    }

    @Test
    public void testBlockNestedLoopWhenRightSideIsSmallerAndOneExecutionNode() throws IOException {
        // rebuild executor + cluster state with 1 node
        resetClusterService();
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .build();

        e.updateTableStats(Map.of(
            T3.T1, new Stats(23, 64, Map.of()),
            T3.T4, new Stats(42, 64, Map.of())
        ));

        QueriedSelectRelation mss = e.analyze("select * from t4, t1");

        LogicalPlan operator = buildLogicalPlan(mss);
        assertThat(operator).isExactlyInstanceOf(NestedLoopJoin.class);

        Join join = buildJoin(operator);
        assertThat(join.joinPhase()).isExactlyInstanceOf(NestedLoopPhase.class);
        NestedLoopPhase joinPhase = (NestedLoopPhase) join.joinPhase();
        assertThat(joinPhase.blockNestedLoop).isEqualTo(true);

        assertThat(join.left()).isExactlyInstanceOf(Collect.class);
        // right side will be flipped to the left
        assertThat(tableName(join.left())).isEqualTo(T3.T1.name());
    }

    @Test
    public void testNoBlockNestedLoopWithOrderBy() throws IOException {
        // rebuild executor + cluster state with 1 node
        resetClusterService();
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .build();

        e.updateTableStats(Map.of(
            T3.T1, new Stats(23, 64, Map.of()),
            T3.T4, new Stats(42, 64, Map.of())
        ));

        var plannerCtx = e.getPlannerContext(clusterService.state());

        QueriedSelectRelation mss = e.analyze("select * from t1, t4 order by t1.x");
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            new ForeignDataWrappers(Settings.EMPTY, clusterService, e.nodeCtx),
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        LogicalPlan operator = logicalPlanner.plan(mss, plannerCtx);
        ExecutionPlan build = operator.build(
            mock(DependencyCarrier.class), plannerCtx, Set.of(), projectionBuilder, -1, 0, null, null, Row.EMPTY, SubQueryResults.EMPTY);

        assertThat((((NestedLoopPhase) ((Join) build).joinPhase())).blockNestedLoop).isEqualTo(false);
    }

    @Test
    public void testPlanChainedJoinsWithWindowFunctionInOutput() {
        QueriedSelectRelation mss = e.analyze("SELECT t1.a, t2.b, row_number() OVER(ORDER BY t3.z) " +
                                              "FROM t1 t1 " +
                                              "JOIN t2 t2 on t1.a = t2.b " +
                                              "JOIN t3 t3 on t3.c = t2.b");
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            new ForeignDataWrappers(Settings.EMPTY, clusterService, e.nodeCtx),
            () -> clusterService.state().nodes().getMinNodeVersion()
        );

        var plannerCtx = e.getPlannerContext(clusterService.state());
        plannerCtx.transactionContext().sessionSettings().setHashJoinEnabled(false);

        LogicalPlan join = logicalPlanner.plan(mss, plannerCtx);
        WindowAgg windowAggOperator = (WindowAgg) ((Eval) ((RootRelationBoundary) join).source).source;
        assertThat(join.outputs()).contains(windowAggOperator.windowFunctions().get(0));
    }

    @Test
    public void testSameOutputIsNotDeDuplicated() throws Exception {
        resetClusterService(); // drop existing tables
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int)")
            .addTable("create table t2 (x int)")
            .build();
        String statement = "select * from (select * from t1, t2) tjoin";
        var logicalPlan = e.logicalPlan(statement);
        var expectedPlan =
            """
            Rename[x, x] AS tjoin
              └ NestedLoopJoin[CROSS]
                ├ Collect[doc.t1 | [x] | true]
                └ Collect[doc.t2 | [x] | true]
            """;
        assertThat(logicalPlan).isEqualTo(expectedPlan);

        Join join = e.plan(statement);
        assertThat(join.joinPhase().projections().get(0).outputs()).satisfiesExactly(
            isInputColumn(0),
            isInputColumn(1));
    }

    @Test
    public void test_join_with_alias_and_order_by_alias_with_limit_creates_valid_plan() throws Exception {
        LogicalPlan plan = e.logicalPlan("""
            select users.name as usr from users, t1
            order by usr
            limit 10
            """);

        assertThat(plan).isEqualTo(
            """
            Eval[name AS usr]
              └ Limit[10::bigint;0]
                └ NestedLoopJoin[CROSS]
                  ├ OrderBy[name ASC]
                  │  └ Collect[doc.users | [name] | true]
                  └ Collect[doc.t1 | [] | true]
            """
        );
    }

    @Test
    public void testForbidJoinWhereMatchOnBothTables() throws Exception {
        assertThatThrownBy(
            () -> e.plan("select * from t1, t2 " +
                         "where match(t1.a, 'Lanistas experimentum!') or match(t2.b, 'Rationes ridetis!')"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessageEndingWith("Using constructs like `match(r1.c) OR match(r2.c)` is not supported.");
    }

    /**
     * This scenario will result having a {@link io.crate.analyze.relations.AbstractTableRelation} as a direct
     * child of the {@link QueriedSelectRelation} instead of a {@link io.crate.analyze.QueriedSelectRelation} before ANY optimization
     * and validates that the plan can be build.
     *
     */
    @Test
    public void test_subscript_inside_query_spec_of_a_join_is_part_of_the_source_outputs() {
        var logicalPlan = e.logicalPlan("select users.name" +
                                            " from users, t1" +
                                            " where t1.a = users.address['postcode']");
        var expectedPlan =
            """
            Eval[name]
              └ HashJoin[(a = address['postcode'])]
                ├ Collect[doc.users | [name, address['postcode']] | true]
                └ Collect[doc.t1 | [a] | true]
            """;
        assertThat(logicalPlan).isEqualTo(expectedPlan);

        // Same using an table alias (MSS -> AliasAnalyzedRelation -> AbstractTableRelation)
        logicalPlan = e.logicalPlan("select u.name" +
                                        " from users u, t1" +
                                        " where t1.a = u.address['postcode']");
        expectedPlan =
            """
            Eval[name]
              └ HashJoin[(a = address['postcode'])]
                ├ Rename[name, address['postcode']] AS u
                │  └ Collect[doc.users | [name, address['postcode']] | true]
                └ Collect[doc.t1 | [a] | true]
            """;
        assertThat(logicalPlan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_filter_on_aliased_symbol_is_moved_below_join_if_left_join_can_be_rewritten_to_inner_join() throws Exception {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("""
                CREATE TABLE doc."metric_mini" (
                    "ts" TIMESTAMP WITH TIME ZONE,
                    "ts_production" TIMESTAMP WITH TIME ZONE
                )
            """
            )
            .addView(
                new RelationName("doc", "v1"),
                """
                    SELECT "b".ts_production AS "start"
                    FROM (
                        SELECT MAX(ts) AS max_ts FROM doc.metric_mini
                    ) last_record
                    LEFT JOIN metric_mini b ON "b".ts_production = "last_record"."max_ts"
                """
            )
            .build();

        LogicalPlan plan = executor.logicalPlan(
            "SELECT * FROM v1 WHERE start >= '2021-12-01' and start <= '2021-12-01 00:59:59'");
        String expectedPlan =
            """
            Rename[start] AS doc.v1
              └ Eval[ts_production AS start]
                └ HashJoin[(ts_production = max_ts)]
                  ├ Rename[max_ts] AS last_record
                  │  └ Eval[max(ts) AS max_ts]
                  │    └ HashAggregate[max(ts)]
                  │      └ Collect[doc.metric_mini | [ts] | true]
                  └ Rename[ts_production] AS b
                    └ Collect[doc.metric_mini | [ts_production] | ((ts_production AS start >= 1638316800000::bigint) AND (ts_production AS start <= 1638320399000::bigint))]
            """;
        assertThat(plan).isEqualTo(expectedPlan);

        plan = executor.logicalPlan(
            "SELECT * FROM v1 WHERE start >= ? and start <= ?");
        expectedPlan =
            """
            Rename[start] AS doc.v1
              └ Eval[ts_production AS start]
                └ HashJoin[(ts_production = max_ts)]
                  ├ Rename[max_ts] AS last_record
                  │  └ Eval[max(ts) AS max_ts]
                  │    └ HashAggregate[max(ts)]
                  │      └ Collect[doc.metric_mini | [ts] | true]
                  └ Rename[ts_production] AS b
                    └ Collect[doc.metric_mini | [ts_production] | ((ts_production AS start >= $1) AND (ts_production AS start <= $2))]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    /**
     * Verifies a bug fix
     * See: <a href="https://github.com/crate/crate/issues/13942"/>
     */
    @Test
    public void test_can_create_execution_plan_from_join_condition_depending_on_multiple_tables() {
        LogicalPlan logicalPlan = e.logicalPlan("""
            SELECT * FROM t1 CROSS JOIN t2 INNER JOIN t3 ON t3.z = t1.x AND t3.z = t2.y
            """);
        assertThat(logicalPlan).hasOperators(
            "Eval[a, x, i, b, y, i, c, z]",
            "  └ HashJoin[(z = y)]",
            "    ├ HashJoin[(z = x)]",
            "    │  ├ Collect[doc.t1 | [a, x, i] | true]",
            "    │  └ Collect[doc.t3 | [c, z] | true]",
            "    └ Collect[doc.t2 | [b, y, i] | true]"
        );
    }

    @Test
    public void test_can_create_execution_plan_from_join_condition_depending_on_multiple_aliased_tables() throws Exception {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("""
                CREATE TABLE IF NOT EXISTS sensor_readings (
                    "time" TIMESTAMP WITH TIME ZONE NOT NULL,
                    "sensor_id" INTEGER NOT NULL,
                    "battery_level" DOUBLE PRECISION)
            """
            ).build();
        String statement = """
            SELECT
                time_series."time",
                sensors.sensor_id,
                readings.battery_level
            FROM generate_series(
                '2022-02-09 10:00:00',
                '2022-02-09 10:10:00',
                '1 minute'::INTERVAL
            ) time_series ("time")
            CROSS JOIN UNNEST([1]) sensors(sensor_id)
            LEFT JOIN sensor_readings readings
                ON time_series.time = readings.time AND sensors.sensor_id = readings.sensor_id
            """;
        LogicalPlan logicalPlan = executor.logicalPlan(statement);
        assertThat(logicalPlan).isEqualTo(
            """
            Eval[time, sensor_id, battery_level]
              └ NestedLoopJoin[LEFT | ((time = time) AND (sensor_id = sensor_id))]
                ├ NestedLoopJoin[CROSS]
                │  ├ Rename[time] AS time_series
                │  │  └ TableFunction[generate_series | [generate_series] | true]
                │  └ Rename[sensor_id] AS sensors
                │    └ TableFunction[unnest | [unnest] | true]
                └ Rename[battery_level, time, sensor_id] AS readings
                  └ Collect[doc.sensor_readings | [battery_level, time, sensor_id] | true]
            """
        );

        Object plan = executor.plan(statement);
        assertThat(plan).isExactlyInstanceOf(Join.class);
    }

    /**
     * <a href=https://github.com/crate/crate/issues/11404/>
     */
    @Test
    public void test_constant_expression_in_left_join_condition_is_pushed_down_to_relation() throws Exception {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of()).build();

        LogicalPlan logicalPlan = executor.logicalPlan(
            "select doc.t1.*, doc.t2.b from doc.t1 join doc.t2 on doc.t1.x = doc.t2.y and doc.t2.b = 'abc'");

        assertThat(logicalPlan).isEqualTo(
            """
            Eval[a, x, i, b]
              └ HashJoin[(x = y)]
                ├ Collect[doc.t1 | [a, x, i] | true]
                └ Collect[doc.t2 | [b, y] | (b = 'abc')]
            """);
    }

    @Test
    public void test_rewrite_left_join_to_inner_with_subquery() {
        String statement =
            """
            SELECT *
            FROM t1
            LEFT JOIN t2 USING (i)
            WHERE t2.y IN (SELECT z FROM t3);
            """;
        LogicalPlan logicalPlan = e.logicalPlan(statement);
        assertThat(logicalPlan).isEqualTo(
            """
            MultiPhase
              └ HashJoin[(i = i)]
                ├ Collect[doc.t1 | [a, x, i] | true]
                └ Collect[doc.t2 | [b, y, i] | (y = ANY((SELECT z FROM (doc.t3))))]
              └ OrderBy[z ASC]
                └ Collect[doc.t3 | [z] | true]
            """);

        Object plan = e.plan(statement);
        assertThat(plan).isExactlyInstanceOf(Merge.class);
        assertThat(((Merge) plan).subPlan()).isExactlyInstanceOf(Join.class);
    }

    /**
     * <a href=https://github.com/crate/crate/issues/13592/>
     */
    public void test_correlated_subquery_can_access_all_tables_from_outer_query_that_joins_multiple_tables() throws IOException {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("create table a (x int, y int, z int)")
            .addTable("create table b (x int, y int, z int)")
            .addTable("create table c (x int, y int, z int)")
            .addTable("create table d (x int, y int, z int)")
            .build();
        LogicalPlan logicalPlan = executor.logicalPlan(
            "select (select 1 where a.x=1 and b.x=1 and c.x=1) from a,b,c,d"
        );
        assertThat(logicalPlan).isEqualTo(
            """
            Eval[(SELECT 1 FROM (empty_row))]
              └ CorrelatedJoin[x, x, x, (SELECT 1 FROM (empty_row))]
                └ NestedLoopJoin[CROSS]
                  ├ NestedLoopJoin[CROSS]
                  │  ├ NestedLoopJoin[CROSS]
                  │  │  ├ Collect[doc.a | [x] | true]
                  │  │  └ Collect[doc.b | [x] | true]
                  │  └ Collect[doc.c | [x] | true]
                  └ Collect[doc.d | [] | true]
                └ SubPlan
                  └ Eval[1]
                    └ Limit[2::bigint;0::bigint]
                      └ Filter[(((x = 1) AND (x = 1)) AND (x = 1))]
                        └ TableFunction[empty_row | [] | true]
            """
        );
    }

    @Test
    public void test_nested_join_pair_ordering() throws Exception {
        QueriedSelectRelation relation = e.analyze(
            """
                SELECT *
                FROM
                t1
                  JOIN (t2 JOIN t3 ON t2.i = t3.z)
                  ON t1.i = t2.i
                """
        );

        assertThat(relation.joinPairs()).hasSize(2);
        assertThat(relation.joinPairs().get(0)).hasPair(T3.T2, T3.T3);
        assertThat(relation.joinPairs().get(1)).hasPair(T3.T1, T3.T2);
    }

    @Test
    public void test_nested_join_pair_ordering_with_three_relations_in_join_condition() throws Exception {
        QueriedSelectRelation relation = e.analyze(
            """
             SELECT * from t1
             join t2 ON t1.i = t2.i
             join t3 on array_length(array_unique(ARRAY[t1.i, t2.i, t3.z]), 1) > 2"""
        );

        assertThat(relation.joinPairs()).hasSize(2);
        assertThat(relation.joinPairs().get(0)).hasPair(T3.T1, T3.T2);
        assertThat(relation.joinPairs().get(1)).hasPair(T3.T2, T3.T3);
    }



    @Test
    public void test_nested_joins_with_sub_query_join_pair_ordering() throws Exception {
        QueriedSelectRelation relation = e.analyze(
            """
                SELECT *
                    FROM t1
                    JOIN t2 on (t1.i = t2.i)
                    JOIN (select 1 as foo) temp ON (temp.foo = t1.i AND t2.i = temp.foo)
                """);

        assertThat(relation.joinPairs()).hasSize(2);
        assertThat(relation.joinPairs().get(0)).hasPair(T3.T1, T3.T2);
        assertThat(relation.joinPairs().get(1)).hasPair(T3.T2, relation("temp"));
    }

    @Test
    public void test_nested_join_pg_query_join_pair_ordering() {
        QueriedSelectRelation relation = e.analyze(
            """
                SELECT
                  c.oid,
                  a.attnum,
                  a.attname,
                  c.relname,
                  n.nspname,
                  a.attnotnull
                  OR (
                    t.typtype = 'd'
                    AND t.typnotnull
                  ),
                  a.attidentity != ''
                  OR pg_catalog.pg_get_expr(d.adbin, d.adrelid) LIKE '%nextval(%'
                FROM
                  pg_catalog.pg_class c
                  JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
                  JOIN pg_catalog.pg_attribute a ON (c.oid = a.attrelid)
                  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
                  LEFT JOIN pg_catalog.pg_attrdef d ON (
                    d.adrelid = a.attrelid
                    AND d.adnum = a.attnum
                  )
                  JOIN (
                    SELECT
                      -2002935028 AS oid,
                      1 AS attnum
                    UNION ALL
                    SELECT
                      -2002935028,
                      2
                  ) vals ON (
                    c.oid = vals.oid
                    AND a.attnum = vals.attnum
                  )
                """
        );

        assertThat(relation.joinPairs()).hasSize(5);
        assertThat(relation.joinPairs().get(0)).hasPair(relation("c"), relation("n"));
        assertThat(relation.joinPairs().get(1)).hasPair(relation("c"), relation("a"));
        assertThat(relation.joinPairs().get(2)).hasPair(relation("a"), relation("t"));
        assertThat(relation.joinPairs().get(3)).hasPair(relation("a"), relation("d"));
        assertThat(relation.joinPairs().get(4)).hasPair(relation("a"), relation("vals"));
    }

    private static RelationName relation(String name) {
        return RelationName.of(new QualifiedName(name), null);
    }

    @Test
    public void test_multiple_nested_join_raises_error() throws Exception {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("create table a (x int)")
            .addTable("create table b (y int)")
            .addTable("create table c (z int)")
            .build();

        var stmt = """
            SELECT
              y, z
            FROM
              a
              JOIN (
                b
                JOIN (
                  c
                  JOIN a ON c.z = a.x
                ) temp1 ON b.y = temp1.x
              ) temp2 on a.x = temp2.x;
            """;

        assertThatThrownBy(() -> executor.analyze(stmt))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Joins do not support this operation");
    }

    @Test
    public void test_join_using_non_matching_column_types_raises_error() throws Exception {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("create table a (value int)")
            .addTable("create table b (value string)")
            .build();

        assertThatThrownBy(() -> executor.analyze("SELECT * FROM a INNER JOIN b USING (value)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("JOIN/USING types integer and text varying cannot be matched");
    }

    @Test
    public void test_nested_joins_with_using_duplicate_columns_raises_error() throws Exception {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("CREATE TABLE j1 (x INT)")
            .addTable("CREATE TABLE j2 (x INT)")
            .addTable("CREATE TABLE j3 (x INT)")
            .build();

        assertThatThrownBy(() -> executor.analyze(
            """
            SELECT *
                FROM (j2 JOIN j3 ON j2.x = j3.x)
                JOIN J1
                USING(x);
            """
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("common column name x appears more than once in left table");

        assertThatThrownBy(() -> executor.analyze(
            """
            SELECT *
                FROM j1
                JOIN (j2 JOIN j3 ON j2.x = j3.x)
                USING(x);
            """
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("common column name x appears more than once in right table");

    }

    @Test
    public void test_nested_joins_with_non_existing_columns_raises_error() throws Exception {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("CREATE TABLE j1 (x INT)")
            .addTable("CREATE TABLE j2 (z INT)")
            .addTable("CREATE TABLE j3 (z INT)")
            .build();

        assertThatThrownBy(() -> executor.analyze(
            """
            SELECT *
                FROM (j2 JOIN j3 ON j2.z = j3.z)
                JOIN J1
                USING(x);
            """
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column x specified in USING clause does not exist in left table");

        assertThatThrownBy(() -> executor.analyze(
            """
            SELECT *
                FROM j1
                JOIN (j2 JOIN j3 ON j2.z = j3.z)
                USING(x);
            """
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column x specified in USING clause does not exist in right table");

    }

    @Test
    public void test_using_clause_specifying_multiple_columns_with_a_non_existing_column_raises_error() throws IOException {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("CREATE TABLE j1 (x INT, y INT)")
            .addTable("CREATE TABLE j2 (x INT)")
            .build();

        assertThatThrownBy(() -> executor.analyze("SELECT * FROM j1 JOIN j2 USING(x, y)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column y specified in USING clause does not exist in right table");

        // left and right sides swapped
        assertThatThrownBy(() -> executor.analyze("SELECT * FROM j2 JOIN j1 USING(x, y)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column y specified in USING clause does not exist in left table");

        // column unknown from both sides
        assertThatThrownBy(() -> executor.analyze("SELECT * FROM j2 JOIN j1 USING(x, z)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column z specified in USING clause does not exist in left table");
    }

    /**
     * Verifies a bug fix (and regression that was introduced with 5.2.4)
     * See https://github.com/crate/crate/issues/13808
     */
    @Test
    public void test_nested_joins_with_explicit_and_implicit_join_condition() throws Exception {
        var executor = SQLExecutor.builder(clusterService)
                .addTable("CREATE TABLE j1 (x INT)")
                .addTable("CREATE TABLE j2 (y INT)")
                .addTable("CREATE TABLE j3 (z INT)")
                .build();
        LogicalPlan logicalPlan = executor.logicalPlan(
                "SELECT *" +
                " FROM j1 " +
                "   JOIN j2 ON j1.x = j2.y " +
                "   JOIN j3 ON j1.x = j3.z" +
                " WHERE j2.y = j3.z"
        );
        assertThat(logicalPlan).isEqualTo(
                """
              HashJoin[((x = z) AND (y = z))]
                ├ HashJoin[(x = y)]
                │  ├ Collect[doc.j1 | [x] | true]
                │  └ Collect[doc.j2 | [y] | true]
                └ Collect[doc.j3 | [z] | true]
                """
        );
        // Variant with explicit and implicit join conditions swapped.
        // Due to internal ordering this behaved differently before.
        logicalPlan = executor.logicalPlan(
                "SELECT *" +
                " FROM j1 " +
                "   JOIN j2 ON j1.x = j2.y " +
                "   JOIN j3 ON j2.y = j3.z" +
                " WHERE j1.x = j3.z"
        );
        assertThat(logicalPlan).isEqualTo(
                """
              HashJoin[((x = z) AND (y = z))]
                ├ HashJoin[(x = y)]
                │  ├ Collect[doc.j1 | [x] | true]
                │  └ Collect[doc.j2 | [y] | true]
                └ Collect[doc.j3 | [z] | true]
                """
        );
    }

    @Test
    public void test_relationnames_order_in_nested_loop_join() throws Exception {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("CREATE TABLE doc.t1 (a INT)")
            .addTable("CREATE TABLE doc.t2 (b INT)")
            .addTable("CREATE TABLE doc.t3 (c INT)")
            .build();

        // optimizer_reorder_nested_loop_join moves the smaller table to the right side,
        // therefore we make t2 bigger and it becomes the top-most-left-table
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(new RelationName(Schemas.DOC_SCHEMA_NAME, "t1"), new Stats(100, 10, Map.of()));
        rowCountByTable.put(new RelationName(Schemas.DOC_SCHEMA_NAME, "t2"), new Stats(200, 200, Map.of()));
        executor.updateTableStats(rowCountByTable);

        QueriedSelectRelation mss = e.analyze(
            "Select * from t1 INNER JOIN t2 on t1.a = t2.b INNER JOIN T3 on t2.b = t3.c");

        var plannerCtx = executor.getPlannerContext(clusterService.state());
        plannerCtx.transactionContext().sessionSettings().setHashJoinEnabled(false);
        NestedLoopJoin result = (NestedLoopJoin) buildLogicalPlan(mss, plannerCtx);

        assertThat(result).isEqualTo(
            "NestedLoopJoin[INNER | (b = c)]\n" +
            "  ├ Eval[a, b]\n" +
            "  │  └ NestedLoopJoin[INNER | (a = b)]\n" +
            "  │    ├ Collect[doc.t2 | [b] | true]\n" +
            "  │    └ Collect[doc.t1 | [a] | true]\n" +
            "  └ Collect[doc.t3 | [c] | true]"
        );
        assertThat(result.getRelationNames().get(0).toString()).isEqualTo("doc.t2");
        assertThat(result.getRelationNames().get(1).toString()).isEqualTo("doc.t1");
        assertThat(result.getRelationNames().get(2).toString()).isEqualTo("doc.t3");
    }
}
