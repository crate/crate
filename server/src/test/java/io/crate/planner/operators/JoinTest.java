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
import static io.crate.planner.operators.LogicalPlannerTest.isPlan;
import static io.crate.testing.Asserts.assertList;
import static io.crate.testing.Asserts.isInputColumn;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.Randomness;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.SubqueryPlanner.SubQueries;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class JoinTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private ProjectionBuilder projectionBuilder;
    private PlannerContext plannerCtx;
    private CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();

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
        plannerCtx = e.getPlannerContext(clusterService.state());
    }

    @After
    public void resetEnableHashJoinFlag() {
        txnCtx.sessionSettings().setHashJoinEnabled(true);
    }

    private LogicalPlan createLogicalPlan(QueriedSelectRelation mss, TableStats tableStats) {
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            tableStats,
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, plannerCtx));
        return JoinPlanBuilder.buildJoinTree(
            mss.from(),
            mss.where(),
            mss.joinPairs(),
            new SubQueries(Map.of(), Map.of()),
            rel -> logicalPlanner.plan(rel, plannerCtx, subqueryPlanner, true),
            txnCtx.sessionSettings().hashJoinsEnabled()
        );
    }

    private Join buildJoin(LogicalPlan operator) {
        return (Join) operator.build(mock(DependencyCarrier.class), plannerCtx, Set.of(), projectionBuilder, -1, 0, null, null, Row.EMPTY, SubQueryResults.EMPTY);
    }

    private Join plan(QueriedSelectRelation mss, TableStats tableStats) {
        return buildJoin(createLogicalPlan(mss, tableStats));
    }

    @Test
    public void testNestedLoop_TablesAreSwitchedIfLeftIsSmallerThanRight() {
        txnCtx.sessionSettings().setHashJoinEnabled(false);
        QueriedSelectRelation mss = e.analyze("select * from users, locations where users.id = locations.id");

        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(10, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(10_000, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        Join nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("locations"));

        rowCountByTable.put(USER_TABLE_IDENT, new Stats(10_000, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(10, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("users"));
    }

    @Test
    public void test_nestedloop_tables_are_not_switched_when_expected_numbers_of_rows_are_negative() {
        txnCtx.sessionSettings().setHashJoinEnabled(false);
        QueriedSelectRelation mss = e.analyze("select * from users left join locations on users.id = locations.id");

        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(0, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(NestedLoopJoin.class));

        Join nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("users"));
        assertThat(((Reference) ((Collect) nl.right()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("locations"));
    }

    @Test
    public void test_nestedloop_tables_are_not_switched_when_rhs_expected_numbers_of_rows_are_negative_on_a_single_node() throws IOException {
        resetClusterService();
        e = SQLExecutor.builder(clusterService, 1, Randomness.get(), List.of())
            .addTable(USER_TABLE_DEFINITION)
            .addTable(TEST_DOC_LOCATIONS_TABLE_DEFINITION)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());

        txnCtx.sessionSettings().setHashJoinEnabled(false);
        QueriedSelectRelation mss = e.analyze("select * from users left join locations on users.id = locations.id");

        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(0, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(NestedLoopJoin.class));

        Join nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("users"));
        assertThat(((Reference) ((Collect) nl.right()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("locations"));
    }

    @Test
    public void test_hashjoin_tables_are_not_switched_when_expected_numbers_of_rows_are_negative() {
        QueriedSelectRelation mss = e.analyze("select users.name, locations.id " +
                                              "from users " +
                                              "join locations on users.id = locations.id");

        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(-1, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(0, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(HashJoin.class));

        Join join = buildJoin(operator);
        assertThat(((Reference) ((Collect) join.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("users"));
        assertThat(((Reference) ((Collect) join.right()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("locations"));
    }

    @Test
    public void testNestedLoop_TablesAreSwitchedIfBlockJoinAndRightIsSmallerThanLeft() throws IOException {
        // blockNL is only possible on single node clusters
        e = SQLExecutor.builder(clusterService)
            .addTable("create table j.left_table (id int)")
            .addTable("create table j.right_table (id int)")
            .build();
        RelationName leftName = new RelationName("j", "left_table");
        RelationName rightName = new RelationName("j", "right_table");

        QueriedSelectRelation mss = e.analyze("select * from j.left_table as l left join j.right_table as r on l.id = r.id");

        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(leftName, new Stats(10, 0, Map.of()));
        rowCountByTable.put(rightName, new Stats(10_000, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        Join nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is(leftName.name()));
        assertThat(nl.joinPhase().joinType(), is(JoinType.LEFT));

        rowCountByTable.put(leftName, new Stats(10_000, 0, Map.of()));
        rowCountByTable.put(rightName, new Stats(10, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is(rightName.name()));
        assertThat(nl.joinPhase().joinType(), is(JoinType.RIGHT));  // ensure that also the join type inverted
    }

    @Test
    public void testNestedLoop_TablesAreNotSwitchedIfLeftHasAPushedDownOrderBy() {
        txnCtx.sessionSettings().setHashJoinEnabled(false);
        // we use a subselect to simulate the pushed-down order by
        QueriedSelectRelation mss = e.analyze("select users.id from (select id from users order by id) users, " +
                                              "locations where users.id = locations.id");

        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(10, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(10_0000, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            tableStats,
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));
        LogicalPlan operator = JoinPlanBuilder.buildJoinTree(
            mss.from(),
            mss.where(),
            mss.joinPairs(),
            new SubQueries(Map.of(), Map.of()),
            rel -> logicalPlanner.plan(rel, plannerCtx, subqueryPlanner, false),
            false
        );
        Join nl = (Join) operator.build(
            mock(DependencyCarrier.class), context, Set.of(), projectionBuilder, -1, 0, null, null, Row.EMPTY, SubQueryResults.EMPTY);

        assertList(((Collect) nl.left()).collectPhase().toCollect()).isSQL("doc.users.id");
        assertThat(nl.resultDescription().orderBy(), notNullValue());
    }

    @Test
    public void testNestedLoop_TablesAreNotSwitchedAfterOrderByPushDown() {
        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(10, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(10_0000, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        context.transactionContext().sessionSettings().setHashJoinEnabled(false);
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            tableStats,
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        LogicalPlan plan = logicalPlanner.plan(e.analyze("select users.id from users, locations " +
                                                         "where users.id = locations.id order by users.id"), context);
        Merge merge = (Merge) plan.build(
            mock(DependencyCarrier.class), context, Set.of(), projectionBuilder, -1, 0, null, null, Row.EMPTY, SubQueryResults.EMPTY);
        Join nl = (Join) merge.subPlan();

        assertThat(nl.resultDescription().orderBy(), notNullValue());
    }

    @Test
    public void testHashJoin_TableOrderInLogicalAndExecutionPlan() {
        QueriedSelectRelation mss = e.analyze("select users.name, locations.id " +
                                              "from users " +
                                              "join locations on users.id = locations.id");

        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(100, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(10, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(HashJoin.class));
        assertThat("Smaller table must be on the right-hand-side",
                   ((HashJoin) operator).rhs().getRelationNames(),
                   contains(TEST_DOC_LOCATIONS_TABLE_IDENT));

        Join join = buildJoin(operator);
        Asserts.assertThat(((Collect) join.left()).collectPhase().toCollect().get(1)).isReference("other_id");
    }

    @Test
    public void testHashJoin_TablesSwitchWhenRightBiggerThanLeft() {
        QueriedSelectRelation mss = e.analyze("select users.name, locations.id " +
                                              "from users " +
                                              "join locations on users.id = locations.id");

        TableStats tableStats = new TableStats();
        Map<RelationName, Stats> rowCountByTable = new HashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new Stats(10, 0, Map.of()));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new Stats(100, 0, Map.of()));
        tableStats.updateTableStats(rowCountByTable);

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(HashJoin.class));
        assertThat("Smaller table must be on the right-hand-side",
                   ((HashJoin) operator).rhs().getRelationNames(),
                   contains(TEST_DOC_LOCATIONS_TABLE_IDENT));

        Join join = buildJoin(operator);
        Asserts.assertThat(((Collect) join.left()).collectPhase().toCollect().get(1)).isReference("loc");
    }

    @Test
    public void testMultipleHashJoins() {
        QueriedSelectRelation mss = e.analyze("select * " +
                                              "from t1 inner join t2 on t1.a = t2.b " +
                                              "inner join t3 on t3.c = t2.b");

        LogicalPlan operator = createLogicalPlan(mss, new TableStats());
        assertThat(operator, instanceOf(HashJoin.class));
        LogicalPlan leftPlan = ((HashJoin) operator).lhs;
        assertThat(leftPlan, instanceOf(HashJoin.class));

        Join join = buildJoin(operator);
        assertThat(join.joinPhase(), instanceOf(HashJoinPhase.class));
        assertThat(join.left(), instanceOf(Join.class));
        assertThat(((Join)join.left()).joinPhase(), instanceOf(HashJoinPhase.class));
    }

    @Test
    public void testMixedHashJoinNestedLoop() {
        QueriedSelectRelation mss = e.analyze("select * " +
                                              "from t1 inner join t2 on t1.a = t2.b " +
                                              "left join t3 on t3.c = t2.b");

        LogicalPlan operator = createLogicalPlan(mss, new TableStats());
        assertThat(operator, instanceOf(NestedLoopJoin.class));
        LogicalPlan leftPlan = ((NestedLoopJoin) operator).lhs;
        assertThat(leftPlan, instanceOf(HashJoin.class));

        Join join = buildJoin(operator);
        assertThat(join.joinPhase(), instanceOf(NestedLoopPhase.class));
        assertThat(join.left(), instanceOf(Join.class));
        assertThat(((Join)join.left()).joinPhase(), instanceOf(HashJoinPhase.class));
    }

    @Test
    public void testBlockNestedLoopWhenTableSizeUnknownAndOneExecutionNode() throws IOException {
        // rebuild executor + cluster state with 1 node
        resetClusterService();
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());

        QueriedSelectRelation mss = e.analyze("select * from t1, t4");

        LogicalPlan operator = createLogicalPlan(mss, new TableStats());
        assertThat(operator, instanceOf(NestedLoopJoin.class));

        Join join = buildJoin(operator);
        assertThat(join.joinPhase(), instanceOf(NestedLoopPhase.class));
        NestedLoopPhase joinPhase = (NestedLoopPhase) join.joinPhase();
        assertThat(joinPhase.blockNestedLoop, is(true));
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
            .setTableStats(tableStats)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());

        QueriedSelectRelation mss = e.analyze("select * from t1, t4");

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(NestedLoopJoin.class));

        Join join = buildJoin(operator);
        assertThat(join.joinPhase(), instanceOf(NestedLoopPhase.class));
        NestedLoopPhase joinPhase = (NestedLoopPhase) join.joinPhase();
        assertThat(joinPhase.blockNestedLoop, is(true));

        assertThat(join.left(), instanceOf(Collect.class));
        // no table switch should have been made
        assertThat(((Reference) ((Collect) join.left()).collectPhase().toCollect().get(0)).ident().tableIdent(),
            is(T3.T1));
    }

    @Test
    public void testBlockNestedLoopWhenRightSideIsSmallerAndOneExecutionNode() throws IOException {
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
            .setTableStats(tableStats)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());

        QueriedSelectRelation mss = e.analyze("select * from t4, t1");

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(NestedLoopJoin.class));

        Join join = buildJoin(operator);
        assertThat(join.joinPhase(), instanceOf(NestedLoopPhase.class));
        NestedLoopPhase joinPhase = (NestedLoopPhase) join.joinPhase();
        assertThat(joinPhase.blockNestedLoop, is(true));

        assertThat(join.left(), instanceOf(Collect.class));
        // right side will be flipped to the left
        assertThat(((Reference) ((Collect) join.left()).collectPhase().toCollect().get(0)).ident().tableIdent(),
            is(T3.T1));
    }

    @Test
    public void testNoBlockNestedLoopWithOrderBy() throws IOException {
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
            .setTableStats(tableStats)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());

        QueriedSelectRelation mss = e.analyze("select * from t1, t4 order by t1.x");
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            tableStats,
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        LogicalPlan operator = logicalPlanner.plan(mss, plannerCtx);
        ExecutionPlan build = operator.build(
            mock(DependencyCarrier.class), plannerCtx, Set.of(), projectionBuilder, -1, 0, null, null, Row.EMPTY, SubQueryResults.EMPTY);

        assertThat((((NestedLoopPhase) ((Join) build).joinPhase())).blockNestedLoop, is(false));
    }

    @Test
    public void testPlanChainedJoinsWithWindowFunctionInOutput() {
        txnCtx.sessionSettings().setHashJoinEnabled(false);
        QueriedSelectRelation mss = e.analyze("SELECT t1.a, t2.b, row_number() OVER(ORDER BY t3.z) " +
                                              "FROM t1 t1 " +
                                              "JOIN t2 t2 on t1.a = t2.b " +
                                              "JOIN t3 t3 on t3.c = t2.b");
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            new TableStats(),
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        LogicalPlan join = logicalPlanner.plan(mss, plannerCtx);

        WindowAgg windowAggOperator = (WindowAgg) ((Eval) ((RootRelationBoundary) join).source).source;
        assertThat(join.outputs(), hasItem(windowAggOperator.windowFunctions().get(0)));
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
            "Rename[x, x] AS tjoin\n" +
            "  └ NestedLoopJoin[CROSS]\n" +
            "    ├ Collect[doc.t1 | [x] | true]\n" +
            "    └ Collect[doc.t2 | [x] | true]";
        assertThat(logicalPlan, isPlan(expectedPlan));

        Join join = e.plan(statement);
        Asserts.assertThat(join.joinPhase().projections().get(0).outputs()).satisfiesExactly(
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

        assertThat(plan, isPlan(
            "Eval[name AS usr]\n" +
                "  └ Limit[10::bigint;0]\n" +
                "    └ NestedLoopJoin[CROSS]\n" +
                "      ├ OrderBy[name ASC]\n" +
                "      │  └ Collect[doc.users | [name] | true]\n" +
                "      └ Collect[doc.t1 | [] | true]"));
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
            "Eval[name]\n" +
            "  └ HashJoin[(a = address['postcode'])]\n" +
            "    ├ Collect[doc.users | [name, address['postcode']] | true]\n" +
            "    └ Collect[doc.t1 | [a] | true]";
        assertThat(logicalPlan, is(isPlan(expectedPlan)));

        // Same using an table alias (MSS -> AliasAnalyzedRelation -> AbstractTableRelation)
        logicalPlan = e.logicalPlan("select u.name" +
                                        " from users u, t1" +
                                        " where t1.a = u.address['postcode']");
        expectedPlan =
            "Eval[name]\n" +
            "  └ HashJoin[(a = address['postcode'])]\n" +
            "    ├ Rename[name, address['postcode']] AS u\n" +
            "    │  └ Collect[doc.users | [name, address['postcode']] | true]\n" +
            "    └ Collect[doc.t1 | [a] | true]";
        assertThat(logicalPlan, is(isPlan(expectedPlan)));
    }

    @Test
    public void test_filter_on_aliased_symbol_is_moved_below_nl_if_left_join_can_be_rewritten_to_inner_join() throws Exception {
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
            "Rename[start] AS doc.v1\n" +
            "  └ Eval[ts_production AS start]\n" +
            "    └ NestedLoopJoin[INNER | (ts_production = max_ts)]\n" +
            "      ├ Rename[max_ts] AS last_record\n" +
            "      │  └ Eval[max(ts) AS max_ts]\n" +
            "      │    └ HashAggregate[max(ts)]\n" +
            "      │      └ Collect[doc.metric_mini | [ts] | true]\n" +
            "      └ Rename[ts_production] AS b\n" +
            "        └ Collect[doc.metric_mini | [ts_production] | ((ts_production AS start >= 1638316800000::bigint) AND (ts_production AS start <= 1638320399000::bigint))]";
        assertThat(plan, is(isPlan(expectedPlan)));

        plan = executor.logicalPlan(
            "SELECT * FROM v1 WHERE start >= ? and start <= ?");
        expectedPlan =
            "Rename[start] AS doc.v1\n" +
            "  └ Eval[ts_production AS start]\n" +
            "    └ NestedLoopJoin[INNER | (ts_production = max_ts)]\n" +
            "      ├ Rename[max_ts] AS last_record\n" +
            "      │  └ Eval[max(ts) AS max_ts]\n" +
            "      │    └ HashAggregate[max(ts)]\n" +
            "      │      └ Collect[doc.metric_mini | [ts] | true]\n" +
            "      └ Rename[ts_production] AS b\n" +
            "        └ Collect[doc.metric_mini | [ts_production] | ((ts_production AS start >= $1) AND (ts_production AS start <= $2))]";
        assertThat(plan, is(isPlan(expectedPlan)));
    }

    @Test
    public void test_can_create_execution_plan_from_join_condition_depending_on_multiple_tables() throws Exception {
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
        assertThat(logicalPlan, is(isPlan(
            "Eval[time, sensor_id, battery_level]\n" +
            "  └ NestedLoopJoin[LEFT | ((time = time) AND (sensor_id = sensor_id))]\n" +
            "    ├ NestedLoopJoin[CROSS]\n" +
            "    │  ├ Rename[time] AS time_series\n" +
            "    │  │  └ TableFunction[generate_series | [generate_series] | true]\n" +
            "    │  └ Rename[sensor_id] AS sensors\n" +
            "    │    └ TableFunction[unnest | [unnest] | true]\n" +
            "    └ Rename[battery_level, time, sensor_id] AS readings\n" +
            "      └ Collect[doc.sensor_readings | [battery_level, time, sensor_id] | true]"
        )));

        Object plan = executor.plan(statement);
        assertThat(plan, Matchers.instanceOf(Join.class));
    }

    /**
     * https://github.com/crate/crate/issues/11404
     */
    @Test
    public void test_constant_expression_in_left_join_condition_is_pushed_down_to_relation() throws Exception {
        var executor = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of()).build();

        LogicalPlan logicalPlan = executor.logicalPlan(
            "select doc.t1.*, doc.t2.b from doc.t1 join doc.t2 on doc.t1.x = doc.t2.y and doc.t2.b = 'abc'");

        assertThat(logicalPlan, is(isPlan(
            "Eval[a, x, i, b]\n" +
            "  └ HashJoin[(x = y)]\n" +
            "    ├ Collect[doc.t1 | [a, x, i] | true]\n" +
            "    └ Collect[doc.t2 | [b, y] | (b = 'abc')]")));
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
        assertThat(logicalPlan, is(isPlan(
            "MultiPhase\n" +
            "  └ NestedLoopJoin[INNER | (i = i)]\n" +
            "    ├ Collect[doc.t1 | [a, x, i] | true]\n" +
            "    └ Collect[doc.t2 | [b, y, i] | (y = ANY((SELECT z FROM (doc.t3))))]\n" +
            "  └ OrderBy[z ASC]\n" +
            "    └ Collect[doc.t3 | [z] | true]")));

        Object plan = e.plan(statement);
        assertThat(plan, instanceOf(Merge.class));
        assertThat(((Merge) plan).subPlan(), instanceOf(Join.class));
    }
}
