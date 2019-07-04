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

package io.crate.planner.operators;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import io.crate.analyze.MultiSourceSelect;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.types.DataTypes;
import org.elasticsearch.common.Randomness;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static io.crate.analyze.TableDefinitions.TEST_DOC_LOCATIONS_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static io.crate.planner.operators.LogicalPlannerTest.isPlan;
import static io.crate.testing.SymbolMatchers.isFetchRef;
import static io.crate.testing.SymbolMatchers.isInputColumn;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class JoinTest extends CrateDummyClusterServiceUnitTest {

    private static final RelationName TEST_DOC_LOCATIONS_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "locations");

    private SQLExecutor e;
    private Functions functions = getFunctions();
    private ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions);
    private PlannerContext plannerCtx;
    private CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void setUpExecutor() throws IOException {
        e = SQLExecutor.builder(clusterService, 2, Randomness.get())
            .addTable(USER_TABLE_DEFINITION)
            .addTable(TEST_DOC_LOCATIONS_TABLE_DEFINITION)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T3_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());
    }

    @After
    public void resetEnableHashJoinFlag() {
        txnCtx.sessionContext().setHashJoinEnabled(true);
    }

    private LogicalPlan createLogicalPlan(MultiSourceSelect mss, TableStats tableStats) {
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, plannerCtx));
        return JoinPlanBuilder.createNodes(mss, mss.where(), subqueryPlanner, functions, txnCtx)
            .build(tableStats, Set.of(), Set.of());
    }

    private Join buildJoin(LogicalPlan operator) {
        return (Join) operator.build(plannerCtx, projectionBuilder, -1, 0, null, null, Row.EMPTY, SubQueryResults.EMPTY);
    }

    private Join plan(MultiSourceSelect mss, TableStats tableStats) {
        return buildJoin(createLogicalPlan(mss, tableStats));
    }

    @Test
    public void testNestedLoop_TablesAreSwitchedIfLeftIsSmallerThanRight() {
        txnCtx.sessionContext().setHashJoinEnabled(false);
        MultiSourceSelect mss = e.normalize("select * from users, locations where users.id = locations.id");

        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> rowCountByTable = new ObjectObjectHashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new TableStats.Stats(10, 0));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(10_000, 0));
        tableStats.updateTableStats(rowCountByTable);

        Join nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("locations"));

        rowCountByTable.put(USER_TABLE_IDENT, new TableStats.Stats(10_000, 0));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(10, 0));
        tableStats.updateTableStats(rowCountByTable);

        nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("users"));
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

        MultiSourceSelect mss = e.normalize("select * from j.left_table as l left join j.right_table as r on l.id = r.id");

        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> rowCountByTable = new ObjectObjectHashMap<>();
        rowCountByTable.put(leftName, new TableStats.Stats(10, 0));
        rowCountByTable.put(rightName, new TableStats.Stats(10_000, 0));
        tableStats.updateTableStats(rowCountByTable);

        Join nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is(leftName.name()));
        assertThat(nl.joinPhase().joinType(), is(JoinType.LEFT));

        rowCountByTable.put(leftName, new TableStats.Stats(10_000, 0));
        rowCountByTable.put(rightName, new TableStats.Stats(10, 0));
        tableStats.updateTableStats(rowCountByTable);

        nl = plan(mss, tableStats);
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is(rightName.name()));
        assertThat(nl.joinPhase().joinType(), is(JoinType.RIGHT));  // ensure that also the join type inverted
    }

    @Test
    public void testNestedLoop_TablesAreNotSwitchedIfLeftHasAPushedDownOrderBy() {
        txnCtx.sessionContext().setHashJoinEnabled(false);
        // we use a subselect to simulate the pushed-down order by
        MultiSourceSelect mss = e.normalize("select users.id from (select id from users order by id) users, " +
                                            "locations where users.id = locations.id");

        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> rowCountByTable = new ObjectObjectHashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new TableStats.Stats(10, 0));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(10_0000, 0));
        tableStats.updateTableStats(rowCountByTable);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));
        LogicalPlan operator = JoinPlanBuilder.createNodes(mss, mss.where(), subqueryPlanner, e.functions(), txnCtx)
            .build(tableStats, Set.of(), Set.of());
        Join nl = (Join) operator.build(
            context, projectionBuilder, -1, 0, null, null, Row.EMPTY, SubQueryResults.EMPTY);

        assertThat(((Collect) nl.left()).collectPhase().toCollect(), isSQL("doc.users.id"));
        assertThat(nl.resultDescription().orderBy(), notNullValue());
    }

    @Test
    public void testNestedLoop_TablesAreNotSwitchedAfterOrderByPushDown() {
        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> rowCountByTable = new ObjectObjectHashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new TableStats.Stats(10, 0));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(10_0000, 0));
        tableStats.updateTableStats(rowCountByTable);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        context.transactionContext().sessionContext().setHashJoinEnabled(false);
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        LogicalPlan plan = logicalPlanner.plan(e.analyze("select users.id from users, locations " +
                                                         "where users.id = locations.id order by users.id"), context);
        Merge merge = (Merge) plan.build(
            context, projectionBuilder, -1, 0, null, null, Row.EMPTY, SubQueryResults.EMPTY);
        Join nl = (Join) merge.subPlan();

        assertThat(((Collect) nl.left()).collectPhase().toCollect(), isSQL("doc.users._fetchid, doc.users.id"));
        assertThat(nl.resultDescription().orderBy(), notNullValue());
    }

    @Test
    public void testHashJoin_TableOrderInLogicalAndExecutionPlan() {
        MultiSourceSelect mss = e.normalize("select users.name, locations.id " +
                                            "from users " +
                                            "join locations on users.id = locations.id");

        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> rowCountByTable = new ObjectObjectHashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new TableStats.Stats(100, 0));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(10, 0));
        tableStats.updateTableStats(rowCountByTable);

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(HashJoin.class));
        assertThat(((HashJoin) operator).concreteRelation.toString(), is("DocTableRelation{doc.locations}"));

        Join join = buildJoin(operator);
        assertThat(join.joinPhase().leftMergePhase().inputTypes(), contains(DataTypes.LONG, DataTypes.LONG));
        assertThat(join.joinPhase().rightMergePhase().inputTypes(), contains(DataTypes.LONG, DataTypes.LONG));
        assertThat(join.joinPhase().projections().get(0).outputs().toString(),
            is("[IC{0, bigint}, IC{1, bigint}, IC{2, bigint}, IC{3, bigint}]"));
    }

    @Test
    public void testHashJoin_TablesSwitchWhenRightBiggerThanLeft() {
        MultiSourceSelect mss = e.normalize("select users.name, locations.id " +
                                            "from users " +
                                            "join locations on users.id = locations.id");

        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> rowCountByTable = new ObjectObjectHashMap<>();
        rowCountByTable.put(USER_TABLE_IDENT, new TableStats.Stats(10, 0));
        rowCountByTable.put(TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(100, 0));
        tableStats.updateTableStats(rowCountByTable);

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(HashJoin.class));
        assertThat(((HashJoin) operator).concreteRelation.toString(), is("DocTableRelation{doc.locations}"));

        Join join = buildJoin(operator);
        // Plans must be switched (left<->right)
        assertThat(join.joinPhase().leftMergePhase().inputTypes(), Matchers.contains(DataTypes.LONG, DataTypes.LONG));
        assertThat(join.joinPhase().rightMergePhase().inputTypes(), Matchers.contains(DataTypes.LONG, DataTypes.LONG));
        assertThat(join.joinPhase().projections().get(0).outputs().toString(),
            is("[IC{2, bigint}, IC{3, bigint}, IC{0, bigint}, IC{1, bigint}]"));
    }

    @Test
    public void testMultipleHashJoins() {
        MultiSourceSelect mss = e.normalize("select * " +
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
        MultiSourceSelect mss = e.normalize("select * " +
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

        MultiSourceSelect mss = e.normalize("select * from t1, t4");

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
        ObjectObjectHashMap<RelationName, TableStats.Stats> stats = new ObjectObjectHashMap<>();
        stats.put(T3.T1_RN, new TableStats.Stats(23, 64));
        stats.put(T3.T4_RN, new TableStats.Stats(42, 64));
        tableStats.updateTableStats(stats);

        // rebuild executor + cluster state with 1 node
        resetClusterService();
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .setTableStats(tableStats)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());

        MultiSourceSelect mss = e.normalize("select * from t1, t4");

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(NestedLoopJoin.class));

        Join join = buildJoin(operator);
        assertThat(join.joinPhase(), instanceOf(NestedLoopPhase.class));
        NestedLoopPhase joinPhase = (NestedLoopPhase) join.joinPhase();
        assertThat(joinPhase.blockNestedLoop, is(true));

        assertThat(join.left(), instanceOf(Collect.class));
        // no table switch should have been made
        assertThat(((Reference) ((Collect) join.left()).collectPhase().toCollect().get(0)).ident().tableIdent(),
            is(T3.T1_RN));
    }

    @Test
    public void testBlockNestedLoopWhenRightSideIsSmallerAndOneExecutionNode() throws IOException {
        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> stats = new ObjectObjectHashMap<>();
        stats.put(T3.T1_RN, new TableStats.Stats(23, 64));
        stats.put(T3.T4_RN, new TableStats.Stats(42, 64));
        tableStats.updateTableStats(stats);

        // rebuild executor + cluster state with 1 node
        resetClusterService();
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .setTableStats(tableStats)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());

        MultiSourceSelect mss = e.normalize("select * from t4, t1");

        LogicalPlan operator = createLogicalPlan(mss, tableStats);
        assertThat(operator, instanceOf(NestedLoopJoin.class));

        Join join = buildJoin(operator);
        assertThat(join.joinPhase(), instanceOf(NestedLoopPhase.class));
        NestedLoopPhase joinPhase = (NestedLoopPhase) join.joinPhase();
        assertThat(joinPhase.blockNestedLoop, is(true));

        assertThat(join.left(), instanceOf(Collect.class));
        // right side will be flipped to the left
        assertThat(((Reference) ((Collect) join.left()).collectPhase().toCollect().get(0)).ident().tableIdent(),
            is(T3.T1_RN));
    }

    @Test
    public void testNoBlockNestedLoopWithOrderBy() throws IOException {
        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> stats = new ObjectObjectHashMap<>();
        stats.put(T3.T1_RN, new TableStats.Stats(23, 64));
        stats.put(T3.T4_RN, new TableStats.Stats(42, 64));
        tableStats.updateTableStats(stats);

        // rebuild executor + cluster state with 1 node
        resetClusterService();
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T4_DEFINITION)
            .setTableStats(tableStats)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());

        MultiSourceSelect mss = e.normalize("select * from t1, t4 order by t1.x");

        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        LogicalPlan operator = logicalPlanner.plan(mss, plannerCtx);
        ExecutionPlan build = operator.build(plannerCtx, projectionBuilder, -1, 0, null,
            null, Row.EMPTY, SubQueryResults.EMPTY);

        assertThat((((NestedLoopPhase) ((Join) ((QueryThenFetch) build).subPlan()).joinPhase())).blockNestedLoop,
            is(false));
    }

    @Test
    public void testPlanChainedJoinsWithWindowFunctionInOutput() {
        txnCtx.sessionContext().setHashJoinEnabled(false);
        MultiSourceSelect mss = e.normalize("SELECT t1.a, t2.b, row_number() OVER(ORDER BY t3.z) " +
                                            "FROM t1 t1 " +
                                            "JOIN t2 t2 on t1.a = t2.b " +
                                            "JOIN t3 t3 on t3.c = t2.b");
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, new TableStats());
        LogicalPlan join = logicalPlanner.plan(mss, plannerCtx);

        WindowAgg windowAggOperator = (WindowAgg) ((FetchOrEval) ((RootRelationBoundary) join).source).source;
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
            "RootBoundary[x, x]\n" +
            "FetchOrEval[x, x]\n" +
            "Boundary[_fetchid, _fetchid]\n" +      // Aliased relation boundary
            "Boundary[_fetchid, _fetchid]\n" +
            "NestedLoopJoin[\n" +
            "    Boundary[_fetchid]\n" +
            "    Collect[doc.t1 | [_fetchid] | All]\n" +
            "    --- CROSS ---\n" +
            "    Boundary[_fetchid]\n" +
            "    Collect[doc.t2 | [_fetchid] | All]\n" +
            "]\n";
        assertThat(logicalPlan, isPlan(e.functions(), expectedPlan));

        var plan = e.plan(statement);
        var qtf = (QueryThenFetch) plan;
        var join = (Join) qtf.subPlan();
        ColumnIdent column = new ColumnIdent("_doc", "x");
        assertThat(join.joinPhase().projections().get(1).outputs(), contains(
            isFetchRef(isInputColumn(0), isReference(is(column), is(new RelationName("doc", "t1")), is(DataTypes.INTEGER))),
            isFetchRef(isInputColumn(1), isReference(is(column), is(new RelationName("doc", "t2")), is(DataTypes.INTEGER)))
        ));
    }

    @Test
    public void testForbidJoinWhereMatchOnBothTables() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            "Using constructs like `match(r1.c) OR match(r2.c)` is not supported");
        e.plan("select * from t1, t2 " +
               "where match(t1.a, 'Lanistas experimentum!') or match(t2.b, 'Rationes ridetis!')");
    }

    /**
     * This scenario will result having a {@link io.crate.analyze.relations.AbstractTableRelation} as a direct
     * child of the {@link MultiSourceSelect} instead of a {@link io.crate.analyze.QueriedSelectRelation} before ANY optimization
     * and validates that the plan can be build.
     *
     */
    @Test
    public void test_subscript_inside_query_spec_of_a_join_is_part_of_the_source_outputs() {
        var logicalPlan = e.logicalPlan("select users.name" +
                                            " from users, t1" +
                                            " where t1.a = users.address['postcode']");
        var expectedPlan =
            "RootBoundary[name]\n" +
            "FetchOrEval[name]\n" +
            "HashJoin[\n" +
            "    Boundary[_fetchid, address['postcode']]\n" +
            "    Collect[doc.users | [_fetchid, address['postcode']] | All]\n" +
            "    --- INNER ---\n" +
            "    Boundary[_fetchid, a]\n" +
            "    Collect[doc.t1 | [_fetchid, a] | All]\n" +
            "]\n";
        assertThat(logicalPlan, is(isPlan(e.functions(), expectedPlan)));

        // Same using an table alias (MSS -> AliasAnalyzedRelation -> AbstractTableRelation)
        logicalPlan = e.logicalPlan("select u.name" +
                                        " from users u, t1" +
                                        " where t1.a = u.address['postcode']");
        expectedPlan =
            "RootBoundary[name]\n" +
            "FetchOrEval[name]\n" +
            "HashJoin[\n" +
            "    Boundary[_fetchid, address['postcode']]\n" +
            "    Boundary[_fetchid, address['postcode']]\n" +
            "    Collect[doc.users | [_fetchid, address['postcode']] | All]\n" +
            "    --- INNER ---\n" +
            "    Boundary[_fetchid, a]\n" +
            "    Collect[doc.t1 | [_fetchid, a] | All]\n" +
            "]\n";
        assertThat(logicalPlan, is(isPlan(e.functions(), expectedPlan)));
    }
}
