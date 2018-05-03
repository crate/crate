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
import io.crate.action.sql.SessionContext;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.join.Join;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static io.crate.testing.TestingHelpers.getFunctions;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class JoinTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private Functions functions = getFunctions();
    private ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions);
    private TransactionContext txnCtx = new TransactionContext();

    @Before
    public void setUpExecutor() {
        e = SQLExecutor.builder(clusterService)
            .addDocTable(TableDefinitions.USER_TABLE_INFO)
            .addDocTable(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_INFO)
            .addDocTable(T3.T1_INFO)
            .addDocTable(T3.T2_INFO)
            .addDocTable(T3.T3_INFO)
            .build();
    }

    @Test
    public void testTablesAreSwitchedIfLeftIsSmallerThanRight() {
        MultiSourceSelect mss = e.analyze("select * from users, locations where users.id = locations.id");

        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> rowCountByTable = new ObjectObjectHashMap<>();
        rowCountByTable.put(TableDefinitions.USER_TABLE_IDENT, new TableStats.Stats(10, 0));
        rowCountByTable.put(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(10_000, 0));
        tableStats.updateTableStats(rowCountByTable);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));
        LogicalPlan operator = JoinPlanBuilder.createNodes(mss, mss.where(), subqueryPlanner, e.functions(), txnCtx)
            .build(tableStats, Collections.emptySet());
        Join nl = (Join) operator.build(
            context, projectionBuilder, -1, 0, null, null, Row.EMPTY, emptyMap());
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("locations"));

        rowCountByTable.put(TableDefinitions.USER_TABLE_IDENT, new TableStats.Stats(10_000, 0));
        rowCountByTable.put(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(10, 0));
        tableStats.updateTableStats(rowCountByTable);

        operator = JoinPlanBuilder.createNodes(mss, mss.where(), subqueryPlanner, functions, txnCtx)
            .build(tableStats, Collections.emptySet());
        nl = (Join) operator.build(context, projectionBuilder, -1, 0, null, null, Row.EMPTY, emptyMap());
        assertThat(((Reference) ((Collect) nl.left()).collectPhase().toCollect().get(0)).ident().tableIdent().name(), is("users"));
    }

    @Test
    public void testHashJoinTableOrderInLogicalAndExecutionPlan() {
        MultiSourceSelect mss = e.analyze("select users.name, locations.id " +
                                          "from users " +
                                          "join locations on users.id = locations.id");

        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> rowCountByTable = new ObjectObjectHashMap<>();
        rowCountByTable.put(TableDefinitions.USER_TABLE_IDENT, new TableStats.Stats(100, 0));
        rowCountByTable.put(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(10, 0));
        tableStats.updateTableStats(rowCountByTable);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));
        LogicalPlan operator = JoinPlanBuilder.createNodes(mss, mss.where(), subqueryPlanner, functions, txnCtx)
            .build(tableStats, Collections.emptySet());
        assertThat(operator, instanceOf(HashJoin.class));
        assertThat(((HashJoin) operator).concreteRelation.toString(), is("QueriedTable{DocTableRelation{doc.locations}}"));

        Join join = (Join) operator.build(context, projectionBuilder, -1, 0, null, null, Row.EMPTY, emptyMap());
        assertThat(join.joinPhase().leftMergePhase().inputTypes(), contains(DataTypes.LONG, DataTypes.LONG));
        assertThat(join.joinPhase().rightMergePhase().inputTypes(), contains(DataTypes.LONG));
        assertThat(join.joinPhase().projections().get(0).outputs().toString(),
            is("[IC{0, long}, IC{1, long}, IC{2, long}]"));
    }

    @Test
    public void testHashJoinTablesSwitchWhenRightBiggerThanLeft() {
        MultiSourceSelect mss = e.analyze("select users.name, locations.id " +
                                          "from users " +
                                          "join locations on users.id = locations.id");

        TableStats tableStats = new TableStats();
        ObjectObjectHashMap<RelationName, TableStats.Stats> rowCountByTable = new ObjectObjectHashMap<>();
        rowCountByTable.put(TableDefinitions.USER_TABLE_IDENT, new TableStats.Stats(10, 0));
        rowCountByTable.put(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT, new TableStats.Stats(100, 0));
        tableStats.updateTableStats(rowCountByTable);

        PlannerContext context = e.getPlannerContext(clusterService.state());
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));
        SessionContext sessionContext = SessionContext.create();
        LogicalPlan operator = JoinPlanBuilder.createNodes(mss, mss.where(), subqueryPlanner, functions, txnCtx)
            .build(tableStats, Collections.emptySet());
        assertThat(operator, instanceOf(HashJoin.class));
        assertThat(((HashJoin) operator).concreteRelation.toString(), is("QueriedTable{DocTableRelation{doc.locations}}"));

        Join join = (Join) operator.build(context, projectionBuilder, -1, 0, null, null, Row.EMPTY, emptyMap());
        // Plans must be switched (left<->right)
        assertThat(join.joinPhase().leftMergePhase().inputTypes(), Matchers.contains(DataTypes.LONG));
        assertThat(join.joinPhase().rightMergePhase().inputTypes(), Matchers.contains(DataTypes.LONG, DataTypes.LONG));
        assertThat(join.joinPhase().projections().get(0).outputs().toString(),
            is("[IC{1, long}, IC{2, long}, IC{0, long}]"));
    }

    @Test
    public void testMultipleHashJoins() {
        MultiSourceSelect mss = e.analyze("select * " +
                                          "from t1 inner join t2 on t1.a = t2.b " +
                                          "inner join t3 on t3.c = t2.b");

        TableStats tableStats = new TableStats();
        PlannerContext context = e.getPlannerContext(clusterService.state());
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));
        SessionContext sessionContext = SessionContext.create();
        sessionContext.setHashJoinEnabled(true);
        LogicalPlan operator = JoinPlanBuilder.createNodes(mss, mss.where(), subqueryPlanner, functions, txnCtx)
            .build(tableStats, Collections.emptySet());
        assertThat(operator, instanceOf(HashJoin.class));
        LogicalPlan leftPlan = ((HashJoin) operator).lhs;
        assertThat(leftPlan, instanceOf(HashJoin.class));

        Join join = (Join) operator.build(context, projectionBuilder, -1, 0, null, null, Row.EMPTY, emptyMap());
        assertThat(join.joinPhase(), instanceOf(HashJoinPhase.class));
        assertThat(join.left(), instanceOf(Join.class));
        assertThat(((Join)join.left()).joinPhase(), instanceOf(HashJoinPhase.class));
    }

    @Test
    public void testMixedHashJoinNestedLoop() {
        MultiSourceSelect mss = e.analyze("select * " +
                                          "from t1 inner join t2 on t1.a = t2.b " +
                                          "left join t3 on t3.c = t2.b");

        TableStats tableStats = new TableStats();
        PlannerContext context = e.getPlannerContext(clusterService.state());
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));
        SessionContext sessionContext = SessionContext.create();
        sessionContext.setHashJoinEnabled(true);
        LogicalPlan operator = JoinPlanBuilder.createNodes(mss, mss.where(), subqueryPlanner, functions, txnCtx)
            .build(tableStats, Collections.emptySet());
        assertThat(operator, instanceOf(NestedLoopJoin.class));
        LogicalPlan leftPlan = ((NestedLoopJoin) operator).lhs;
        assertThat(leftPlan, instanceOf(HashJoin.class));

        Join join = (Join) operator.build(context, projectionBuilder, -1, 0, null, null, Row.EMPTY, emptyMap());
        assertThat(join.joinPhase(), instanceOf(NestedLoopPhase.class));
        assertThat(join.left(), instanceOf(Join.class));
        assertThat(((Join)join.left()).joinPhase(), instanceOf(HashJoinPhase.class));
    }

}
