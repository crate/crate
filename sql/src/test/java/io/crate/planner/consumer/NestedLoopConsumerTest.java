/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.planner.consumer;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.planner.*;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.*;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.crate.testing.SymbolMatchers.*;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class NestedLoopConsumerTest extends CrateDummyClusterServiceUnitTest {

    private final DocTableInfo emptyRoutingTable = TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, "empty"),
        new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("nope", DataTypes.BOOLEAN)
        .build();

    private NestedLoopConsumer consumer;
    private Planner.Context plannerContext;
    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        TableStats tableStats = getTableStats();
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .setTableStats(tableStats)
            .addDocTable(emptyRoutingTable)
            .build();
        Functions functions = e.functions();
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.COPY);
        plannerContext = new Planner.Context(
            e.planner,
            clusterService,
            UUID.randomUUID(),
            new ConsumingPlanner(clusterService, functions, tableStats),
            normalizer,
            new TransactionContext(SessionContext.SYSTEM_SESSION),
            0,
            0);
        consumer = new NestedLoopConsumer(clusterService, functions, tableStats);
    }

    private TableStats getTableStats() {
        ObjectLongMap<TableIdent> stats = new ObjectLongHashMap<>(3);
        stats.put(TableDefinitions.USER_TABLE_IDENT, 10L);
        stats.put(TableDefinitions.USER_TABLE_IDENT_MULTI_PK, 5000L);
        stats.put(emptyRoutingTable.ident(), 0L);
        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(stats);
        return tableStats;
    }

    public <T extends Plan> T plan(String statement) {
        return e.plan(statement, UUID.randomUUID(), 0, 0);
    }

    @Test
    public void testWhereWithNoMatchShouldReturnNoopPlan() throws Exception {
        Plan plan = plan("select u1.name, u2.name from users u1, users u2 where 1 = 2");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testInvalidRelation() throws Exception {
        QueriedTable queriedTable = mock(QueriedTable.class);
        Plan relation = consumer.consume(queriedTable, new ConsumerContext(plannerContext));

        assertThat(relation, Matchers.nullValue());
    }

    @Test
    public void testFetch() throws Exception {
        QueryThenFetch plan = plan("select u1.name, u2.id from users u1, users u2 order by 2");
        NestedLoopPhase nlp = ((NestedLoop) plan.subPlan()).nestedLoopPhase();
        assertThat(nlp.projections().get(0).outputs(), isSQL("INPUT(1), INPUT(0)"));
    }

    @Test
    public void testGlobalAggWithWhereDoesNotResultInFilterProjection() throws Exception {
        NestedLoop nl = plan("select min(u1.name) from users u1, users u2 where u1.name like 'A%'");
        assertThat(nl.nestedLoopPhase().projections(), contains(
            instanceOf(EvalProjection.class),
            instanceOf(AggregationProjection.class),
            instanceOf(EvalProjection.class)
        ));
    }

    @Test
    public void testFunctionWithJoinCondition() throws Exception {
        QueryThenFetch qtf = plan("select u1.name || u2.name from users u1, users u2");
        FetchProjection fetch = (FetchProjection) ((NestedLoop) qtf.subPlan()).nestedLoopPhase().projections().get(1);
        assertThat(fetch.outputs(), isSQL("concat(FETCH(INPUT(0), doc.users._doc['name']), FETCH(INPUT(1), doc.users._doc['name']))"));
    }

    @Test
    public void testNoLimitPushDownWithJoinConditionOnDocTables() throws Exception {
        Merge merge = plan("select u1.name, u2.name from users u1, users u2 where u1.name = u2.name  order by 1, 2 limit 10");
        NestedLoop nl = (NestedLoop) merge.subPlan();
        assertThat(((Collect) nl.left()).collectPhase().projections().size(), is(0));
        assertThat(((Collect) nl.right()).collectPhase().projections().size(), is(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testJoinConditionInWhereClause() throws Exception {
        QueryThenFetch qtf = plan("select u1.floats, u2.name from users u1, users u2 where u1.name || u2.name = 'foobar'");
        Merge merge = (Merge) qtf.subPlan();

        NestedLoop nestedLoop = (NestedLoop) merge.subPlan();
        assertThat(nestedLoop.nestedLoopPhase().projections(),
            Matchers.contains(instanceOf(FilterProjection.class), instanceOf(EvalProjection.class)));

        EvalProjection eval = ((EvalProjection) nestedLoop.nestedLoopPhase().projections().get(1));
        assertThat(eval.outputs().size(), is(3));

        MergePhase localMergePhase = merge.mergePhase();
        assertThat(localMergePhase.projections(),
            Matchers.contains(instanceOf(FetchProjection.class)));

        FetchProjection fetchProjection = (FetchProjection) localMergePhase.projections().get(0);
        assertThat(fetchProjection.outputs(), isSQL("FETCH(INPUT(0), doc.users._doc['floats']), INPUT(2)"));
    }

    @Test
    public void testLeftSideIsBroadcastIfLeftTableIsSmaller() throws Exception {
        Merge merge = plan("select users.name, u2.name from users, users_multi_pk u2 " +
                           "where users.name = u2.name " +
                           "order by users.name, u2.name ");
        NestedLoop nl = (NestedLoop) merge.subPlan();
        Collect collect = (Collect) nl.left();
        assertThat(collect.collectPhase().distributionInfo().distributionType(), is(DistributionType.BROADCAST));
    }


    @Test
    public void testExplicitCrossJoinWithoutLimitOrOrderBy() throws Exception {
        QueryThenFetch plan = plan("select u1.name, u2.name from users u1 cross join users u2");
        NestedLoop nestedLoop = (NestedLoop) plan.subPlan();
        assertThat(nestedLoop.nestedLoopPhase().projections(),
            Matchers.contains(instanceOf(EvalProjection.class), instanceOf(FetchProjection.class)));
        EvalProjection eval = ((EvalProjection) nestedLoop.nestedLoopPhase().projections().get(0));
        assertThat(eval.outputs().size(), is(2));

        MergePhase leftMerge = nestedLoop.nestedLoopPhase().leftMergePhase();
        assertThat(leftMerge.projections().size(), is(0));

        MergePhase rightMerge = nestedLoop.nestedLoopPhase().rightMergePhase();
        assertThat(rightMerge.projections().size(), is(0));
    }


    @Test
    public void testNoLimitPushDownWithJoinCondition() throws Exception {
        NestedLoop plan = plan("select * from information_schema.tables, information_schema .columns " +
                               "where tables.table_schema = columns.table_schema " +
                               "and tables.table_name = columns.table_name limit 10");
        assertThat(((Collect) plan.left()).collectPhase().projections().size(), is(0));
        assertThat(((Collect) plan.right()).collectPhase().projections().size(), is(0));
    }

    @Test
    public void testNoNodePageSizeHintPushDownWithJoinCondition() throws Exception {
        NestedLoop plan = plan("select * from information_schema.tables, information_schema .columns " +
                               "where tables.table_schema = columns.table_schema " +
                               "and tables.table_name = columns.table_name limit 10");
        assertThat(((RoutedCollectPhase) ((Collect) plan.left()).collectPhase()).nodePageSizeHint(), nullValue());
        assertThat(((RoutedCollectPhase) ((Collect) plan.right()).collectPhase()).nodePageSizeHint(), nullValue());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testOrderByPushDown() throws Exception {
        QueryThenFetch qtf = plan("select u1.name, u2.name from users u1, users u2 order by u1.name");
        NestedLoop nl = (NestedLoop) qtf.subPlan();

        assertThat(nl.left().resultDescription(), instanceOf(Collect.class));
        Collect leftPlan = (Collect) nl.left();
        CollectPhase collectPhase = leftPlan.collectPhase();
        assertThat(collectPhase.projections().size(), is(0));
        assertThat(collectPhase.toCollect().get(0), isReference("name"));
    }

    @Test
    public void testNodePageSizePushDown() throws Exception {
        NestedLoop plan = plan("select u1.name from users u1, users u2 order by 1 limit 1000");
        RoutedCollectPhase cpL = ((RoutedCollectPhase) ((Collect) plan.left()).collectPhase());
        assertThat(cpL.nodePageSizeHint(), is(750));

        RoutedCollectPhase cpR = ((RoutedCollectPhase) ((Collect) plan.right()).collectPhase());
        assertThat(cpR.nodePageSizeHint(), is(750));
    }

    @Test
    public void testAggregationOnCrossJoin() throws Exception {
        NestedLoop nl = plan("select min(u1.name) from users u1, users u2");
        NestedLoopPhase nlPhase = nl.nestedLoopPhase();
        assertThat(nlPhase.projections(), contains(
            instanceOf(EvalProjection.class),
            instanceOf(AggregationProjection.class),
            instanceOf(EvalProjection.class)
        ));
        AggregationProjection aggregationProjection = (AggregationProjection) nlPhase.projections().get(1);
        assertThat(aggregationProjection.mode(), is(AggregateMode.ITER_FINAL));
    }

    @Test
    public void testAggregationOnNoMatch() throws Exception {
        // shouldn't result in a NoopPlan because aggregations still need to be executed
        NestedLoop nl = plan("select count(*) from users u1, users u2 where false");
        assertThat(nl.nestedLoopPhase().projections(), contains(
            instanceOf(EvalProjection.class),
            instanceOf(AggregationProjection.class),
            instanceOf(EvalProjection.class)
        ));
    }

    @Test
    public void testOrderByOnJoinCondition() throws Exception {
        NestedLoop nl = plan("select u1.name || u2.name from users u1, users u2 order by u1.name, u1.name || u2.name");
        List<Symbol> orderBy = ((OrderedTopNProjection) nl.nestedLoopPhase().projections().get(0)).orderBy();
        assertThat(orderBy, notNullValue());
        assertThat(orderBy.size(), is(2));
        assertThat(orderBy.get(0), isInputColumn(0));
        assertThat(orderBy.get(1), isFunction("concat"));
    }

    @Test
    public void testLimitIncludesOffsetOnNestedLoopTopNProjection() throws Exception {
        Merge merge = plan("select u1.name, u2.name from users u1, users u2 where u1.id = u2.id order by u1.name, u2.name limit 15 offset 10");
        NestedLoop nl = (NestedLoop) merge.subPlan();
        TopNProjection distTopN = (TopNProjection) nl.nestedLoopPhase().projections().get(1);

        assertThat(distTopN.limit(), is(25));
        assertThat(distTopN.offset(), is(0));

        TopNProjection localTopN = (TopNProjection) merge.mergePhase().projections().get(0);
        assertThat(localTopN.limit(), is(15));
        assertThat(localTopN.offset(), is(10));
    }

    @Test
    public void testRefsAreNotConvertedToSourceLookups() throws Exception {
        Merge merge = plan("select u1.name from users u1, users u2 where u1.id = u2.id order by 1");
        NestedLoop nl = (NestedLoop) merge.subPlan();
        CollectPhase cpLeft = ((Collect) nl.left()).collectPhase();
        assertThat(cpLeft.toCollect(), contains(isReference("id"), isReference("name")));
        CollectPhase cpRight = ((Collect) nl.right()).collectPhase();
        assertThat(cpRight.toCollect(), contains(isReference("id")));
    }

    @Test
    public void testEmptyRoutingSource() throws Exception {
        Plan plan = plan("select e.nope, u.name from empty e, users u order by e.nope, u.name");
        assertThat(plan, instanceOf(NestedLoop.class));
    }

    @Test
    public void testLimitNotAppliedWhenFilteringRemains() throws Exception {
        QueryThenFetch plan = plan("select * from users u1 " +
                                   "left join users u2 on u1.id=u2.id " +
                                   "left join users u3 on u2.id=u3.id " +
                                   "left join users u4 on u3.id=u4.id " +
                                   "where u1.name = u4.name " +
                                   "limit 10");
        NestedLoopPhase nl = ((NestedLoop) plan.subPlan()).nestedLoopPhase();
        assertThat(nl.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(((TopNProjection)nl.projections().get(1)).limit(), is(10));
        nl = ((NestedLoop) ((NestedLoop) plan.subPlan()).left()).nestedLoopPhase();
        assertThat(nl.projections().get(0), instanceOf(EvalProjection.class));
        nl = ((NestedLoop) ((NestedLoop) ((NestedLoop) plan.subPlan()).left()).left()).nestedLoopPhase();
        assertThat(nl.projections().get(0), instanceOf(EvalProjection.class));
    }

    @Test
    public void testGlobalAggregateWithExplicitCrossJoinSyntax() throws Exception {
        // using explicit cross join syntax caused a NPE due to joinPair being present but the condition being null.
        Plan plan = plan("select count(t1.col1) from unnest([1, 2]) as t1 cross join unnest([1, 2]) as t2");
        assertThat(plan, instanceOf(NestedLoop.class));
    }

    @Test
    public void testDistributedJoinWithGroupByNotSupported() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Distributed execution is not supported");
        plan("select count(u1.name) from users u1, users u2 where u1.id = u2.id group by u1.name");
    }
}
