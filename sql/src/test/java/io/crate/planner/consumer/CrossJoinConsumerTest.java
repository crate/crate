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

import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.TableStatsService;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrossJoinConsumerTest extends CrateUnitTest {

    private Analyzer analyzer;
    private Planner planner;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final ClusterService clusterService = mock(ClusterService.class);
    private CrossJoinConsumer consumer;
    private final Planner.Context plannerContext = new Planner.Context(clusterService, UUID.randomUUID(), null);
    private TableStatsService statsService;

    @Before
    public void initPlanner() throws Exception {
        Injector injector = new ModulesBuilder()
                .add(new MockedClusterServiceModule())
                .add(new TestModule())
                .add(new MetaDataInformationModule())
                .add(new AggregationImplModule())
                .add(new ScalarFunctionModule())
                .add(new PredicateModule())
                .add(new OperatorModule())
                .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        planner = injector.getInstance(Planner.class);
        consumer = new CrossJoinConsumer(clusterService, mock(AnalysisMetaData.class), statsService);
    }

    private static final TableInfo EMPTY_ROUTING_TABLE = TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, "empty"), new Routing())
            .add("nope", DataTypes.BOOLEAN)
            .build();


    private class TestModule extends MetaDataModule {

        @Override
        protected void configure() {
            super.configure();
            bind(ThreadPool.class).toInstance(newMockedThreadPool());
            statsService = mock(TableStatsService.class);
            when(statsService.numDocs(eq(BaseAnalyzerTest.USER_TABLE_IDENT))).thenReturn(10L);
            when(statsService.numDocs(eq(BaseAnalyzerTest.USER_TABLE_IDENT_MULTI_PK))).thenReturn(5000L);
            when(statsService.numDocs(eq(EMPTY_ROUTING_TABLE.ident()))).thenReturn(0L);
            bind(TableStatsService.class).toInstance(statsService);
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(BaseAnalyzerTest.USER_TABLE_IDENT.name())).thenReturn(BaseAnalyzerTest.USER_TABLE_INFO);
            when(schemaInfo.getTableInfo(BaseAnalyzerTest.USER_TABLE_IDENT_MULTI_PK.name())).thenReturn(BaseAnalyzerTest.USER_TABLE_INFO_MULTI_PK);
            when(schemaInfo.getTableInfo(EMPTY_ROUTING_TABLE.ident().name())).thenReturn(EMPTY_ROUTING_TABLE);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }
    }

    public <T> T plan(String statement) {
        Analysis analysis = analyzer.analyze(
                SqlParser.createStatement(statement),
                new ParameterContext(new Object[0], new Object[0][], null));
        //noinspection unchecked
        return (T) planner.plan(analysis, UUID.randomUUID());
    }

    @Test
    public void testWhereWithNoMatchShouldReturnNoopPlan() throws Exception {
        // TODO: once fetch is supported for cross joins, reset query to:
        // select u1.name, u2.name from users u1, users u2 where 1 = 2
        Plan plan = plan("select u1.name, u2.name from users u1, users u2 where 1 = 2 order by u1.name, u2.name");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testInvalidRelation() throws Exception {
        QueriedTable queriedTable = mock(QueriedTable.class);
        PlannedAnalyzedRelation relation = consumer.consume(
                queriedTable, new ConsumerContext(queriedTable, plannerContext));

        assertThat(relation, Matchers.nullValue());
    }

    @Test
    public void testFetch() throws Exception {
        QueryThenFetch plan = plan("select u1.name, u2.id from users u1, users u2 order by 2");
        NestedLoopPhase nlp = (NestedLoopPhase) ((NestedLoop) plan.subPlan()).resultPhase();
        assertThat(nlp.projections().get(0).outputs(), isSQL("INPUT(0), INPUT(1)"));
        //TopNProjection topN = (TopNProjection) plan.nestedLoopPhase().projections().get(0);
        //assertThat(topN.outputs().get(0), isFunction("concat"));
    }


    @Test
    public void testFunctionWithJoinCondition() throws Exception {
        // TODO: once fetch is supported for cross joins, reset query to:
        // select u1.name || u2.name from users u1, users u2
        NestedLoop plan = plan("select u1.name || u2.name from users u1, users u2 order by u1.name, u2.name");
        TopNProjection topN = (TopNProjection) plan.nestedLoopPhase().projections().get(0);
        assertThat(topN.outputs().get(0), isFunction("concat"));
    }

    @Test
    public void testNoLimitPushDownWithJoinConditionOnDocTables() throws Exception {
        NestedLoop plan = plan("select u1.name, u2.name from users u1, users u2 where u1.name = u2.name  order by 1, 2 limit 10");
        assertThat(((CollectAndMerge) plan.left()).collectPhase().projections().size(), is(0));
        assertThat(((CollectAndMerge) plan.right()).collectPhase().projections().size(), is(0));
    }

    @Test
    public void testJoinConditionInWhereClause() throws Exception {
        // TODO: once fetch is supported for cross joins, reset query to:
        // select u1.name, u2.name from users u1, users u2 where u1.name || u2.name = 'foobar'
        NestedLoop plan = plan("select u1.floats, u2.name from users u1, users u2 where u1.name || u2.name = 'foobar' order by u1.floats, u2.name");

        assertThat(plan.nestedLoopPhase().projections().size(), is(2));
        FilterProjection fp = ((FilterProjection) plan.nestedLoopPhase().projections().get(0));

        assertThat(((Function)fp.query()).arguments().size(), is(2));
        assertThat(fp.outputs().size(), is(3));

        TopNProjection topN = ((TopNProjection) plan.nestedLoopPhase().projections().get(1));
        assertThat(topN.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(topN.offset(), is(0));
        // TODO: once fetch is supported and query is changed, output size will be 4 here
        assertThat(topN.outputs().size(), is(2));

        assertThat(plan.resultPhase(), instanceOf(MergePhase.class));
        MergePhase localMergePhase = (MergePhase) plan.resultPhase();
        assertThat(localMergePhase.projections().size(), is(1));

        TopNProjection finalTopN = ((TopNProjection) localMergePhase.projections().get(0));
        assertThat(finalTopN.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(finalTopN.offset(), is(0));
        assertThat(finalTopN.outputs().size(), is(2));
    }

    @Test
    public void testLeftSideIsBroadcastIfLeftTableIsSmaller() throws Exception {
        NestedLoop plan = plan("select users.name, u2.name from users, users_multi_pk u2 " +
                               "where users.name = u2.name " +
                               "order by users.name, u2.name ");
        assertThat(plan.left().resultPhase().distributionInfo().distributionType(), is(DistributionType.BROADCAST));
    }


    @Test
    public void testExplicitCrossJoinWithoutLimitOrOrderBy() throws Exception {
        QueryThenFetch plan = plan("select u1.name, u2.name from users u1 cross join users u2");
        NestedLoop nestedLoop = (NestedLoop) plan.subPlan();
        assertThat(nestedLoop.nestedLoopPhase().projections().size(), is(1));
        TopNProjection topN = ((TopNProjection) nestedLoop.nestedLoopPhase().projections().get(0));
        assertThat(topN.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
        assertThat(topN.offset(), is(0));
        assertThat(topN.outputs().size(), is(2));

        MergePhase leftMerge = nestedLoop.nestedLoopPhase().leftMergePhase();
        assertThat(leftMerge.projections().size(), is(0));

        MergePhase rightMerge = nestedLoop.nestedLoopPhase().rightMergePhase();
        assertThat(rightMerge.projections().size(), is(0));
    }


    @Test
    public void testNoLimitPushDownWithJoinCondition() throws Exception {
        NestedLoop plan = plan("select * from information_schema.tables, information_schema .columns " +
                               "where tables.schema_name = columns.schema_name " +
                               "and tables.table_name = columns.table_name limit 10");
        assertThat(((CollectAndMerge) plan.left()).collectPhase().projections().size(), is(0));
        assertThat(((CollectAndMerge) plan.right()).collectPhase().projections().size(), is(0));
    }

    @Test
    public void testNoNodePageSizeHintPushDownWithJoinCondition() throws Exception {
        NestedLoop plan = plan("select * from information_schema.tables, information_schema .columns " +
                               "where tables.schema_name = columns.schema_name " +
                               "and tables.table_name = columns.table_name limit 10");
        assertThat(((CollectAndMerge) plan.left()).collectPhase().nodePageSizeHint(), nullValue());
        assertThat(((CollectAndMerge) plan.right()).collectPhase().nodePageSizeHint(), nullValue());
    }

    @Test
    public void testNLPhaseHasFilterProjection() throws Exception {
        NestedLoop plan = plan("select * from information_schema.tables, information_schema .columns " +
                               "where tables.schema_name = columns.schema_name " +
                               "and tables.table_name = columns.table_name limit 10");
        assertThat(plan.nestedLoopPhase().projections().get(0), instanceOf(FilterProjection.class));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testOrderByPushDown() throws Exception {
        // TODO: once fetch is supported for cross joins, reset query to:
        // select u1.name, u2.name from users u1, users u2 order by u1.name

        NestedLoop plan = plan("select u1.name, u2.name from users u1, users u2 order by u1.name, u2.name");

        assertThat(plan.left().resultPhase(), instanceOf(CollectPhase.class));
        CollectAndMerge leftPlan = (CollectAndMerge) plan.left().plan();
        CollectPhase collectPhase = leftPlan.collectPhase();
        assertThat(collectPhase.projections().size(), is(0));
        assertThat(collectPhase.toCollect().get(0), isReference("name"));
    }

    @Test
    public void testNodePageSizePushDown() throws Exception {
        NestedLoop plan = plan("select u1.name from users u1, users u2 order by 1 limit 1000");
        CollectPhase cpL = ((CollectAndMerge) plan.left().plan()).collectPhase();
        assertThat(cpL.nodePageSizeHint(), is(750));

        CollectPhase cpR = ((CollectAndMerge) plan.right().plan()).collectPhase();
        assertThat(cpR.nodePageSizeHint(), is(750));
    }

    @Test
    public void testCrossJoinWithGroupBy() throws Exception {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("GROUP BY on CROSS JOIN is not supported");
        plan("select u1.name, count(*) from users u1, users u2 group by u1.name");
    }

    @Test
    public void testAggregationOnCrossJoin() throws Exception {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("AGGREGATIONS on CROSS JOIN is not supported");
        // TODO: once fetch is supported for cross joins, reset query to:
        // select min(u1.name) from users u1, users u2

        plan("select min(u1.name) from users u1, users u2");
    }

    @Test
    public void testOrderByOnJoinCondition() throws Exception {
        NestedLoop nl = plan("select u1.name || u2.name from users u1, users u2 order by u1.name, u1.name || u2.name");
        List<Symbol> orderBy = ((TopNProjection) nl.nestedLoopPhase().projections().get(0)).orderBy();
        assertThat(orderBy, notNullValue());
        assertThat(orderBy.size(), is(1));
        assertThat(orderBy.get(0), isFunction("concat"));
    }

    @Test
    public void testLimitIncludesOffsetOnNestedLoopTopNProjection() throws Exception {
        NestedLoop nl = plan("select u1.name, u2.name from users u1, users u2 where u1.id = u2.id order by u1.name, u2.name limit 15 offset 10");
        TopNProjection distTopN = (TopNProjection) nl.nestedLoopPhase().projections().get(1);

        assertThat(distTopN.limit(), is(25));
        assertThat(distTopN.offset(), is(0));

        TopNProjection localTopN = (TopNProjection) nl.localMerge().projections().get(0);
        assertThat(localTopN.limit(), is(15));
        assertThat(localTopN.offset(), is(10));
    }

    @Test
    public void testRefsAreNotConvertedToSourceLookups() throws Exception {
        NestedLoop nl = plan("select u1.name from users u1, users u2 where u1.id = u2.id order by 1");
        CollectPhase cpLeft = ((CollectAndMerge) nl.left().plan()).collectPhase();
        assertThat(cpLeft.toCollect(), contains(isReference("id"), isReference("name")));
        CollectPhase cpRight = ((CollectAndMerge) nl.right().plan()).collectPhase();
        assertThat(cpRight.toCollect(), contains(isReference("id")));
    }

    @Test
    public void testEmptyRoutingSource() throws Exception {
        Plan plan = plan("select e.nope, u.name from empty e, users u order by e.nope, u.name");
        assertThat(plan, instanceOf(NoopPlan.class));
    }
}