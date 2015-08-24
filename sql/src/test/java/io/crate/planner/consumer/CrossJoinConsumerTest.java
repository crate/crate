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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.MergeProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Iterator;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class CrossJoinConsumerTest extends CrateUnitTest {

    private Analyzer analyzer;
    private Planner planner;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final ClusterService clusterService = mock(ClusterService.class);
    private final CrossJoinConsumer consumer =
            new CrossJoinConsumer(clusterService, mock(AnalysisMetaData.class), mock(ConsumingPlanner.class));
    private final Planner.Context plannerContext = new Planner.Context(clusterService, UUID.randomUUID(), null);

    @Before
    public void initPlanner() throws Exception {
        Injector injector = new ModulesBuilder()
                .add(new TestModule())
                .add(new AggregationImplModule())
                .add(new ScalarFunctionModule())
                .add(new PredicateModule())
                .add(new OperatorModule())
                .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
        planner = injector.getInstance(Planner.class);
    }


    private class TestModule implements Module {
        @Override
        public void configure(Binder binder) {
            binder.bind(NestedReferenceResolver.class).toInstance(mock(NestedReferenceResolver.class));
            binder.bind(FulltextAnalyzerResolver.class).toInstance(mock(FulltextAnalyzerResolver.class));
            binder.bind(ClusterService.class).toInstance(new NoopClusterService());
            binder.bind(Schemas.class).toInstance(new Schemas() {
                @Override
                public DocTableInfo getWritableTable(TableIdent tableIdent) {
                    return null;
                }

                @Override
                public TableInfo getTableInfo(TableIdent ident) {
                    if (ident.name().equals("users")) {
                        return BaseAnalyzerTest.userTableInfo;
                    }
                    throw new TableUnknownException(ident);
                }

                @Override
                public boolean tableExists(TableIdent tableIdent) {
                    return false;
                }

                @Override
                public Iterator<SchemaInfo> iterator() {
                    return null;
                }
            });
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
        Plan plan = plan("select * from users u1, users u2 where 1 = 2");
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
    public void testInvalidMultiSourceSelect() throws Exception {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("At least 2 relations are required for a CROSS JOIN");

        MultiSourceSelect statement = new MultiSourceSelect(
                ImmutableMap.<QualifiedName, AnalyzedRelation>of(new QualifiedName("foo"), mock(QueriedTable.class)),
                ImmutableList.<OutputName>of(),
                mock(QuerySpec.class)
        );
        ConsumerContext context = new ConsumerContext(statement, plannerContext);
        consumer.consume(statement, context);
        throw context.validationException();
    }

    @Test
    public void testFunctionWithJoinCondition() throws Exception {
        NestedLoop plan = plan("select u1.name || u2.name from users u1, users u2");
        TopNProjection topN = (TopNProjection) plan.nestedLoopPhase().projections().get(0);
        assertThat(topN.outputs().get(0), isFunction("concat"));
    }

    @Test
    public void testJoinConditionInWhereClause() throws Exception {
        NestedLoop plan = plan("select * from users u1, users u2 where u1.name || u2.name = 'foobar'");
        Projection projection = plan.nestedLoopPhase().projections().get(0);
        assertThat(projection, instanceOf(FilterProjection.class));
        Symbol query = ((FilterProjection) projection).query();
        assertThat(query, isFunction(EqOperator.NAME));
    }

    @Test
    public void testExplicitCrossJoinWithoutLimitOrOrderBy() throws Exception {
         NestedLoop plan = plan("select u1.name, u2.name from users u1 cross join users u2");

         assertThat(plan.nestedLoopPhase().projections().size(), is(1));
         TopNProjection topN = ((TopNProjection) plan.nestedLoopPhase().projections().get(0));

         assertThat(topN.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
         assertThat(topN.offset(), is(0));
         assertThat(topN.outputs().size(), is(2));


         assertThat(plan.left().resultNode().projections().size(), is(0));
         MergePhase leftMerge = plan.nestedLoopPhase().leftMergePhase();
         assertThat(leftMerge.projections().size(), is(0));

         assertThat(plan.right().resultNode().projections().size(), is(0));
         MergePhase rightMerge = plan.nestedLoopPhase().rightMergePhase();
         assertThat(rightMerge.projections().size(), is(0));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testOrderByPushDown() throws Exception {
        NestedLoop plan = plan("select u1.name, u2.name from users u1, users u2 order by u1.name");
        assertThat(plan.left().resultNode().projections().size(), is(2));

        CollectAndMerge leftPlan = (CollectAndMerge) plan.left().plan();
        CollectPhase collectPhase = leftPlan.collectPhase();
        assertThat(collectPhase.projections().size(), is(2));
        MergeProjection projection = ((MergeProjection) collectPhase.projections().get(0));
        assertThat(collectPhase.toCollect().get(0), isReference("name"));
        assertThat(projection.orderBy().size(), is(1));
        assertThat(projection.orderBy().get(0), isInputColumn(0));
        TopNProjection topNProjection = (TopNProjection)collectPhase.projections().get(1);
        assertThat(topNProjection.isOrdered(), is(false));
        assertThat(topNProjection.offset(), is(0));
        assertThat(topNProjection.limit(), is(Constants.DEFAULT_SELECT_LIMIT));
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
        plan("select min(u1.name) from users u1, users u2");
    }

    @Test
    public void testCrossJoinWithMoreThanTwoTables() throws Exception {
        NestedLoop plan = plan("select * from users u1, users u2, users u3");
        assertThat(plan.left().plan(), instanceOf(CollectAndMerge.class));
        assertThat(plan.right().plan(), instanceOf(NestedLoop.class));
        NestedLoop innerPlan = (NestedLoop) plan.right().plan();
        assertThat(innerPlan.left().plan(), instanceOf(CollectAndMerge.class));
        assertThat(innerPlan.right().plan(), instanceOf(CollectAndMerge.class));
    }
}