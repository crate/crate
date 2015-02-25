/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.*;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.AverageAggregation;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static io.crate.testing.TestingHelpers.isRow;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;


public class ProjectionToProjectorVisitorTest {

    protected static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));
    private ProjectionToProjectorVisitor visitor;
    private FunctionInfo countInfo;
    private FunctionInfo avgInfo;
    private Functions functions;

    private final RowN spare = new RowN(new Object[]{});

    private Row spare(Object... cells) {
        spare.cells(cells);
        return spare;
    }

    @Before
    public void prepare() {
        MockitoAnnotations.initMocks(this);
        ReferenceResolver referenceResolver = new GlobalReferenceResolver(new HashMap<ReferenceIdent, ReferenceImplementation>());
        Injector injector = new ModulesBuilder()
                .add(new AggregationImplModule())
                .add(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Client.class).toInstance(mock(Client.class));
                    }
                })
                .add(new OperatorModule())
                .createInjector();
        functions = injector.getInstance(Functions.class);
        ImplementationSymbolVisitor symbolvisitor =
                new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.NODE);
        visitor = new ProjectionToProjectorVisitor(
                mock(ClusterService.class),
                ImmutableSettings.EMPTY,
                mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
                symbolvisitor);

        countInfo = new FunctionInfo(new FunctionIdent(CountAggregation.NAME, Arrays.<DataType>asList(DataTypes.STRING)), DataTypes.LONG);
        avgInfo = new FunctionInfo(new FunctionIdent(AverageAggregation.NAME, Arrays.<DataType>asList(DataTypes.INTEGER)), DataTypes.DOUBLE);
    }

    @Test
    public void testSimpleTopNProjection() throws ExecutionException, InterruptedException {
        TopNProjection projection = new TopNProjection(10, 2);
        projection.outputs(Arrays.<Symbol>asList(Literal.newLiteral("foo"), new InputColumn(0)));

        CollectingProjector collectingProjector = new CollectingProjector();
        Projector projector = visitor.process(projection, RAM_ACCOUNTING_CONTEXT);
        projector.registerUpstream(null);
        ((ProjectorUpstream) projector).downstream(collectingProjector);
        assertThat(projector, instanceOf(SimpleTopNProjector.class));

        projector.startProjection();
        int i;
        for (i = 0; i < 20; i++) {
            if (!projector.setNextRow(spare(42))) {
                break;
            }
        }
        assertThat(i, is(11));
        projector.upstreamFinished();
        Bucket rows = collectingProjector.result().get();
        assertThat(rows.size(), is(10));
        assertThat(rows.iterator().next(), isRow("foo", 42));
    }

    @Test
    public void testSortingTopNProjection() throws ExecutionException, InterruptedException {
        TopNProjection projection = new TopNProjection(10, 0,
                Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)),
                new boolean[]{false, false},
                new Boolean[]{null, null}
        );
        projection.outputs(Arrays.<Symbol>asList(Literal.newLiteral("foo"), new InputColumn(0), new InputColumn(1)));
        Projector projector = visitor.process(projection, RAM_ACCOUNTING_CONTEXT);
        projector.registerUpstream(null);
        assertThat(projector, instanceOf(SortingTopNProjector.class));

        projector.startProjection();
        int i;
        for (i = 20; i > 0; i--) {
            if (!projector.setNextRow(spare(i % 4, i))) {
                break;
            }
        }
        projector.upstreamFinished();
        Bucket rows = ((ResultProvider) projector).result().get();
        assertThat(printedTable(rows), is(
                "foo| 0| 4\n" +
                        "foo| 0| 8\n" +
                        "foo| 0| 12\n" +
                        "foo| 0| 16\n" +
                        "foo| 0| 20\n" +
                        "foo| 1| 1\n" +
                        "foo| 1| 5\n" +
                        "foo| 1| 9\n" +
                        "foo| 1| 13\n" +
                        "foo| 1| 17\n"
        ));
    }

    @Test
    public void testAggregationProjector() throws ExecutionException, InterruptedException {
        AggregationProjection projection = new AggregationProjection();
        projection.aggregations(Arrays.asList(
                new Aggregation(avgInfo, Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.ITER, Aggregation.Step.FINAL),
                new Aggregation(countInfo, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER, Aggregation.Step.FINAL)
        ));
        Projector projector = visitor.process(projection, RAM_ACCOUNTING_CONTEXT);
        projector.registerUpstream(new ProjectorUpstream() {
            @Override
            public void downstream(Projector downstream) {

            }
        });
        CollectingProjector collectingProjector = new CollectingProjector();
        ((ProjectorUpstream) projector).downstream(collectingProjector);
        assertThat(projector, instanceOf(AggregationProjector.class));


        projector.startProjection();
        projector.setNextRow(spare("foo", 10));
        projector.setNextRow(spare("bar", 20));
        projector.upstreamFinished();
        Bucket rows = collectingProjector.result().get();
        assertThat(rows.size(), is(1));
        assertThat(rows, contains(isRow(15.0, 2L)));
    }

    @Test
    public void testGroupProjector() throws ExecutionException, InterruptedException {
        //         in(0)  in(1)      in(0),      in(2)
        // select  race, avg(age), count(race), gender  ... group by race, gender
        GroupProjection projection = new GroupProjection();
        projection.keys(Arrays.<Symbol>asList(new InputColumn(0, DataTypes.STRING), new InputColumn(2, DataTypes.STRING)));
        projection.values(Arrays.asList(
                new Aggregation(avgInfo, Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.ITER, Aggregation.Step.FINAL),
                new Aggregation(countInfo, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER, Aggregation.Step.FINAL)
        ));

        Projector projector = visitor.process(projection, RAM_ACCOUNTING_CONTEXT);
        projector.registerUpstream(null);

        // use a topN projection in order to get sorted outputs
        TopNProjection topNProjection = new TopNProjection(10, 0,
                ImmutableList.<Symbol>of(new InputColumn(2, DataTypes.DOUBLE)),
                new boolean[]{false},
                new Boolean[]{null});
        topNProjection.outputs(Arrays.<Symbol>asList(
                new InputColumn(0, DataTypes.STRING), new InputColumn(1, DataTypes.STRING),
                new InputColumn(2, DataTypes.DOUBLE), new InputColumn(3, DataTypes.LONG)));
        SortingTopNProjector topNProjector = (SortingTopNProjector) visitor.process(topNProjection, RAM_ACCOUNTING_CONTEXT);
        ((ProjectorUpstream) projector).downstream(topNProjector);
        topNProjector.startProjection();

        assertThat(projector, instanceOf(GroupingProjector.class));

        projector.startProjection();
        BytesRef human = new BytesRef("human");
        BytesRef vogon = new BytesRef("vogon");
        BytesRef male = new BytesRef("male");
        BytesRef female = new BytesRef("female");
        projector.setNextRow(spare(human, 34, male));
        projector.setNextRow(spare(human, 22, female));
        projector.setNextRow(spare(vogon, 40, male));
        projector.setNextRow(spare(vogon, 48, male));
        projector.setNextRow(spare(human, 34, male));
        projector.upstreamFinished();

        Bucket rows = topNProjector.result().get();
        assertThat(rows, contains(
                isRow(human, female, 22.0, 1L),
                isRow(human, male, 34.0, 2L),
                isRow(vogon, male, 44.0, 2L)
        ));
    }

    @Test
    public void testFilterProjection() throws Exception {
        EqOperator op = (EqOperator) functions.get(
                new FunctionIdent(EqOperator.NAME, ImmutableList.<DataType>of(DataTypes.INTEGER, DataTypes.INTEGER)));
        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Literal.newLiteral(2), new InputColumn(1)));
        FilterProjection projection = new FilterProjection(function);
        projection.outputs(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)));

        CollectingProjector collectingProjector = new CollectingProjector();
        Projector projector = visitor.process(projection, RAM_ACCOUNTING_CONTEXT);
        projector.registerUpstream(null);
        ((ProjectorUpstream) projector).downstream(collectingProjector);
        assertThat(projector, instanceOf(FilterProjector.class));

        projector.startProjection();
        projector.setNextRow(spare("human", 2));
        projector.setNextRow(spare("vogon", 1));

        projector.upstreamFinished();

        Bucket rows = collectingProjector.result().get();
        assertThat(rows.size(), is(1));
    }

}
