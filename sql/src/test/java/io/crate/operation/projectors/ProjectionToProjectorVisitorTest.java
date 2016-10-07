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
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.*;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.ArrayRow;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.*;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.aggregation.impl.AverageAggregation;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowSender;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;


public class ProjectionToProjectorVisitorTest extends CrateUnitTest {

    protected static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
    private ProjectionToProjectorVisitor visitor;
    private FunctionInfo countInfo;
    private FunctionInfo avgInfo;
    private Functions functions;

    private final ArrayRow spare = new ArrayRow();
    private ThreadPool threadPool;

    private Row spare(Object... cells) {
        spare.cells(cells);
        return spare;
    }

    @Before
    public void prepare() {
        MockitoAnnotations.initMocks(this);
        NestedReferenceResolver referenceResolver = new GlobalReferenceResolver(new HashMap<ReferenceIdent, ReferenceImplementation>());
        functions = getFunctions();
        threadPool = new ThreadPool("testing");
        ImplementationSymbolVisitor symbolvisitor = new ImplementationSymbolVisitor(functions);
        visitor = new ProjectionToProjectorVisitor(
            mock(ClusterService.class),
            functions,
            new IndexNameExpressionResolver(Settings.EMPTY),
            threadPool,
            Settings.EMPTY,
            mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
            mock(BulkRetryCoordinatorPool.class),
            symbolvisitor,
            new EvaluatingNormalizer(functions, RowGranularity.DOC, referenceResolver, null, ReplaceMode.COPY)
        );

        countInfo = new FunctionInfo(new FunctionIdent(CountAggregation.NAME, Arrays.<DataType>asList(DataTypes.STRING)), DataTypes.LONG);
        avgInfo = new FunctionInfo(new FunctionIdent(AverageAggregation.NAME, Arrays.<DataType>asList(DataTypes.INTEGER)), DataTypes.DOUBLE);
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testSimpleTopNProjection() throws Exception {
        TopNProjection projection = new TopNProjection(10, 2);
        projection.outputs(Arrays.<Symbol>asList(Literal.of("foo"), new InputColumn(0)));

        CollectingRowReceiver collectingProjector = new CollectingRowReceiver();
        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        projector.downstream(collectingProjector);
        assertThat(projector, instanceOf(SimpleTopNProjector.class));


        long lastEmittedValue = RowSender.generateRowsInRangeAndEmit(0, 20, projector);
        assertThat(lastEmittedValue, is(11L));
        Bucket rows = collectingProjector.result();
        assertThat(rows.size(), is(10));
        assertThat(rows.iterator().next(), isRow("foo", 2L));
    }

    @Test
    public void testSortingTopNProjection() throws Exception {
        TopNProjection projection = new TopNProjection(10, 0,
            Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)),
            new boolean[]{false, false},
            new Boolean[]{null, null}
        );
        projection.outputs(Arrays.<Symbol>asList(Literal.of("foo"), new InputColumn(0), new InputColumn(1)));
        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        assertThat(projector, instanceOf(SortingTopNProjector.class));
    }

    @Test
    public void testTopNProjectionToSortingProjector() throws Exception {
        TopNProjection projection = new TopNProjection(TopN.NO_LIMIT, TopN.NO_OFFSET,
            Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)),
            new boolean[]{false, false},
            new Boolean[]{null, null}
        );
        projection.outputs(Arrays.<Symbol>asList(Literal.of("foo"), new InputColumn(0), new InputColumn(1)));
        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        assertThat(projector, instanceOf(SortingProjector.class));
    }

    @Test
    public void testAggregationProjector() throws Exception {
        AggregationProjection projection = new AggregationProjection(Arrays.asList(
            Aggregation.finalAggregation(avgInfo, Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.ITER),
            Aggregation.finalAggregation(countInfo, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER)
        ), RowGranularity.SHARD);
        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());

        CollectingRowReceiver collectingProjector = new CollectingRowReceiver();
        projector.downstream(collectingProjector);
        assertThat(projector, instanceOf(AggregationPipe.class));


        projector.prepare();
        projector.setNextRow(spare("foo", 10));
        projector.setNextRow(spare("bar", 20));
        projector.finish(RepeatHandle.UNSUPPORTED);
        Bucket rows = collectingProjector.result();
        assertThat(rows.size(), is(1));
        assertThat(rows, contains(isRow(15.0, 2L)));
    }

    @Test
    public void testGroupProjector() throws Exception {
        //         in(0)  in(1)      in(0),      in(2)
        // select  race, avg(age), count(race), gender  ... group by race, gender
        GroupProjection projection = new GroupProjection();
        projection.keys(Arrays.<Symbol>asList(new InputColumn(0, DataTypes.STRING), new InputColumn(2, DataTypes.STRING)));
        projection.values(Arrays.asList(
            Aggregation.finalAggregation(avgInfo, Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.ITER),
            Aggregation.finalAggregation(countInfo, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER)
        ));

        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());

        // use a topN projection in order to get sorted outputs
        TopNProjection topNProjection = new TopNProjection(10, 0,
            ImmutableList.<Symbol>of(new InputColumn(2, DataTypes.DOUBLE)),
            new boolean[]{false},
            new Boolean[]{null});
        topNProjection.outputs(Arrays.<Symbol>asList(
            new InputColumn(0, DataTypes.STRING), new InputColumn(1, DataTypes.STRING),
            new InputColumn(2, DataTypes.DOUBLE), new InputColumn(3, DataTypes.LONG)));
        Projector topNProjector = visitor.create(topNProjection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        projector.downstream(topNProjector);

        CollectingRowReceiver collector = new CollectingRowReceiver();
        topNProjector.downstream(collector);

        collector.prepare();
        topNProjector.prepare();
        projector.prepare();

        assertThat(projector, instanceOf(GroupingProjector.class));

        BytesRef human = new BytesRef("human");
        BytesRef vogon = new BytesRef("vogon");
        BytesRef male = new BytesRef("male");
        BytesRef female = new BytesRef("female");
        projector.setNextRow(spare(human, 34, male));
        projector.setNextRow(spare(human, 22, female));
        projector.setNextRow(spare(vogon, 40, male));
        projector.setNextRow(spare(vogon, 48, male));
        projector.setNextRow(spare(human, 34, male));
        projector.finish(RepeatHandle.UNSUPPORTED);

        Bucket rows = collector.result();
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
            op.info(), Arrays.<Symbol>asList(Literal.of(2), new InputColumn(1)));
        FilterProjection projection = new FilterProjection(function);
        projection.outputs(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)));

        CollectingRowReceiver collectingProjector = new CollectingRowReceiver();
        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        projector.downstream(collectingProjector);
        assertThat(projector, instanceOf(FilterProjector.class));

        projector.prepare();
        projector.setNextRow(spare("human", 2));
        projector.setNextRow(spare("vogon", 1));

        projector.finish(RepeatHandle.UNSUPPORTED);

        Bucket rows = collectingProjector.result();
        assertThat(rows.size(), is(1));
    }
}
