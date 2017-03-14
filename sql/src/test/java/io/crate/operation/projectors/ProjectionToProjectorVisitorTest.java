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
import io.crate.data.*;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.*;
import io.crate.operation.InputFactory;
import io.crate.operation.aggregation.impl.AverageAggregation;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.projection.*;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingBatchConsumer;
import io.crate.testing.TestingBatchIterators;
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

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class ProjectionToProjectorVisitorTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
    private ProjectionToProjectorVisitor visitor;
    private FunctionInfo countInfo;
    private FunctionInfo avgInfo;
    private Functions functions;

    private ThreadPool threadPool;

    @Before
    public void prepare() {
        MockitoAnnotations.initMocks(this);
        functions = getFunctions();
        threadPool = new ThreadPool("testing");
        visitor = new ProjectionToProjectorVisitor(
            mock(ClusterService.class),
            functions,
            new IndexNameExpressionResolver(Settings.EMPTY),
            threadPool,
            Settings.EMPTY,
            mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
            mock(BulkRetryCoordinatorPool.class),
            new InputFactory(functions),
            EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.COPY),
            null
        );

        countInfo = new FunctionInfo(
            new FunctionIdent(CountAggregation.NAME, Collections.singletonList(DataTypes.STRING)),
            DataTypes.LONG);
        avgInfo = new FunctionInfo(
            new FunctionIdent(AverageAggregation.NAME, Collections.singletonList(DataTypes.INTEGER)),
            DataTypes.DOUBLE);
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testSimpleTopNProjection() throws Exception {
        List<Symbol> outputs = Arrays.asList(Literal.of("foo"), new InputColumn(0));
        TopNProjection projection = new TopNProjection(10, 2, outputs);

        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        assertThat(projector, instanceOf(SimpleTopNProjector.class));

        TestingBatchConsumer consumer = new TestingBatchConsumer();
        consumer.accept(projector.apply(TestingBatchIterators.range(0, 20)), null);

        List<Object[]> result = consumer.getResult();
        assertThat(result.size(), is(10));
        assertThat(result.get(0), is(new Object[] { new BytesRef("foo"), 2 }));
    }

    @Test
    public void testSortingTopNProjection() throws Exception {
        List<Symbol> outputs = Arrays.asList(Literal.of("foo"), new InputColumn(0), new InputColumn(1));
        OrderedTopNProjection projection = new OrderedTopNProjection(10, 0, outputs,
            Arrays.asList(new InputColumn(0), new InputColumn(1)),
            new boolean[]{false, false},
            new Boolean[]{null, null}
        );
        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        assertThat(projector, instanceOf(SortingTopNProjector.class));
    }

    @Test
    public void testTopNProjectionToSortingProjector() throws Exception {
        List<Symbol> outputs = Arrays.asList(Literal.of("foo"), new InputColumn(0), new InputColumn(1));
        OrderedTopNProjection projection = new OrderedTopNProjection(TopN.NO_LIMIT, TopN.NO_OFFSET, outputs,
            Arrays.asList(new InputColumn(0), new InputColumn(1)),
            new boolean[]{false, false},
            new Boolean[]{null, null}
        );
        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        assertThat(projector, instanceOf(SortingProjector.class));
    }

    @Test
    public void testAggregationProjector() throws Exception {
        AggregationProjection projection = new AggregationProjection(Arrays.asList(
            Aggregation.finalAggregation(
                avgInfo,
                Collections.singletonList(new InputColumn(1)),
                Aggregation.Step.ITER),
            Aggregation.finalAggregation(
                countInfo,
                Collections.singletonList(new InputColumn(0)),
                Aggregation.Step.ITER)
        ), RowGranularity.SHARD);
        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());

        assertThat(projector, instanceOf(AggregationPipe.class));


        BatchIterator batchIterator = projector.apply(RowsBatchIterator.newInstance(
            new CollectionBucket(Arrays.asList(
                $("foo", 10),
                $("bar", 20)
            )), 2
        ));
        TestingBatchConsumer consumer = new TestingBatchConsumer();
        consumer.accept(batchIterator, null);
        Bucket rows = consumer.getBucket();
        assertThat(rows.size(), is(1));
        assertThat(rows, contains(isRow(15.0, 2L)));
    }

    @Test
    public void testGroupProjector() throws Exception {
        //         in(0)  in(1)      in(0),      in(2)
        // select  race, avg(age), count(race), gender  ... group by race, gender
        List<Symbol> keys = Arrays.asList(new InputColumn(0, DataTypes.STRING), new InputColumn(2, DataTypes.STRING));
        List<Aggregation> aggregations = Arrays.asList(
            Aggregation.finalAggregation(
                avgInfo,
                Collections.singletonList(new InputColumn(1)),
                Aggregation.Step.ITER),
            Aggregation.finalAggregation(
                countInfo,
                Collections.singletonList(new InputColumn(0)),
                Aggregation.Step.ITER)
        );
        GroupProjection projection = new GroupProjection(keys, aggregations, RowGranularity.CLUSTER);

        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        assertThat(projector, instanceOf(GroupingProjector.class));

        // use a topN projection in order to get sorted outputs
        List<Symbol> outputs = Arrays.asList(
            new InputColumn(0, DataTypes.STRING), new InputColumn(1, DataTypes.STRING),
            new InputColumn(2, DataTypes.DOUBLE), new InputColumn(3, DataTypes.LONG));
        OrderedTopNProjection topNProjection = new OrderedTopNProjection(10, 0, outputs,
            ImmutableList.of(new InputColumn(2, DataTypes.DOUBLE)),
            new boolean[]{false},
            new Boolean[]{null});
        Projector topNProjector = visitor.create(topNProjection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());

        BytesRef human = new BytesRef("human");
        BytesRef vogon = new BytesRef("vogon");
        BytesRef male = new BytesRef("male");
        BytesRef female = new BytesRef("female");

        List<Object[]> rows = new ArrayList<>();
        rows.add($(human, 34, male));
        rows.add($(human, 22, female));
        rows.add($(vogon, 40, male));
        rows.add($(vogon, 48, male));
        rows.add($(human, 34, male));

        BatchIterator batchIterator = topNProjector.apply(projector.apply(
            RowsBatchIterator.newInstance(new CollectionBucket(rows), 3)));
        TestingBatchConsumer consumer = new TestingBatchConsumer();

        consumer.accept(batchIterator, null);

        Bucket bucket = consumer.getBucket();
        assertThat(bucket, contains(
            isRow(human, female, 22.0, 1L),
            isRow(human, male, 34.0, 2L),
            isRow(vogon, male, 44.0, 2L)
        ));
    }

    @Test
    public void testFilterProjection() throws Exception {
        EqOperator op = (EqOperator) functions.get(
            new FunctionIdent(EqOperator.NAME, ImmutableList.of(DataTypes.INTEGER, DataTypes.INTEGER)));
        Function function = new Function(
            op.info(), Arrays.asList(Literal.of(2), new InputColumn(1)));
        FilterProjection projection = new FilterProjection(function,
            Arrays.asList(new InputColumn(0), new InputColumn(1)));

        Projector projector = visitor.create(projection, RAM_ACCOUNTING_CONTEXT, UUID.randomUUID());
        assertThat(projector, instanceOf(FilterProjector.class));

        List<Object[]> rows = new ArrayList<>();
        rows.add($("human", 2));
        rows.add($("vogon", 1));

        BatchIterator filteredBI = projector.apply(RowsBatchIterator.newInstance(new CollectionBucket(rows), 2));
        TestingBatchConsumer consumer = new TestingBatchConsumer();
        consumer.accept(filteredBI, null);
        Bucket bucket = consumer.getBucket();
        assertThat(bucket.size(), is(1));
    }
}
