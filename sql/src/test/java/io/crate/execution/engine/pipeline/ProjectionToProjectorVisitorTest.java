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

package io.crate.execution.engine.pipeline;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.RamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.OrderedTopNProjection;
import io.crate.execution.dsl.projection.TopNProjection;
import io.crate.execution.engine.aggregation.AggregationPipe;
import io.crate.execution.engine.aggregation.GroupingProjector;
import io.crate.execution.engine.aggregation.impl.AverageAggregation;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.sort.SortingProjector;
import io.crate.execution.engine.sort.SortingTopNProjector;
import io.crate.execution.jobs.NodeJobsCounter;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class ProjectionToProjectorVisitorTest extends CrateDummyClusterServiceUnitTest {

    private ProjectionToProjectorVisitor visitor;
    private FunctionInfo countInfo;
    private FunctionInfo avgInfo;
    private Functions functions;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    private OnHeapMemoryManager memoryManager;

    @Before
    public void prepare() {
        MockitoAnnotations.initMocks(this);
        functions = getFunctions();
        visitor = new ProjectionToProjectorVisitor(
            clusterService,
            new NodeJobsCounter(),
            functions,
            THREAD_POOL,
            Settings.EMPTY,
            mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS),
            new InputFactory(functions),
            EvaluatingNormalizer.functionOnlyNormalizer(functions),
            t -> null,
            t -> null
        );
        memoryManager = new OnHeapMemoryManager(usedBytes -> {});

        countInfo = new FunctionInfo(
            new FunctionIdent(CountAggregation.NAME, Collections.singletonList(DataTypes.STRING)),
            DataTypes.LONG);
        avgInfo = new FunctionInfo(
            new FunctionIdent(AverageAggregation.NAME, Collections.singletonList(DataTypes.INTEGER)),
            DataTypes.DOUBLE);
    }

    @Test
    public void testSimpleTopNProjection() throws Exception {
        TopNProjection projection = new TopNProjection(10, 2, Collections.singletonList(DataTypes.LONG));

        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector, instanceOf(SimpleTopNProjector.class));

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(projector.apply(TestingBatchIterators.range(0, 20)), null);

        List<Object[]> result = consumer.getResult();
        assertThat(result.size(), is(10));
        assertThat(result.get(0), is(new Object[]{2}));
    }

    @Test
    public void testSortingTopNProjection() throws Exception {
        List<Symbol> outputs = Arrays.asList(Literal.of("foo"), new InputColumn(0), new InputColumn(1));
        OrderedTopNProjection projection = new OrderedTopNProjection(10, 0, outputs,
            Arrays.asList(new InputColumn(0), new InputColumn(1)),
            new boolean[]{false, false},
            new boolean[]{false, false}
        );
        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector, instanceOf(SortingTopNProjector.class));
    }

    @Test
    public void testTopNProjectionToSortingProjector() throws Exception {
        List<Symbol> outputs = Arrays.asList(Literal.of("foo"), new InputColumn(0), new InputColumn(1));
        OrderedTopNProjection projection = new OrderedTopNProjection(TopN.NO_LIMIT, TopN.NO_OFFSET, outputs,
            Arrays.asList(new InputColumn(0), new InputColumn(1)),
            new boolean[]{false, false},
            new boolean[]{false, false}
        );
        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector, instanceOf(SortingProjector.class));
    }

    @Test
    public void testAggregationProjector() throws Exception {
        AggregationProjection projection = new AggregationProjection(Arrays.asList(
            new Aggregation(
                avgInfo,
                avgInfo.returnType(),
                Collections.singletonList(new InputColumn(1))),
            new Aggregation(
                countInfo,
                countInfo.returnType(),
                Collections.singletonList(new InputColumn(0)))
        ), RowGranularity.SHARD, AggregateMode.ITER_FINAL);
        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());

        assertThat(projector, instanceOf(AggregationPipe.class));


        BatchIterator<Row> batchIterator = projector.apply(InMemoryBatchIterator.of(
            new CollectionBucket(Arrays.asList(
                $("foo", 10),
                $("bar", 20)
            )),
            SENTINEL,
            true));
        TestingRowConsumer consumer = new TestingRowConsumer();
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
            new Aggregation(
                avgInfo,
                avgInfo.returnType(),
                Collections.singletonList(new InputColumn(1))),
            new Aggregation(
                countInfo,
                countInfo.returnType(),
                Collections.singletonList(new InputColumn(0)))
        );
        GroupProjection projection = new GroupProjection(
            keys, aggregations, AggregateMode.ITER_FINAL, RowGranularity.CLUSTER);

        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector, instanceOf(GroupingProjector.class));

        // use a topN projection in order to get sorted outputs
        List<Symbol> outputs = Arrays.asList(
            new InputColumn(0, DataTypes.STRING), new InputColumn(1, DataTypes.STRING),
            new InputColumn(2, DataTypes.DOUBLE), new InputColumn(3, DataTypes.LONG));
        OrderedTopNProjection topNProjection = new OrderedTopNProjection(10, 0, outputs,
            ImmutableList.of(new InputColumn(2, DataTypes.DOUBLE)),
            new boolean[]{false},
            new boolean[]{false});
        Projector topNProjector = visitor.create(
            topNProjection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());

        String human = "human";
        String vogon = "vogon";
        String male = "male";
        String female = "female";

        List<Object[]> rows = new ArrayList<>();
        rows.add($(human, 34, male));
        rows.add($(human, 22, female));
        rows.add($(vogon, 40, male));
        rows.add($(vogon, 48, male));
        rows.add($(human, 34, male));

        BatchIterator<Row> batchIterator = topNProjector.apply(projector.apply(
            InMemoryBatchIterator.of(new CollectionBucket(rows), SENTINEL, true)));
        TestingRowConsumer consumer = new TestingRowConsumer();

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
        List<Symbol> arguments = Arrays.asList(Literal.of(2), new InputColumn(1));
        EqOperator op =
            (EqOperator) functions.get(null, EqOperator.NAME, arguments, SearchPath.pathWithPGCatalogAndDoc());
        Function function = new Function(
            op.info(), arguments);
        FilterProjection projection = new FilterProjection(function,
            Arrays.asList(new InputColumn(0), new InputColumn(1)));

        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector, instanceOf(FilterProjector.class));

        List<Object[]> rows = new ArrayList<>();
        rows.add($("human", 2));
        rows.add($("vogon", 1));

        BatchIterator<Row> filteredBI = projector.apply(
            InMemoryBatchIterator.of(new CollectionBucket(rows), SENTINEL, true));
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(filteredBI, null);
        Bucket bucket = consumer.getBucket();
        assertThat(bucket.size(), is(1));
    }
}
