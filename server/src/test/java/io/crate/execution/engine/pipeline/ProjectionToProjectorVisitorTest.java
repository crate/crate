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

package io.crate.execution.engine.pipeline;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_LIMIT;
import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_OFFSET;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.OrderedLimitAndOffsetProjection;
import io.crate.execution.engine.aggregation.AggregationPipe;
import io.crate.execution.engine.aggregation.GroupingProjector;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.sort.SortingLimitAndOffsetProjector;
import io.crate.execution.engine.sort.SortingProjector;
import io.crate.execution.jobs.NodeLimits;
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
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;

public class ProjectionToProjectorVisitorTest extends CrateDummyClusterServiceUnitTest {

    private ProjectionToProjectorVisitor visitor;
    private Signature avgSignature;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private NodeContext nodeCtx;

    private OnHeapMemoryManager memoryManager;

    @Before
    public void prepare() {
        nodeCtx = createNodeContext();
        MockitoAnnotations.initMocks(this);
        visitor = new ProjectionToProjectorVisitor(
            clusterService,
            new NodeLimits(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            new NoneCircuitBreakerService(),
            nodeCtx,
            THREAD_POOL,
            Settings.EMPTY,
            mock(ElasticsearchClient.class),
            new InputFactory(nodeCtx),
            EvaluatingNormalizer.functionOnlyNormalizer(nodeCtx),
            t -> null,
            t -> null
        );
        memoryManager = new OnHeapMemoryManager(usedBytes -> {});

        avgSignature = Signature.aggregate(
            "avg",
            DataTypes.INTEGER.getTypeSignature(),
            DataTypes.DOUBLE.getTypeSignature()
        );
    }

    @Test
    public void testLimitAndOffsetProjection() throws Exception {
        LimitAndOffsetProjection projection = new LimitAndOffsetProjection(10, 2, Collections.singletonList(DataTypes.LONG));

        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector).isExactlyInstanceOf(LimitAndOffsetProjector.class);

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(projector.apply(TestingBatchIterators.range(0, 20)), null);

        List<Object[]> result = consumer.getResult();
        assertThat(result).hasSize(10);
        assertThat(result.get(0)).isEqualTo(new Object[]{2});
    }

    @Test
    public void testSortingLimitAndOffsetProjection() throws Exception {
        List<Symbol> outputs = Arrays.asList(Literal.of("foo"), new InputColumn(0), new InputColumn(1));
        OrderedLimitAndOffsetProjection projection = new OrderedLimitAndOffsetProjection(10, 0, outputs,
                                                                                         Arrays.asList(new InputColumn(0), new InputColumn(1)),
                                                                                         new boolean[]{false, false},
                                                                                         new boolean[]{false, false}
        );
        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector).isExactlyInstanceOf(SortingLimitAndOffsetProjector.class);
    }

    @Test
    public void testNoLimitWithOffsetProjection() throws Exception {
        LimitAndOffsetProjection projection = new LimitAndOffsetProjection(NO_LIMIT, 10, Collections.singletonList(DataTypes.LONG));

        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector).isExactlyInstanceOf(OffsetProjector.class);
    }

    @Test
    public void testNoLimitNorOffsetProjection() throws Exception {
        LimitAndOffsetProjection projection = new LimitAndOffsetProjection(NO_LIMIT, NO_OFFSET, Collections.singletonList(DataTypes.LONG));

        assertThatThrownBy(() -> visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("LimitAndOffsetProjection should have at least one of the limit and offset set");
    }

    @Test
    public void testOrderedLimitAndOffsetProjectionToSortingProjector() throws Exception {
        List<Symbol> outputs = Arrays.asList(Literal.of("foo"), new InputColumn(0), new InputColumn(1));
        OrderedLimitAndOffsetProjection projection =
            new OrderedLimitAndOffsetProjection(NO_LIMIT, LimitAndOffset.NO_OFFSET, outputs,
                                                Arrays.asList(new InputColumn(0), new InputColumn(1)),
                                                new boolean[]{false, false},
                                                new boolean[]{false, false}
        );
        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector).isExactlyInstanceOf(SortingProjector.class);
    }

    @Test
    public void testAggregationProjector() throws Exception {
        AggregationProjection projection = new AggregationProjection(Arrays.asList(
            new Aggregation(
                avgSignature,
                avgSignature.getReturnType().createType(),
                Collections.singletonList(new InputColumn(1))),
            new Aggregation(
                CountAggregation.SIGNATURE,
                CountAggregation.SIGNATURE.getReturnType().createType(),
                Collections.singletonList(new InputColumn(0)))
        ), RowGranularity.SHARD, AggregateMode.ITER_FINAL);
        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());

        assertThat(projector).isExactlyInstanceOf(AggregationPipe.class);


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
        assertThat(rows).hasSize(1);
        assertThat(rows).containsExactly(new RowN(15.0, 2L));
    }

    @Test
    public void testGroupProjector() throws Exception {
        //         in(0)  in(1)      in(0),      in(2)
        // select  race, avg(age), count(race), gender  ... group by race, gender
        List<Symbol> keys = Arrays.asList(new InputColumn(0, DataTypes.STRING), new InputColumn(2, DataTypes.STRING));
        List<Aggregation> aggregations = Arrays.asList(
            new Aggregation(
                avgSignature,
                avgSignature.getReturnType().createType(),
                Collections.singletonList(new InputColumn(1))),
            new Aggregation(
                CountAggregation.SIGNATURE,
                CountAggregation.SIGNATURE.getReturnType().createType(),
                Collections.singletonList(new InputColumn(0)))
        );
        GroupProjection projection = new GroupProjection(
            keys, aggregations, AggregateMode.ITER_FINAL, RowGranularity.CLUSTER);

        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector).isExactlyInstanceOf(GroupingProjector.class);

        // use an OrderedAndLimitAndOffset projection in order to get sorted outputs
        List<Symbol> outputs = Arrays.asList(
            new InputColumn(0, DataTypes.STRING), new InputColumn(1, DataTypes.STRING),
            new InputColumn(2, DataTypes.DOUBLE), new InputColumn(3, DataTypes.LONG));
        OrderedLimitAndOffsetProjection orderedLimitAndOffsetProjection =
            new OrderedLimitAndOffsetProjection(10, 0, outputs,
                                                List.of(new InputColumn(2, DataTypes.DOUBLE)),
                                                new boolean[]{false},
                                                new boolean[]{false});
        Projector limitAndOffsetProjector = visitor.create(
            orderedLimitAndOffsetProjection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());

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

        BatchIterator<Row> batchIterator = limitAndOffsetProjector.apply(projector.apply(
            InMemoryBatchIterator.of(new CollectionBucket(rows), SENTINEL, true)));
        TestingRowConsumer consumer = new TestingRowConsumer();

        consumer.accept(batchIterator, null);

        Bucket bucket = consumer.getBucket();
        List<Object[]> resultRows = StreamSupport.stream(bucket.spliterator(), false)
            .map(Row::materialize)
            .toList();
        assertThat(resultRows).containsExactly(
            new Object[] { human, female, 22.0, 1L },
            new Object[] { human, male, 34.0, 2L },
            new Object[] { vogon, male, 44.0, 2L }
        );
    }

    @Test
    public void testFilterProjection() throws Exception {
        List<Symbol> arguments = Arrays.asList(Literal.of(2), new InputColumn(1));
        EqOperator op =
            (EqOperator) nodeCtx.functions().get(null, EqOperator.NAME, arguments, SearchPath.pathWithPGCatalogAndDoc());
        Function function = new Function(op.signature(), arguments, EqOperator.RETURN_TYPE);
        FilterProjection projection = new FilterProjection(function,
            Arrays.asList(new InputColumn(0), new InputColumn(1)));

        Projector projector = visitor.create(
            projection, txnCtx, RamAccounting.NO_ACCOUNTING, memoryManager, UUID.randomUUID());
        assertThat(projector).isExactlyInstanceOf(FilterProjector.class);

        List<Object[]> rows = new ArrayList<>();
        rows.add($("human", 2));
        rows.add($("vogon", 1));

        BatchIterator<Row> filteredBI = projector.apply(
            InMemoryBatchIterator.of(new CollectionBucket(rows), SENTINEL, true));
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(filteredBI, null);
        Bucket bucket = consumer.getBucket();
        assertThat(bucket).hasSize(1);
    }
}
