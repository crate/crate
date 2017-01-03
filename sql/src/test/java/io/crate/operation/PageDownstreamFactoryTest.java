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

package io.crate.operation;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.operation.aggregation.impl.MinimumAggregation;
import io.crate.operation.projectors.TopN;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.OrderedTopNProjection;
import io.crate.planner.projection.Projection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;

public class PageDownstreamFactoryTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    private GroupProjection groupProjection;
    private Functions functions;
    private ThreadPool threadPool;

    @Before
    @SuppressWarnings("unchecked")
    public void prepare() {
        threadPool = new ThreadPool("testing");
        functions = getFunctions();

        FunctionIdent minAggIdent = new FunctionIdent(MinimumAggregation.NAME, Arrays.asList(DataTypes.DOUBLE));
        FunctionInfo minAggInfo = new FunctionInfo(minAggIdent, DataTypes.DOUBLE);

        List<Symbol> keys = Collections.singletonList(new InputColumn(0, DataTypes.INTEGER));
        List<Aggregation> aggregations = Collections.singletonList(
            Aggregation.finalAggregation(minAggInfo, Collections.singletonList(new InputColumn(1)), Aggregation.Step.PARTIAL));
        groupProjection = new GroupProjection(keys, aggregations, RowGranularity.CLUSTER);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testMergeSingleResult() throws Exception {
        OrderedTopNProjection topNProjection = new OrderedTopNProjection(3, TopN.NO_OFFSET, InputColumn.numInputs(2),
            Arrays.<Symbol>asList(new InputColumn(0)), new boolean[]{false}, new Boolean[]{null});

        MergePhase mergePhase = new MergePhase(
            UUID.randomUUID(),
            0,
            "merge",
            2,
            Collections.emptyList(),
            ImmutableList.<DataType>of(DataTypes.INTEGER, DataTypes.DOUBLE),
            Arrays.asList(groupProjection, topNProjection),
            DistributionInfo.DEFAULT_BROADCAST,
            null);

        Object[][] objs = new Object[20][];
        for (int i = 0; i < objs.length; i++) {
            objs[i] = new Object[]{i % 4, i + 0.5d};
        }
        Bucket rows = new ArrayBucket(objs);
        BucketPage page = new BucketPage(Futures.immediateFuture(rows));
        final PageDownstreamFactory pageDownstreamFactory = new PageDownstreamFactory(
            mock(ClusterService.class),
            new IndexNameExpressionResolver(Settings.EMPTY),
            threadPool,
            Settings.EMPTY,
            mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
            mock(BulkRetryCoordinatorPool.class),
            functions
        );
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final PageDownstream pageDownstream = getPageDownstream(mergePhase, pageDownstreamFactory, rowReceiver);
        final SettableFuture<?> future = SettableFuture.create();
        pageDownstream.nextPage(page, new PageConsumeListener() {
            @Override
            public void needMore() {
                pageDownstream.finish();
                future.set(null);
            }

            @Override
            public void finish() {
                fail("operation should want more");
            }
        });
        future.get();
        Bucket mergeResult = rowReceiver.result();
        assertThat(mergeResult, IsIterableContainingInOrder.contains(
            isRow(0, 0.5d),
            isRow(1, 1.5d),
            isRow(2, 2.5d)
        ));
    }

    private PageDownstream getPageDownstream(MergePhase mergePhase,
                                             PageDownstreamFactory pageDownstreamFactory,
                                             CollectingRowReceiver rowReceiver) {
        return pageDownstreamFactory.createMergeNodePageDownstream(
            mergePhase,
            rowReceiver,
            randomBoolean(),
            RAM_ACCOUNTING_CONTEXT,
            Optional.absent());
    }

    @Test
    public void testMergeMultipleResults() throws Exception {
        MergePhase mergePhase = new MergePhase(
            UUID.randomUUID(),
            0,
            "merge",
            2,
            Collections.emptyList(),
            ImmutableList.<DataType>of(DataTypes.INTEGER, DataTypes.DOUBLE),
            Arrays.<Projection>asList(groupProjection),
            DistributionInfo.DEFAULT_BROADCAST,
            null);
        final PageDownstreamFactory pageDownstreamFactory = new PageDownstreamFactory(
            mock(ClusterService.class),
            new IndexNameExpressionResolver(Settings.EMPTY),
            threadPool,
            Settings.EMPTY,
            mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
            mock(BulkRetryCoordinatorPool.class),
            functions
        );
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final PageDownstream pageDownstream = getPageDownstream(mergePhase, pageDownstreamFactory, rowReceiver);

        Bucket rows = new ArrayBucket(new Object[][]{{0, 100.0d}});
        BucketPage page1 = new BucketPage(Futures.immediateFuture(rows));
        Bucket otherRows = new ArrayBucket(new Object[][]{{0, 2.5d}});
        BucketPage page2 = new BucketPage(Futures.immediateFuture(otherRows));
        final Iterator<BucketPage> iterator = Iterators.forArray(page1, page2);

        final SettableFuture<?> future = SettableFuture.create();
        pageDownstream.nextPage(iterator.next(), new PageConsumeListener() {
            @Override
            public void needMore() {
                if (iterator.hasNext()) {
                    pageDownstream.nextPage(iterator.next(), this);
                } else {
                    pageDownstream.finish();
                    future.set(null);
                }
            }

            @Override
            public void finish() {
                fail("should still want more");
            }
        });
        future.get();
        Bucket mergeResult = rowReceiver.result();
        assertThat(mergeResult, contains(isRow(0, 2.5)));
    }
}
