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
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.*;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.MinimumAggregation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.testing.CollectingProjector;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;

public class PageDownstreamFactoryTest extends CrateUnitTest {

    private static final RamAccountingContext ramAccountingContext =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    private GroupProjection groupProjection;
    private Functions functions;
    private ReferenceResolver referenceResolver;
    private ThreadPool threadPool;

    @Before
    @SuppressWarnings("unchecked")
    public void prepare() {
        Injector injector = new ModulesBuilder()
                .add(new AggregationImplModule())
                .add(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Client.class).toInstance(mock(Client.class));
                    }
                })
                .createInjector();
        threadPool = new ThreadPool("testing");
        functions = injector.getInstance(Functions.class);
        referenceResolver = new GlobalReferenceResolver(
                Collections.<ReferenceIdent, ReferenceImplementation>emptyMap());

        FunctionIdent minAggIdent = new FunctionIdent(MinimumAggregation.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE));
        FunctionInfo minAggInfo = new FunctionInfo(minAggIdent, DataTypes.DOUBLE);

        groupProjection = new GroupProjection();
        groupProjection.keys(Arrays.<Symbol>asList(new InputColumn(0, DataTypes.INTEGER)));
        groupProjection.values(Arrays.asList(
                new Aggregation(minAggInfo, Arrays.<Symbol>asList(new InputColumn(1)),
                        Aggregation.Step.PARTIAL, Aggregation.Step.FINAL)
        ));
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
        TopNProjection topNProjection = new TopNProjection(3, TopN.NO_OFFSET,
                Arrays.<Symbol>asList(new InputColumn(0)), new boolean[]{false}, new Boolean[]{null});
        topNProjection.outputs(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)));

        MergeNode mergeNode = new MergeNode(0, "merge", 2); // no need for inputTypes here
        mergeNode.projections(Arrays.asList(
                groupProjection,
                topNProjection
        ));

        Object[][] objs = new Object[20][];
        for (int i = 0; i < objs.length; i++) {
            objs[i] = new Object[]{i % 4, i + 0.5d};
        }
        Bucket rows = new ArrayBucket(objs);
        BucketPage page = new BucketPage(Futures.immediateFuture(rows));
        final PageDownstreamFactory pageDownstreamFactory = new PageDownstreamFactory(
                mock(ClusterService.class),
                threadPool,
                ImmutableSettings.EMPTY,
                mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
                mock(BulkRetryCoordinatorPool.class),
                referenceResolver,
                functions
        );
        CollectingProjector collectingProjector = new CollectingProjector();
        final PageDownstream pageDownstream = getPageDownstream(mergeNode, pageDownstreamFactory, collectingProjector);
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
        Bucket mergeResult = collectingProjector.result().get();
        assertThat(mergeResult, IsIterableContainingInOrder.contains(
                isRow(0, 0.5d),
                isRow(1, 1.5d),
                isRow(2, 2.5d)
        ));
    }

    private PageDownstream getPageDownstream(MergeNode mergeNode, PageDownstreamFactory pageDownstreamFactory, CollectingProjector collectingProjector) {
        Tuple<PageDownstream, FlatProjectorChain> downstreamFlatProjectorChainTuple =
                pageDownstreamFactory.createMergeNodePageDownstream(
                        mergeNode, collectingProjector, ramAccountingContext, Optional.<Executor>absent());
        downstreamFlatProjectorChainTuple.v2().startProjections(mock(ExecutionState.class));
        return downstreamFlatProjectorChainTuple.v1();
    }

    @Test
    public void testMergeMultipleResults() throws Exception {
        MergeNode mergeNode = new MergeNode(0, "merge", 2); // no need for inputTypes here
        mergeNode.projections(Arrays.<Projection>asList(
                groupProjection
        ));
        final PageDownstreamFactory pageDownstreamFactory = new PageDownstreamFactory(
                mock(ClusterService.class),
                threadPool,
                ImmutableSettings.EMPTY,
                mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
                mock(BulkRetryCoordinatorPool.class),
                referenceResolver,
                functions
        );
        CollectingProjector collectingProjector = new CollectingProjector();
        final PageDownstream pageDownstream = getPageDownstream(mergeNode, pageDownstreamFactory, collectingProjector);

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
        Bucket mergeResult = collectingProjector.result().get();
        assertThat(mergeResult, contains(isRow(0, 2.5)));
    }

}
