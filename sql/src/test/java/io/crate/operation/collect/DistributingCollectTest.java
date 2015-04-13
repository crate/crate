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

package io.crate.operation.collect;

import com.google.common.collect.Lists;
import io.crate.action.sql.query.TransportQueryShardAction;
import io.crate.analyze.WhereClause;
import io.crate.blob.BlobEnvironment;
import io.crate.blob.v2.BlobIndices;
import io.crate.breaker.CircuitBreakerModule;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.metadata.*;
import io.crate.metadata.shard.ShardReferenceImplementation;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.shard.blob.BlobShardReferenceImplementation;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.reference.sys.shard.ShardIdExpression;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecidersModule;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.InternalSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistributingCollectTest extends CrateUnitTest {

    private IndexService indexService = mock(IndexService.class);
    private DistributingCollectOperation operation;
    private TransportService transportService;

    private final UUID jobId = UUID.randomUUID();
    private final ThreadPool testThreadPool = new ThreadPool(getClass().getSimpleName());
    private final static String TEST_NODE_ID = "dcollect_node";
    private final static String OTHER_NODE_ID = "other_node";
    private final static String TEST_TABLE_NAME = "dcollect_table";

    private Reference testShardIdReference = new Reference(SysShardsTableInfo.INFOS.get(new ColumnIdent("id")));

    class TestModule extends AbstractModule {
        @Override
        protected void configure() {
            MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);

            bind(Functions.class).asEagerSingleton();
            bind(ThreadPool.class).toInstance(testThreadPool);

            bind(CircuitBreakerService.class).toInstance(new NoneCircuitBreakerService());
            bind(ActionFilters.class).toInstance(mock(ActionFilters.class));
            bind(ScriptService.class).toInstance(mock(ScriptService.class));
            bind(SearchService.class).toInstance(mock(InternalSearchService.class));
            bind(AllocationService.class).toInstance(mock(AllocationService.class));
            bind(DynamicSettings.class).annotatedWith(ClusterDynamicSettings.class).toInstance(mock(DynamicSettings.class));
            bind(MetaDataDeleteIndexService.class).toInstance(mock(MetaDataDeleteIndexService.class));
            bind(ClusterInfoService.class).toInstance(mock(ClusterInfoService.class));
            bind(MapperService.class).toInstance(mock(MapperService.class));

            bind(TransportShardBulkAction.class).toInstance(mock(TransportShardBulkAction.class));
            bind(TransportCreateIndexAction.class).toInstance(mock(TransportCreateIndexAction.class));
            bind(TransportQueryShardAction.class).toInstance(mock(TransportQueryShardAction.class));

            DiscoveryNode testNode = mock(DiscoveryNode.class);
            when(testNode.id()).thenReturn(TEST_NODE_ID);

            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.localNode()).thenReturn(testNode);
            bind(ClusterService.class).toInstance(clusterService);

            DiscoveryNode otherNode = mock(DiscoveryNode.class);
            when(otherNode.id()).thenReturn(OTHER_NODE_ID);
            ImmutableOpenMap<String, DiscoveryNode> nodeMap = ImmutableOpenMap.<String, DiscoveryNode>builder()
                    .fPut(TEST_NODE_ID, testNode)
                    .fPut(OTHER_NODE_ID, otherNode)
                    .build();

            ClusterState state = mock(ClusterState.class);
            DiscoveryNodes nodes = mock(DiscoveryNodes.class);
            when(nodes.get(TEST_NODE_ID)).thenReturn(testNode);
            when(nodes.get(OTHER_NODE_ID)).thenReturn(otherNode);
            when(nodes.localNodeId()).thenReturn(TEST_NODE_ID);
            when(nodes.iterator()).thenReturn(nodeMap.valuesIt());
            when(state.nodes()).thenReturn(nodes);
            when(clusterService.state()).thenReturn(state);

            DiscoveryService discoveryService = mock(DiscoveryService.class);
            when(discoveryService.localNode()).thenReturn(testNode);
            bind(DiscoveryService.class).toInstance(discoveryService);

            IndicesService indicesService = mock(IndicesService.class);
            bind(IndicesService.class).toInstance(indicesService);
            when(indicesService.indexServiceSafe(TEST_TABLE_NAME)).thenReturn(indexService);

            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            bind(MetaDataUpdateSettingsService.class).toInstance(mock(MetaDataUpdateSettingsService.class));
            bind(Client.class).toInstance(mock(Client.class));

            Provider<TransportCreateIndexAction> transportCreateIndexActionProvider = mock(Provider.class);
            when(transportCreateIndexActionProvider.get()).thenReturn(mock(TransportCreateIndexAction.class));
            Provider<TransportDeleteIndexAction> transportDeleteActionProvider = mock(Provider.class);
            when(transportDeleteActionProvider.get()).thenReturn(mock(TransportDeleteIndexAction.class));
            Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider = mock(Provider.class);
            when(transportUpdateSettingsActionProvider.get()).thenReturn(mock(TransportUpdateSettingsAction.class));

            BlobIndices blobIndices = new BlobIndices(
                    ImmutableSettings.EMPTY,
                    transportCreateIndexActionProvider,
                    transportDeleteActionProvider,
                    transportUpdateSettingsActionProvider,
                    indicesService,
                    mock(IndicesLifecycle.class),
                    mock(BlobEnvironment.class),
                    clusterService
            );
            bind(BlobIndices.class).toInstance(blobIndices);

            MapBinder.newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
            bind(ReferenceResolver.class).to(GlobalReferenceResolver.class);

            bind(IndexService.class).toInstance(indexService);

            transportService = mock(TransportService.class);
            bind(TransportService.class).toInstance(transportService);
        }
    }

    class TestShardModule extends AbstractModule {

        private final ShardId shardId;
        private final ShardIdExpression shardIdExpression;

        public TestShardModule(int shardId) {
            super();
            this.shardId = new ShardId(TEST_TABLE_NAME, shardId);
            this.shardIdExpression = new ShardIdExpression(this.shardId);
        }

        @Override
        protected void configure() {
            IndexShard shard = mock(InternalIndexShard.class);
            bind(IndexShard.class).toInstance(shard);
            when(shard.shardId()).thenReturn(shardId);
            Index index = new Index(TEST_TABLE_NAME);
            bind(Index.class).toInstance(index);
            bind(ShardId.class).toInstance(shardId);
            MapBinder<ReferenceIdent, ShardReferenceImplementation> binder = MapBinder
                    .newMapBinder(binder(), ReferenceIdent.class, ShardReferenceImplementation.class);
            binder.addBinding(this.shardIdExpression.info().ident()).toInstance(this.shardIdExpression);
            bind(ShardReferenceResolver.class).asEagerSingleton();
            bind(AllocationDecider.class).to(DiskThresholdDecider.class);
            bind(ShardCollectService.class).asEagerSingleton();

            // blob stuff
            MapBinder<ReferenceIdent, BlobShardReferenceImplementation> blobBinder = MapBinder
                    .newMapBinder(binder(), ReferenceIdent.class, BlobShardReferenceImplementation.class);
            bind(Settings.class).annotatedWith(IndexSettings.class).toInstance(ImmutableSettings.EMPTY);

        }
    }

    @Before
    public void prepare() {
        Injector injector = new ModulesBuilder()
                .add(new CircuitBreakerModule())
                .add(new OperatorModule())
                .add(new AllocationDecidersModule(ImmutableSettings.EMPTY))
                .add(new TestModule())
                .createInjector();
        operation = injector.getInstance(DistributingCollectOperation.class);

        Injector shard0Injector = injector.createChildInjector(
                new TestShardModule(0)
        );
        Injector shard1Injector = injector.createChildInjector(
                new TestShardModule(1)
        );
        when(indexService.shardInjectorSafe(0)).thenReturn(shard0Injector);
        when(indexService.shardInjectorSafe(1)).thenReturn(shard1Injector);
        when(indexService.shardSafe(0)).thenReturn(shard0Injector.getInstance(IndexShard.class));
        when(indexService.shardSafe(1)).thenReturn(shard1Injector.getInstance(IndexShard.class));
    }

    @After
    public void cleanUp() throws Exception {
        testThreadPool.shutdownNow();
    }

    private final Routing nodeRouting = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
        .put(TEST_NODE_ID, new TreeMap<String, List<Integer>>())
        .map());

    private Routing shardRouting(final Integer ... shardIds) {
        return new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put(TEST_NODE_ID, TreeMapBuilder.<String, List<Integer>>newMapBuilder()
                    .put(TEST_TABLE_NAME, Arrays.asList(shardIds))
                    .map()
            )
            .put(OTHER_NODE_ID, TreeMapBuilder.<String, List<Integer>>newMapBuilder()
                    .put(TEST_TABLE_NAME, Arrays.asList(shardIds))
                    .map()
            )
            .map()
        );
    }


    @Test
    public void testCollectFromShardsToBuckets() throws Exception {
        final Map<String, Bucket> buckets = new HashMap<>();
        final CountDownLatch countDown = new CountDownLatch(2);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                // gather buckets to verify calls
                buckets.put(
                        ((DiscoveryNode) args[0]).id(),
                        ((DistributedResultRequest) args[2]).rows()
                );
                countDown.countDown();
                return null;
            }
        }).when(transportService).submitRequest(any(DiscoveryNode.class), Matchers.same(TransportMergeNodeAction.mergeRowsAction),
                Matchers.<TransportRequest>any(),
                any(TransportResponseHandler.class));

        CollectNode collectNode = new CollectNode("dcollect", shardRouting(0, 1));
        collectNode.downStreamNodes(Arrays.asList(TEST_NODE_ID, OTHER_NODE_ID));
        collectNode.jobId(jobId);
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference));


        assertThat(operation.collect(collectNode, null).get(), emptyIterable());
        countDown.await();
        assertThat(buckets.size(), is(2));
        assertTrue(buckets.containsKey(TEST_NODE_ID));
        assertTrue(buckets.containsKey(OTHER_NODE_ID));
    }

    @Test
    public void testCollectFromNodes() throws Exception {

        ArgumentCaptor<DistributedResultRequest> capture = ArgumentCaptor.forClass(DistributedResultRequest.class);
        Mockito.doNothing().when(transportService).sendRequest(
                any(DiscoveryNode.class),
                Matchers.same(TransportMergeNodeAction.mergeRowsAction),
                capture.capture(),
                //Matchers.<DistributedResultRequest>any(),
                any(TransportResponseHandler.class));
        CollectNode collectNode = new CollectNode("dcollect", nodeRouting);
        collectNode.downStreamNodes(Arrays.asList(TEST_NODE_ID, OTHER_NODE_ID));
        collectNode.jobId(jobId);
        collectNode.maxRowGranularity(RowGranularity.NODE);
        collectNode.toCollect(Arrays.<Symbol>asList(Literal.newLiteral(true)));
        PlanNodeBuilder.setOutputTypes(collectNode);
        Bucket pseudoResult = operation.collect(collectNode, null).get();
        // empty rows, since this projector sends data and does not return it
        assertThat(pseudoResult, emptyIterable());
        assertThat(Lists.transform(capture.getAllValues(), new com.google.common.base.Function<DistributedResultRequest, Bucket>() {
            @Nullable
            @Override
            public Bucket apply(DistributedResultRequest input) {
                return input.rows();
            }
        }), containsInAnyOrder(emptyIterable(), contains(isRow(true))));
    }

    @Test
    public void testCollectWithFalseWhereClause() throws Exception {
        ArgumentCaptor<DistributedResultRequest> capture = ArgumentCaptor.forClass(DistributedResultRequest.class);
        Mockito.doReturn(null).when(transportService).submitRequest(
                any(DiscoveryNode.class),
                Matchers.same(TransportMergeNodeAction.mergeRowsAction),
                capture.capture(),
                any(TransportResponseHandler.class));

        CollectNode collectNode = new CollectNode("collect all the things", shardRouting(0, 1));
        collectNode.downStreamNodes(Arrays.asList(TEST_NODE_ID, OTHER_NODE_ID));
        collectNode.jobId(jobId);
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference));

        collectNode.whereClause(new WhereClause(new Function(
                AndOperator.INFO,
                Arrays.<Symbol>asList(Literal.newLiteral(false), Literal.newLiteral(false))
        )));
        PlanNodeBuilder.setOutputTypes(collectNode);
        operation.collect(collectNode, null).get();

        assertThat(Lists.transform(capture.getAllValues(), new com.google.common.base.Function<DistributedResultRequest, Bucket>() {
            @Nullable
            @Override
            public Bucket apply(DistributedResultRequest input) {
                return input.rows();
            }
        }), containsInAnyOrder(emptyIterable(), emptyIterable()));
    }
}
