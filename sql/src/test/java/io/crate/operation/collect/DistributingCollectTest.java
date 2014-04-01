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

import com.google.common.collect.ImmutableSet;
import io.crate.Constants;
import io.crate.action.SQLXContentQueryParser;
import io.crate.analyze.WhereClause;
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
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistributingCollectTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private IndexService indexService = mock(IndexService.class);
    private DistributingCollectOperation operation;

    private final UUID jobId = UUID.randomUUID();
    private final ThreadPool testThreadPool = new ThreadPool();
    private final static String TEST_NODE_ID = "dcollect_node";
    private final static String OTHER_NODE_ID = "other_node";
    private final static String TEST_TABLE_NAME = "dcollect_table";

    private final Map<String, Object[][]> buckets = new HashMap<>();
    private Reference testShardIdReference = new Reference(SysShardsTableInfo.INFOS.get(new ColumnIdent("id")));

    class TestModule extends AbstractModule {
        @Override
        protected void configure() {
            MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
            bind(Functions.class).asEagerSingleton();
            bind(ThreadPool.class).toInstance(testThreadPool);

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
            when(nodes.iterator()).thenReturn(nodeMap.valuesIt());
            when(state.nodes()).thenReturn(nodes);
            when(clusterService.state()).thenReturn(state);

            bind(SQLXContentQueryParser.class).toInstance(mock(SQLXContentQueryParser.class));

            IndicesService indicesService = mock(IndicesService.class);
            bind(IndicesService.class).toInstance(indicesService);
            when(indicesService.indexServiceSafe(TEST_TABLE_NAME)).thenReturn(indexService);

            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            MapBinder.newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
            bind(ReferenceResolver.class).to(GlobalReferenceResolver.class);

            bind(IndexService.class).toInstance(indexService);

            TransportService transportService = mock(TransportService.class);
            Mockito.doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    // gather buckets to verify calls
                    DistributingCollectTest.this.buckets.put(
                            ((DiscoveryNode) args[0]).id(),
                            ((DistributedResultRequest) args[2]).rows()
                    );
                    return null;
                }
            }).when(transportService).submitRequest(any(DiscoveryNode.class), Matchers.same(TransportMergeNodeAction.mergeRowsAction),
                    Matchers.<TransportRequest>any(),
                    any(TransportResponseHandler.class));
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
            Index index = new Index(TEST_TABLE_NAME);
            bind(Index.class).toInstance(index);
            bind(ShardId.class).toInstance(shardId);
            MapBinder<ReferenceIdent, ShardReferenceImplementation> binder = MapBinder
                    .newMapBinder(binder(), ReferenceIdent.class, ShardReferenceImplementation.class);
            binder.addBinding(this.shardIdExpression.info().ident()).toInstance(this.shardIdExpression);
            bind(ShardReferenceResolver.class).asEagerSingleton();
            bind(ScriptService.class).toInstance(mock(ScriptService.class));
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
                .add(new OperatorModule())
                .add(new TestModule())
                .createInjector();
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

        operation = injector.getInstance(DistributingCollectOperation.class);
    }

    private final Routing nodeRouting = new Routing(new HashMap<String, Map<String, Set<Integer>>>(1){{
        put(TEST_NODE_ID, new HashMap<String, Set<Integer>>());
    }});

    private Routing shardRouting(final Integer ... shardIds) {
        return new Routing(new HashMap<String, Map<String, Set<Integer>>>(){{
            put(TEST_NODE_ID, new HashMap<String, Set<Integer>>(){{
                put(TEST_TABLE_NAME, ImmutableSet.copyOf(shardIds));
            }});
            put(OTHER_NODE_ID, new HashMap<String, Set<Integer>>(){{
                put(TEST_TABLE_NAME, ImmutableSet.copyOf(shardIds));
            }});
        }});
    }

    @Test
    public void testCollectFromShardsToBuckets() throws Exception {
        CollectNode collectNode = new CollectNode("dcollect", shardRouting(0, 1));
        collectNode.downStreamNodes(Arrays.asList(TEST_NODE_ID, OTHER_NODE_ID));
        collectNode.jobId(jobId);
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference));

        assertThat(operation.collect(collectNode).get(), is(Constants.EMPTY_RESULT));
        Thread.sleep(20); // give the mocked transport time to operate
        assertThat(buckets.size(), is(2));
        assertTrue(buckets.containsKey(TEST_NODE_ID));
        assertTrue(buckets.containsKey(OTHER_NODE_ID));
    }

    @Test( expected = UnsupportedOperationException.class )
    public void testCollectFromNodes() throws Exception {
        CollectNode collectNode = new CollectNode("dcollect", nodeRouting);
        collectNode.downStreamNodes(Arrays.asList(TEST_NODE_ID, OTHER_NODE_ID));
        collectNode.jobId(jobId);
        collectNode.maxRowGranularity(RowGranularity.NODE);
        collectNode.toCollect(Arrays.<Symbol>asList(new BooleanLiteral(true)));
        operation.collect(collectNode).get();
    }

    @Test
    public void testCollectWithFalseWhereClause() throws Exception {
        CollectNode collectNode = new CollectNode("collect all the things", shardRouting(0, 1));
        collectNode.downStreamNodes(Arrays.asList(TEST_NODE_ID, OTHER_NODE_ID));
        collectNode.jobId(jobId);
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference));

        collectNode.whereClause(new WhereClause(new Function(
                AndOperator.INFO,
                Arrays.<Symbol>asList(new BooleanLiteral(false), new BooleanLiteral(false))
        )));

        Object[][] pseudoResult = operation.collect(collectNode).get();
        assertThat(pseudoResult, is(Constants.EMPTY_RESULT));
        Thread.sleep(20);
        assertThat(buckets.size(), is(2));
        assertThat(buckets.get(TEST_NODE_ID), is(Constants.EMPTY_RESULT));
        assertThat(buckets.get(OTHER_NODE_ID), is(Constants.EMPTY_RESULT));
    }
}
