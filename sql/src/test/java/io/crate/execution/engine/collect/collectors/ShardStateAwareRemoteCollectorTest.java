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

package io.crate.execution.engine.collect.collectors;

import io.crate.data.CollectingRowConsumer;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CrateCollector;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.TestingBatchIterators;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardStateAwareRemoteCollectorTest extends CrateDummyClusterServiceUnitTest {

    private CollectingRowConsumer<?, List<Object[]>> consumer;
    private CrateCollector localCrateCollector;
    private RemoteCollector remoteCrateCollector;
    private Index index;
    private ShardId shardId;
    private ExecutorService executor;
    private ShardStateAwareRemoteCollector shardAwareRemoteCollector;

    @Before
    public void prepare() throws Exception {
        executor = Executors.newFixedThreadPool(1);

        String indexUUID = UUID.randomUUID().toString();
        final String indexName = "t";
        index = new Index(indexName, indexUUID);
        shardId = new ShardId(indexName, indexUUID, 0);

        consumer = new CollectingRowConsumer<>(Collectors.mapping(Row::materialize, Collectors.toList()));
        localCrateCollector = mock(CrateCollector.class);
        // this is a bit crafty but we can use the consumer's future to wait for the execution to complete
        // this way we can be sure the cluster changes were picked up by the listener and the collectors triggered
        doAnswer(invocation -> {
            consumer.accept(TestingBatchIterators.range(0, 1), null);
            return null;
        }).when(localCrateCollector).doCollect();
        remoteCrateCollector = mock(RemoteCollector.class);

        doAnswer(invocation -> {
            consumer.accept(TestingBatchIterators.range(0, 1), null);
            return null;
        }).when(remoteCrateCollector).doCollect();

        IndexShard indexShard = mock(IndexShard.class);
        IndexService indexService = mock(IndexService.class);
        when(indexService.getShard(0)).thenReturn(indexShard);
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(any(Index.class))).thenReturn(indexService);

        shardAwareRemoteCollector = new ShardStateAwareRemoteCollector(
            new ShardId(indexName, indexUUID, 0),
            consumer,
            clusterService,
            indicesService,
            shard -> localCrateCollector,
            remotenode -> remoteCrateCollector,
            executor,
            new ThreadContext(Settings.EMPTY)
        );

        // add 2 nodes and the table to the cluster state
        SQLExecutor.builder(clusterService, 2, Randomness.get())
            .addTable("create table t (id long primary key)")
            .build();
    }

    @After
    public void tearDownExecutor() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testIsRemoteCollectorIfRemoteNodeIsNotLocalNode() throws InterruptedException, ExecutionException, TimeoutException {
        setNewClusterStateFor(createStartedShardRouting("n2"));
        shardAwareRemoteCollector.doCollect();

        consumer.resultFuture().get(10, TimeUnit.SECONDS);
        verify(remoteCrateCollector, times(1)).doCollect();
    }

    @Test
    public void testIsLocalCollectorIfRemoteNodeEqualsLocalNodeAndShardStarted() throws InterruptedException, ExecutionException, TimeoutException {
        setNewClusterStateFor(createStartedShardRouting("n1"));
        shardAwareRemoteCollector.doCollect();
        consumer.resultFuture().get(10, TimeUnit.SECONDS);

        verify(localCrateCollector, times(1)).doCollect();
    }

    @Test
    public void testCollectorWaitsForShardToRelocateBeforeRemoteCollect() throws InterruptedException, ExecutionException, TimeoutException {
        ShardRouting relocatingShardRouting = createRelocatingShardRouting("n1", "n2");
        setNewClusterStateFor(relocatingShardRouting);
        shardAwareRemoteCollector.doCollect();
        setNewClusterStateFor(createStartedShardRouting("n2"));

        consumer.resultFuture().get(10, TimeUnit.SECONDS);
        verify(remoteCrateCollector, times(1)).doCollect();
    }

    private void setNewClusterStateFor(ShardRouting shardRouting) {
        ClusterState newState = ClusterState.builder(clusterService.state()).routingTable(
            RoutingTable.builder().add(
                IndexRoutingTable.builder(index)
                    .addShard(shardRouting)
                    .build()
            ).build())
            .build();
        ClusterServiceUtils.setState(clusterService, newState);
    }

    private ShardRouting createRelocatingShardRouting(String originalNodeId, String targetNodeId) {
        ShardRouting startedRouting = createStartedShardRouting(originalNodeId);
        return startedRouting.relocate(targetNodeId, 1L);
    }

    private ShardRouting createStartedShardRouting(String nodeId) {
        return ShardRouting.newUnassigned(shardId,
            true,
            PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        ).initialize(nodeId, null, 1L)
            .moveToStarted();
    }
}
