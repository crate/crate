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
package org.elasticsearch.action.admin.indices.close;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.crate.execution.ddl.tables.TransportCloseTable.INDEX_CLOSED_BLOCK_ID;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportVerifyShardBeforeCloseActionTests extends ESTestCase {

    private static ThreadPool threadPool;

    private IndexShard indexShard;
    private TransportVerifyShardBeforeCloseAction action;
    private ClusterService clusterService;
    private ClusterBlock clusterBlock;
    private CapturingTransport transport;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(getTestClass().getName());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        indexShard = mock(IndexShard.class);
        when(indexShard.getActiveOperationsCount()).thenReturn(IndexShard.OPERATIONS_BLOCKED);

        ShardId shardId = new ShardId("index", "_na_", randomIntBetween(0, 3));
        when(indexShard.shardId()).thenReturn(shardId);

        clusterService = createClusterService(threadPool);
        clusterBlock = new ClusterBlock(
            INDEX_CLOSED_BLOCK_ID,
            UUIDs.randomBase64UUID(),
            "index preparing to close. Reopen the index to allow " +
            "writes again or retry closing the index to fully close the index.",
            false,
            false,
            false,
            RestStatus.FORBIDDEN,
            EnumSet.of(ClusterBlockLevel.WRITE));

        setState(
            clusterService,
            new ClusterState.Builder(clusterService.state())
                .blocks(
                    ClusterBlocks
                        .builder().blocks(clusterService.state().blocks())
                        .addIndexBlock("index", clusterBlock)
                        .build())
                .build()
        );

        transport = new CapturingTransport();
        TransportService transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            x -> clusterService.localNode(),
            null
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        ShardStateAction shardStateAction = new ShardStateAction(
            clusterService,
            transportService,
            null,
            null);
        action = new TransportVerifyShardBeforeCloseAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            mock(
                IndicesService.class),
            mock(ThreadPool.class),
            shardStateAction
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void executeOnPrimaryOrReplica() throws Throwable {
        executeOnPrimaryOrReplica(false);
    }

    public void executeOnPrimaryOrReplica(boolean phase1) throws Throwable {
        var request = new TransportVerifyShardBeforeCloseAction.ShardRequest(
            indexShard.shardId(),
            phase1,
            clusterBlock
        );
        PlainActionFuture<Void> res = PlainActionFuture.newFuture();
        action.shardOperationOnPrimary(
            request,
            indexShard,
            ActionListener.wrap(
                r -> {
                    assertNotNull(r);
                    res.onResponse(null);
                },
                res::onFailure
            ));
        try {
            res.get();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testShardIsFlushed() throws Throwable {
        final ArgumentCaptor<FlushRequest> flushRequest = ArgumentCaptor.forClass(FlushRequest.class);
        when(indexShard.flush(flushRequest.capture())).thenReturn(new Engine.CommitId(new byte[0]));

        executeOnPrimaryOrReplica();
        verify(indexShard, times(1)).flush(any(FlushRequest.class));
        assertThat(flushRequest.getValue().force(), is(true));
    }

    @Test
    public void testShardIsSynced() throws Throwable {
        executeOnPrimaryOrReplica(true);
        verify(indexShard, times(1)).sync();
    }

    @Test
    public void testOperationFailsWhenNotBlocked() {
        when(indexShard.getActiveOperationsCount()).thenReturn(randomIntBetween(0, 10));

        IllegalStateException exception = expectThrows(IllegalStateException.class, this::executeOnPrimaryOrReplica);
        assertThat(exception.getMessage(),
                   equalTo("Index shard " + indexShard.shardId() + " is not blocking all operations during closing"));
        verify(indexShard, times(0)).flush(any(FlushRequest.class));
    }

    @Test
    public void testOperationFailsWithNoBlock() {
        setState(clusterService, new ClusterState.Builder(new ClusterName("test")).build());

        IllegalStateException exception = expectThrows(IllegalStateException.class, this::executeOnPrimaryOrReplica);
        assertThat(exception.getMessage(),
                   equalTo("Index shard " + indexShard.shardId() + " must be blocked by " + clusterBlock +
                           " before closing"));
        verify(indexShard, times(0)).flush(any(FlushRequest.class));
    }

    @Test
    public void testVerifyShardBeforeIndexClosing() throws Throwable {
        executeOnPrimaryOrReplica();
        verify(indexShard, times(1)).verifyShardBeforeIndexClosing();
        verify(indexShard, times(1)).flush(any(FlushRequest.class));
    }

    @Test
    public void testVerifyShardBeforeIndexClosingFailed() {
        doThrow(new IllegalStateException("test")).when(indexShard).verifyShardBeforeIndexClosing();
        expectThrows(IllegalStateException.class, this::executeOnPrimaryOrReplica);
        verify(indexShard, times(1)).verifyShardBeforeIndexClosing();
        verify(indexShard, times(0)).flush(any(FlushRequest.class));
    }

    @Test
    public void testUnavailableShardsMarkedAsStale() throws Exception {
        String index = "test";
        ShardId shardId = new ShardId(index, "_na_", 0);

        int nbReplicas = randomIntBetween(1, 10);
        ShardRoutingState[] replicaStates = new ShardRoutingState[nbReplicas];
        Arrays.fill(replicaStates, ShardRoutingState.STARTED);
        final ClusterState clusterState = state(index, true, ShardRoutingState.STARTED, replicaStates);
        setState(clusterService, clusterState);

        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index(index).shard(shardId.id());
        IndexMetadata indexMetaData = clusterState.getMetadata().index(index);
        ShardRouting primaryRouting = shardRoutingTable.primaryShard();
        long primaryTerm = indexMetaData.primaryTerm(0);

        Set<String> inSyncAllocationIds = indexMetaData.inSyncAllocationIds(0);
        Set<String> trackedShards = shardRoutingTable.getAllAllocationIds();

        List<ShardRouting> unavailableShards = randomSubsetOf(
            randomIntBetween(1, nbReplicas),
            shardRoutingTable.replicaShards());
        IndexShardRoutingTable.Builder shardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardRoutingTable);
        unavailableShards.forEach(shardRoutingTableBuilder::removeShard);
        shardRoutingTable = shardRoutingTableBuilder.build();

        ReplicationGroup replicationGroup = new ReplicationGroup(
            shardRoutingTable,
            inSyncAllocationIds,
            trackedShards);
        assertThat(replicationGroup.getUnavailableInSyncShards().size(), greaterThan(0));

        PlainActionFuture<PrimaryResult> listener = new PlainActionFuture<>();
        TransportVerifyShardBeforeCloseAction.ShardRequest request =
            new TransportVerifyShardBeforeCloseAction.ShardRequest(shardId, false, clusterBlock);
        ReplicationOperation.Replicas<TransportVerifyShardBeforeCloseAction.ShardRequest> proxy =
            action.newReplicasProxy();
        ReplicationOperation<TransportVerifyShardBeforeCloseAction.ShardRequest,
            TransportVerifyShardBeforeCloseAction.ShardRequest, PrimaryResult> operation = new ReplicationOperation<>(
            request,
            createPrimary(primaryRouting, replicationGroup),
            listener,
            proxy,
            logger,
            "test",
            primaryTerm
        );
        operation.execute();

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(nbReplicas));

        for (CapturingTransport.CapturedRequest capturedRequest : capturedRequests) {
            String actionName = capturedRequest.action;
            if (actionName.startsWith(ShardStateAction.SHARD_FAILED_ACTION_NAME)) {
                assertThat(capturedRequest.request, instanceOf(ShardStateAction.FailedShardEntry.class));
                String allocationId = ((ShardStateAction.FailedShardEntry) capturedRequest.request).getAllocationId();
                assertTrue(unavailableShards.stream()
                               .anyMatch(shardRouting -> shardRouting.allocationId().getId().equals(allocationId)));
                transport.handleResponse(capturedRequest.requestId, TransportResponse.Empty.INSTANCE);

            } else if (actionName.startsWith(TransportVerifyShardBeforeCloseAction.NAME)) {
                assertThat(capturedRequest.request, instanceOf(ConcreteShardRequest.class));
                String allocationId = ((ConcreteShardRequest) capturedRequest.request).getTargetAllocationID();
                assertFalse(unavailableShards.stream()
                                .anyMatch(shardRouting -> shardRouting.allocationId().getId().equals(allocationId)));
                assertTrue(inSyncAllocationIds.stream()
                               .anyMatch(inSyncAllocationId -> inSyncAllocationId.equals(allocationId)));
                transport.handleResponse(
                    capturedRequest.requestId,
                    new TransportReplicationAction.ReplicaResponse(0L, 0L));

            } else {
                fail("Test does not support action " + capturedRequest.action);
            }
        }

        ReplicationResponse.ShardInfo shardInfo = listener.get().getShardInfo();
        assertThat(shardInfo.getFailed(), equalTo(0));
        assertThat(shardInfo.getFailures(), arrayWithSize(0));
        assertThat(shardInfo.getSuccessful(), equalTo(1 + nbReplicas - unavailableShards.size()));
    }

    private static ReplicationOperation.Primary<
        TransportVerifyShardBeforeCloseAction.ShardRequest,
        TransportVerifyShardBeforeCloseAction.ShardRequest,
        PrimaryResult>
    createPrimary(final ShardRouting primary, final ReplicationGroup replicationGroup) {
        return new ReplicationOperation.Primary<>() {
            @Override
            public ShardRouting routingEntry() {
                return primary;
            }

            @Override
            public ReplicationGroup getReplicationGroup() {
                return replicationGroup;
            }

            @Override
            public void perform(
                TransportVerifyShardBeforeCloseAction.ShardRequest request, ActionListener<PrimaryResult> listener) {
                listener.onResponse(new PrimaryResult(request));
            }

            @Override
            public void failShard(String message, Exception exception) {

            }

            @Override
            public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
            }

            @Override
            public void updateGlobalCheckpointForShard(String allocationId, long globalCheckpoint) {
            }

            @Override
            public long localCheckpoint() {
                return 0;
            }

            @Override
            public long computedGlobalCheckpoint() {
                return 0;
            }

            @Override
            public long globalCheckpoint() {
                return 0;
            }

            @Override
            public long maxSeqNoOfUpdatesOrDeletes() {
                return 0;
            }
        };
    }

    private static class PrimaryResult implements ReplicationOperation.PrimaryResult<TransportVerifyShardBeforeCloseAction.ShardRequest> {

        private final TransportVerifyShardBeforeCloseAction.ShardRequest replicaRequest;
        private final SetOnce<ReplicationResponse.ShardInfo> shardInfo;

        private PrimaryResult(final TransportVerifyShardBeforeCloseAction.ShardRequest replicaRequest) {
            this.replicaRequest = replicaRequest;
            this.shardInfo = new SetOnce<>();
        }

        @Override
        public TransportVerifyShardBeforeCloseAction.ShardRequest replicaRequest() {
            return replicaRequest;
        }

        @Override
        public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
            this.shardInfo.set(shardInfo);
        }

        @Override
        public void runPostReplicationActions(ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        public ReplicationResponse.ShardInfo getShardInfo() {
            return shardInfo.get();
        }
    }
}
