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

package io.crate.execution.dml.delete;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.execution.dml.ShardResponse;
import io.crate.execution.jobs.TasksService;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.netty.NettyBootstrap;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class TransportShardDeleteActionTest extends CrateDummyClusterServiceUnitTest {

    private static final RelationName TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "characters");

    private TransportShardDeleteAction transportShardDeleteAction;
    private IndexShard indexShard;
    private String indexUUID;
    private NettyBootstrap nettyBootstrap;


    @Before
    public void prepare() throws Exception {
        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();
        indexUUID = UUIDs.randomBase64UUID();
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(new Index(TABLE_IDENT.indexNameOrAlias(), indexUUID))).thenReturn(indexService);
        indexShard = mock(IndexShard.class);
        when(indexService.getShard(0)).thenReturn(indexShard);


        transportShardDeleteAction = new TransportShardDeleteAction(
            Settings.EMPTY,
            MockTransportService.createNewService(
                Settings.EMPTY, Version.CURRENT, THREAD_POOL, nettyBootstrap, clusterService.getClusterSettings()),
            clusterService,
            indicesService,
            mock(TasksService.class),
            mock(ThreadPool.class),
            mock(ShardStateAction.class),
            new NoneCircuitBreakerService()
        );
    }

    @After
    public void teardownNetty() {
        nettyBootstrap.close();
    }

    @Test
    public void testKilledSetWhileProcessingItemsDoesNotThrowExceptionAndMustMarkItemPosition() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), indexUUID, 0);
        final ShardDeleteRequest request = new ShardDeleteRequest(shardId, UUID.randomUUID());
        request.add(1, new ShardDeleteRequest.Item("1"));

        TransportReplicationAction.PrimaryResult<ShardDeleteRequest, ShardResponse> result =
            transportShardDeleteAction.processRequestItems(indexShard, request, new AtomicBoolean(true));

        assertThat(result.finalResponseIfSuccessful.failure()).isExactlyInstanceOf(InterruptedException.class);
        assertThat(result.replicaRequest().skipFromLocation()).isEqualTo(1);
    }

    @Test
    public void testReplicaOperationWillSkipItemsFromMarkedPositionOn() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), indexUUID, 0);
        final ShardDeleteRequest request = new ShardDeleteRequest(shardId, UUID.randomUUID());
        request.add(1, new ShardDeleteRequest.Item("1"));
        request.skipFromLocation(1);

        // replica operation must skip all not by primary processed items
        transportShardDeleteAction.processRequestItemsOnReplica(indexShard, request);
        verify(indexShard, times(0))
            .applyDeleteOperationOnReplica(anyLong(), anyLong(), anyLong(), anyString());
    }

    @Test
    public void test_deletion_failed_with_non_retryable_error_must_not_throw() throws IOException {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), indexUUID, 0);
        final ShardDeleteRequest request = new ShardDeleteRequest(shardId, UUID.randomUUID());
        request.add(1, new ShardDeleteRequest.Item("1"));

        // Mocks are not set. so shardDeleteOperationOnPrimary fails with NPE.
        // test verifies that exception is not thrown (delete is not retried) on NPE.
        TransportReplicationAction.PrimaryResult<ShardDeleteRequest, ShardResponse> result
            = transportShardDeleteAction.processRequestItems(indexShard, request, new AtomicBoolean(false));

        assertThat(result.finalResponseIfSuccessful.failures()).hasSize(1);
        assertThat(result.finalResponseIfSuccessful.failures().getFirst().error().getMessage()).contains("deleteResult\" is null");
    }

    @Test
    public void test_deletion_failed_with_retryable_error_must_throw() throws IOException {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), indexUUID, 0);
        final ShardDeleteRequest request = new ShardDeleteRequest(shardId, UUID.randomUUID());
        request.add(1, new ShardDeleteRequest.Item("1"));

        // item throws exception from the isShardNotAvailableException, processRequestItems must throw as well
        when(indexShard.applyDeleteOperationOnPrimary(
            anyLong(),
            anyString(),
            any(VersionType.class),
            anyLong(),
            anyLong()
        )).thenThrow(new ShardNotFoundException(shardId));

        assertThatThrownBy(() -> transportShardDeleteAction.processRequestItems(indexShard, request, new AtomicBoolean(false)))
            .isExactlyInstanceOf(ShardNotFoundException.class);
    }
}
