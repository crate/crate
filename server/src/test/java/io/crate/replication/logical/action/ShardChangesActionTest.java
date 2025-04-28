/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.action;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Before;
import org.junit.Test;

import io.crate.netty.NettyBootstrap;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ShardChangesActionTest extends CrateDummyClusterServiceUnitTest {

    private static final Index INDEX = new Index("t2", UUIDs.randomBase64UUID());
    protected final ShardId shardId = new ShardId(INDEX, 0);

    private ShardChangesAction.TransportAction transportAction;
    private IndexShard indexShard;
    private NettyBootstrap nettyBootstrap;

    @Before
    public void prepare() throws Exception {
        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();

        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);

        when(indicesService.indexServiceSafe(INDEX)).thenReturn(indexService);
        indexShard = mock(IndexShard.class);
        when(indexService.getShard(0)).thenReturn(indexShard);
        when(indexShard.shardId()).thenReturn(shardId);

        transportAction = new ShardChangesAction.TransportAction(
            THREAD_POOL,
            clusterService,
            MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                THREAD_POOL,
                nettyBootstrap,
                clusterService.getClusterSettings()
            ),
            indicesService
        );

        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)")
            .addTable("CREATE TABLE doc.t2 (id int)")
            .startShards("doc.t1");      // <- only t1 has active primary shards;
    }

    @Test
    public void test_sync_poll_changes_throws_retryable_error_for_index_with_non_active_primary_shards() {
        var request = new ShardChangesAction.Request(shardId, 1, 2);
        assertThatThrownBy(
            () -> transportAction.shardOperation(request, shardId))
            .isExactlyInstanceOf(NoShardAvailableActionException.class);
    }

    @Test
    public void test_async_poll_changes_throws_retryable_error_for_index_with_non_active_primary_shards() throws Exception {
        var request = new ShardChangesAction.Request(shardId, 1, 2);
        ActionListener<ShardChangesAction.Response> listener = mock(ActionListener.class);
        transportAction.asyncShardOperation(request, shardId, listener);
        verify(listener, times(1)).onFailure(any(NoShardAvailableActionException.class));
    }
}
