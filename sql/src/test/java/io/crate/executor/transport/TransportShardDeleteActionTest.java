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

package io.crate.executor.transport;

import io.crate.metadata.TableIdent;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class TransportShardDeleteActionTest {

    private final static TableIdent TABLE_IDENT = new TableIdent(null, "characters");

    private TransportShardDeleteAction transportShardDeleteAction;
    private IndexShard indexShard;

    @Before
    public void prepare() throws Exception {
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(TABLE_IDENT.indexName())).thenReturn(indexService);
        indexShard = mock(IndexShard.class);
        when(indexService.shardSafe(0)).thenReturn(indexShard);


        transportShardDeleteAction = new TransportShardDeleteAction(
            Settings.EMPTY,
            mock(TransportService.class),
            mock(MappingUpdatedAction.class),
            mock(IndexNameExpressionResolver.class),
            mock(ClusterService.class),
            indicesService,
            mock(ThreadPool.class),
            mock(ShardStateAction.class),
            mock(ActionFilters.class)
        );
    }

    @Test
    public void testKilledSetWhileProcessingItemsDoesNotThrowExceptionAndMustMarkItemPosition() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), 0);
        final ShardDeleteRequest request = new ShardDeleteRequest(shardId, null, UUID.randomUUID());
        request.add(1, new ShardDeleteRequest.Item("1"));

        ShardResponse shardResponse = transportShardDeleteAction.processRequestItems(
            shardId, request, new AtomicBoolean(true));

        assertThat(shardResponse.failure(), instanceOf(InterruptedException.class));
        assertThat(request.skipFromLocation(), is(1));
    }

    @Test
    public void testReplicaOperationWillSkipItemsFromMarkedPositionOn() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexName(), 0);
        final ShardDeleteRequest request = new ShardDeleteRequest(shardId, null, UUID.randomUUID());
        request.add(1, new ShardDeleteRequest.Item("1"));
        request.skipFromLocation(1);

        // replica operation must skip all not by primary processed items
        transportShardDeleteAction.processRequestItemsOnReplica(shardId, request);
        verify(indexShard, times(0)).delete(any(Engine.Delete.class));
    }
}
