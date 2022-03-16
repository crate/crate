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

import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.jobs.TasksService;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportShardDeleteActionTest extends CrateDummyClusterServiceUnitTest {

    private final static RelationName TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "characters");

    private TransportShardDeleteAction transportShardDeleteAction;
    private IndexShard indexShard;
    private String indexUUID;

    @Before
    public void prepare() throws Exception {
        indexUUID = UUIDs.randomBase64UUID();
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(new Index(TABLE_IDENT.indexNameOrAlias(), indexUUID))).thenReturn(indexService);
        indexShard = mock(IndexShard.class);
        when(indexService.getShard(0)).thenReturn(indexShard);


        transportShardDeleteAction = new TransportShardDeleteAction(
            Settings.EMPTY,
            MockTransportService.createNewService(
                Settings.EMPTY, Version.CURRENT, THREAD_POOL, clusterService.getClusterSettings()),
            clusterService,
            indicesService,
            mock(TasksService.class),
            mock(ThreadPool.class),
            mock(ShardStateAction.class),
            mock(SchemaUpdateClient.class)
        );
    }

    @Test
    public void testKilledSetWhileProcessingItemsDoesNotThrowExceptionAndMustMarkItemPosition() throws Exception {
        ShardId shardId = new ShardId(TABLE_IDENT.indexNameOrAlias(), indexUUID, 0);
        final ShardDeleteRequest request = new ShardDeleteRequest(shardId, UUID.randomUUID());
        request.add(1, new ShardDeleteRequest.Item("1"));

        TransportWriteAction.WritePrimaryResult<ShardDeleteRequest, ShardResponse> result =
            transportShardDeleteAction.processRequestItems(indexShard, request, new AtomicBoolean(true));

        assertThat(result.finalResponseIfSuccessful.failure(), instanceOf(InterruptedException.class));
        assertThat(request.skipFromLocation(), is(1));
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
}
