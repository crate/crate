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

package io.crate.execution.dml.delete;

import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.dml.ShardResponse;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.es.Version;
import io.crate.es.action.support.replication.TransportWriteAction;
import io.crate.es.cluster.action.shard.ShardStateAction;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.UUIDs;
import io.crate.es.common.settings.Settings;
import io.crate.es.index.Index;
import io.crate.es.index.IndexService;
import io.crate.es.index.VersionType;
import io.crate.es.index.shard.IndexShard;
import io.crate.es.index.shard.ShardId;
import io.crate.es.indices.IndicesService;
import io.crate.es.test.transport.MockTransportService;
import io.crate.es.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
            mock(IndexNameExpressionResolver.class),
            mock(ClusterService.class),
            indicesService,
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
            .applyDeleteOperationOnReplica(
                anyLong(), anyLong(), anyString(), anyString(), any(VersionType.class));
    }
}
