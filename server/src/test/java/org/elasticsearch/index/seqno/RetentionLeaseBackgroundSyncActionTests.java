/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */

package org.elasticsearch.index.seqno;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.mockito.ArgumentCaptor;

import io.crate.common.io.IOUtils;

public class RetentionLeaseBackgroundSyncActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private ShardStateAction shardStateAction;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
    }

    public void tearDown() throws Exception {
        try {
            IOUtils.close(transportService, clusterService, transport);
        } finally {
            terminate(threadPool);
        }
        super.tearDown();
    }

    public void testRetentionLeaseBackgroundSyncActionOnPrimary() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final RetentionLeaseBackgroundSyncAction action = new RetentionLeaseBackgroundSyncAction(
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new IndexNameExpressionResolver()
        );
        final RetentionLeases retentionLeases = mock(RetentionLeases.class);
        final RetentionLeaseBackgroundSyncAction.Request request =
                new RetentionLeaseBackgroundSyncAction.Request(indexShard.shardId(), retentionLeases);

        final ReplicationOperation.PrimaryResult<RetentionLeaseBackgroundSyncAction.Request> result =
                action.shardOperationOnPrimary(request, indexShard);
        // the retention leases on the shard should be periodically flushed
        verify(indexShard).afterWriteOperation();
        // we should forward the request containing the current retention leases to the replica
        assertThat(result.replicaRequest(), sameInstance(request));
    }

    public void testRetentionLeaseBackgroundSyncActionOnReplica() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final RetentionLeaseBackgroundSyncAction action = new RetentionLeaseBackgroundSyncAction(
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new IndexNameExpressionResolver()
        );
        final RetentionLeases retentionLeases = mock(RetentionLeases.class);
        final RetentionLeaseBackgroundSyncAction.Request request =
                new RetentionLeaseBackgroundSyncAction.Request(indexShard.shardId(), retentionLeases);

        final TransportReplicationAction.ReplicaResult result = action.shardOperationOnReplica(request, indexShard);
        // the retention leases on the shard should be updated
        verify(indexShard).updateRetentionLeasesOnReplica(retentionLeases);
        // the retention leases on the shard should be periodically flushed
        verify(indexShard).afterWriteOperation();
        // the result should indicate success
        final AtomicBoolean success = new AtomicBoolean();
        result.respond(ActionListener.wrap(r -> success.set(true), e -> fail(e.toString())));
        assertTrue(success.get());
    }

    public void testRetentionLeaseSyncExecution() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final Logger retentionLeaseSyncActionLogger = mock(Logger.class);

        final RetentionLeases retentionLeases = mock(RetentionLeases.class);
        final AtomicBoolean invoked = new AtomicBoolean();
        final RetentionLeaseBackgroundSyncAction action = new RetentionLeaseBackgroundSyncAction(
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                new IndexNameExpressionResolver()) {

            @Override
            protected void doExecute(Task task, Request request, ActionListener<ReplicationResponse> listener) {
                assertThat(request.shardId(), sameInstance(indexShard.shardId()));
                assertThat(request.getRetentionLeases(), sameInstance(retentionLeases));
                if (randomBoolean()) {
                    listener.onResponse(new ReplicationResponse());
                } else {
                    final Exception e = randomFrom(
                            new AlreadyClosedException("closed"),
                            new IndexShardClosedException(indexShard.shardId()),
                            new RuntimeException("failed"));
                    listener.onFailure(e);
                    if (e instanceof AlreadyClosedException == false && e instanceof IndexShardClosedException == false) {
                        final ArgumentCaptor<ParameterizedMessage> captor = ArgumentCaptor.forClass(ParameterizedMessage.class);
                        verify(retentionLeaseSyncActionLogger).warn(captor.capture(), same(e));
                        final ParameterizedMessage message = captor.getValue();
                        assertThat(message.getFormat(), equalTo("{} retention lease background sync failed"));
                        assertThat(message.getParameters(), arrayContaining(indexShard.shardId()));
                    }
                    verifyNoMoreInteractions(retentionLeaseSyncActionLogger);
                }
                invoked.set(true);
            }

            @Override
            protected Logger getLogger() {
                return retentionLeaseSyncActionLogger;
            }
        };

        action.backgroundSync(indexShard.shardId(), retentionLeases);
        assertTrue(invoked.get());
    }

}
