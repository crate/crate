/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.operation.OperationRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class BulkShardProcessorTest {

    @Captor
    private ArgumentCaptor<ActionListener<BulkShardResponse>> bulkShardResponseListener;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testThatAddAfterFailureBlocksDueToRetry() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        OperationRouting operationRouting = mock(OperationRouting.class);

        mockShard(operationRouting, 1);
        mockShard(operationRouting, 2);
        mockShard(operationRouting, 3);
        when(clusterService.operationRouting()).thenReturn(operationRouting);
        TransportShardBulkAction transportShardBulkAction = mock(TransportShardBulkAction.class);

        final BulkShardProcessor bulkShardProcessor = new BulkShardProcessor(
                clusterService,
                ImmutableSettings.EMPTY,
                transportShardBulkAction,
                mock(TransportCreateIndexAction.class),
                false,
                1
        );

        bulkShardProcessor.add("foo", new BytesArray("{\"foo\": \"bar1\"}"), "1", null);
        verify(transportShardBulkAction).execute(
                any(BulkShardRequest.class),
                bulkShardResponseListener.capture());

        ActionListener<BulkShardResponse> listener = bulkShardResponseListener.getValue();

        listener.onFailure(new EsRejectedExecutionException());

        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        final AtomicBoolean hadBlocked = new AtomicBoolean(false);
        final AtomicBoolean hasBlocked = new AtomicBoolean(true);
        final CountDownLatch latch = new CountDownLatch(1);
        scheduledExecutorService.execute(new Runnable() {
            @Override
            public void run() {
                scheduledExecutorService.schedule(new Runnable() {
                    @Override
                    public void run() {
                        hadBlocked.set(hasBlocked.get());
                        latch.countDown();
                    }
                }, 10, TimeUnit.MILLISECONDS);
                bulkShardProcessor.add("foo", new BytesArray("{\"foo\": \"bar2\"}"), "2", null);
                hasBlocked.set(false);
            }
        });
        latch.await();
        assertTrue(hadBlocked.get());
    }

    private void mockShard(OperationRouting operationRouting, Integer shardId) {
        ShardIterator shardIterator = mock(ShardIterator.class);
        when(operationRouting.indexShards(
                any(ClusterState.class),
                anyString(),
                anyString(),
                Matchers.eq(shardId.toString()),
                anyString())).thenReturn(shardIterator);
        when(shardIterator.shardId()).thenReturn(new ShardId("foo", shardId));
    }
}