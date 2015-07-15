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

import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.executor.transport.SymbolBasedShardUpsertRequest;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Reference;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.operation.OperationRouting;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;
import org.mockito.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SymbolBasedBulkShardProcessorTest extends CrateUnitTest {

    TableIdent charactersIdent = new TableIdent(null, "characters");

    Reference fooRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(charactersIdent, "foo"), RowGranularity.DOC, DataTypes.STRING));

    @Captor
    private ArgumentCaptor<ActionListener<BulkShardResponse>> bulkShardResponseListener;

    @Mock(answer = Answers.RETURNS_MOCKS)
    ClusterService clusterService;

    @Test
    public void testNonEsRejectedExceptionDoesNotResultInRetryButAborts() throws Throwable {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("a random exception");

        final AtomicReference<ActionListener<ShardUpsertResponse>> ref = new AtomicReference<>();
        SymbolBasedTransportShardUpsertActionDelegate transportShardBulkActionDelegate = new SymbolBasedTransportShardUpsertActionDelegate() {
            @Override
            public void execute(SymbolBasedShardUpsertRequest request, ActionListener<ShardUpsertResponse> listener) {
                ref.set(listener);
            }
        };

        BulkRetryCoordinator bulkRetryCoordinator = new BulkRetryCoordinator(
                ImmutableSettings.EMPTY
        );
        BulkRetryCoordinatorPool coordinatorPool = mock(BulkRetryCoordinatorPool.class);
        when(coordinatorPool.coordinator(any(ShardId.class))).thenReturn(bulkRetryCoordinator);

        SymbolBasedShardUpsertRequest.Builder builder = new SymbolBasedShardUpsertRequest.Builder(
                TimeValue.timeValueMillis(10),
                false,
                false,
                null,
                new Reference[]{fooRef}
        );
        final SymbolBasedBulkShardProcessor<SymbolBasedShardUpsertRequest, ShardUpsertResponse> bulkShardProcessor = new SymbolBasedBulkShardProcessor<>(
                clusterService,
                mock(TransportBulkCreateIndicesAction.class),
                ImmutableSettings.EMPTY,
                coordinatorPool,
                false,
                1,
                builder,
                transportShardBulkActionDelegate,
                null
        );
        fail("id");
        bulkShardProcessor.add("foo", "1", new Object[]{"bar1"}, null, null);

        ActionListener<ShardUpsertResponse> listener = ref.get();
        listener.onFailure(new RuntimeException("a random exception"));

        assertFalse(bulkShardProcessor.add("foo", "2", new Object[]{"bar2"}, null, null));

        try {
            bulkShardProcessor.result().get();
        } catch (ExecutionException e) {
            throw e.getCause();
        } finally {
            bulkShardProcessor.close();
        }
    }

    @Test
    public void testThatAddAfterFailureBlocksDueToRetry() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        OperationRouting operationRouting = mock(OperationRouting.class);

        mockShard(operationRouting, 1);
        mockShard(operationRouting, 2);
        mockShard(operationRouting, 3);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        final AtomicReference<ActionListener<ShardUpsertResponse>> ref = new AtomicReference<>();
        SymbolBasedTransportShardUpsertActionDelegate transportShardUpsertActionDelegate = new SymbolBasedTransportShardUpsertActionDelegate() {
            @Override
            public void execute(SymbolBasedShardUpsertRequest request, ActionListener<ShardUpsertResponse> listener) {
                ref.set(listener);
            }
        };

        TransportActionProvider transportActionProvider = mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get());
        when(transportActionProvider.symbolBasedTransportShardUpsertActionDelegate()).thenReturn(transportShardUpsertActionDelegate);

        BulkRetryCoordinator bulkRetryCoordinator = new BulkRetryCoordinator(
                ImmutableSettings.EMPTY
        );
        BulkRetryCoordinatorPool coordinatorPool = mock(BulkRetryCoordinatorPool.class);
        when(coordinatorPool.coordinator(any(ShardId.class))).thenReturn(bulkRetryCoordinator);

        SymbolBasedShardUpsertRequest.Builder builder = new SymbolBasedShardUpsertRequest.Builder(
                TimeValue.timeValueMillis(10),
                false,
                false,
                null,
                new Reference[]{fooRef}
        );

        final SymbolBasedBulkShardProcessor<SymbolBasedShardUpsertRequest, ShardUpsertResponse> bulkShardProcessor = new SymbolBasedBulkShardProcessor<>(
                clusterService,
                mock(TransportBulkCreateIndicesAction.class),
                ImmutableSettings.EMPTY,
                coordinatorPool,
                false,
                1,
                builder,
                transportShardUpsertActionDelegate,
                null
        );
        fail("id");
        bulkShardProcessor.add("foo", "1", new Object[]{"bar1"}, null, null);
        final ActionListener<ShardUpsertResponse> listener = ref.get();

        listener.onFailure(new EsRejectedExecutionException());
        // wait, failure retry lock is done in decoupled thread
        Thread.sleep(1);

        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        try {
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
                    bulkShardProcessor.add("foo", "2", new Object[]{"bar2"}, null, null);
                    hasBlocked.set(false);
                }
            });
            latch.await();
            assertTrue(hadBlocked.get());
        } finally {
            scheduledExecutorService.shutdownNow();
            bulkRetryCoordinator.close();
        }
    }

    @Test
    public void testKill() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        OperationRouting operationRouting = mock(OperationRouting.class);

        mockShard(operationRouting, 1);
        mockShard(operationRouting, 2);
        mockShard(operationRouting, 3);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        final AtomicReference<ActionListener<ShardUpsertResponse>> ref = new AtomicReference<>();
        SymbolBasedTransportShardUpsertActionDelegate transportShardUpsertActionDelegate = new SymbolBasedTransportShardUpsertActionDelegate() {
            @Override
            public void execute(SymbolBasedShardUpsertRequest request, ActionListener<ShardUpsertResponse> listener) {
                ref.set(listener);
            }
        };

        TransportActionProvider transportActionProvider = mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get());
        when(transportActionProvider.symbolBasedTransportShardUpsertActionDelegate()).thenReturn(transportShardUpsertActionDelegate);

        BulkRetryCoordinator bulkRetryCoordinator = new BulkRetryCoordinator(
                ImmutableSettings.EMPTY
        );
        BulkRetryCoordinatorPool coordinatorPool = mock(BulkRetryCoordinatorPool.class);
        when(coordinatorPool.coordinator(any(ShardId.class))).thenReturn(bulkRetryCoordinator);

        SymbolBasedShardUpsertRequest.Builder builder = new SymbolBasedShardUpsertRequest.Builder(
                TimeValue.timeValueMillis(10),
                false,
                false,
                null,
                new Reference[]{fooRef}
        );

        final SymbolBasedBulkShardProcessor<SymbolBasedShardUpsertRequest, ShardUpsertResponse> bulkShardProcessor = new SymbolBasedBulkShardProcessor<>(
                clusterService,
                mock(TransportBulkCreateIndicesAction.class),
                ImmutableSettings.EMPTY,
                coordinatorPool,
                false,
                1,
                builder,
                transportShardUpsertActionDelegate,
                null
        );
        fail("id");
        assertThat(bulkShardProcessor.add("foo", "1", new Object[]{"bar1"}, null, null), is(true));
        bulkShardProcessor.kill();
        // A CancellationException is thrown
        expectedException.expect(CancellationException.class);
        bulkShardProcessor.result().get();
        // it's not possible to add more
        assertThat(bulkShardProcessor.add("foo", "1", new Object[]{"bar1"}, null, null), is(false));
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