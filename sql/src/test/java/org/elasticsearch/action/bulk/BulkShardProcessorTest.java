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

import com.google.common.collect.ImmutableList;
import io.crate.core.collections.RowN;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.operation.collect.ShardingProjector;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.StringType;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkShardProcessorTest extends CrateUnitTest {

    TableIdent charactersIdent = new TableIdent(null, "characters");

    Reference idRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(charactersIdent, "id"), RowGranularity.DOC, DataTypes.INTEGER));

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
        TransportShardUpsertActionDelegate transportShardBulkActionDelegate = new TransportShardUpsertActionDelegate() {
            @Override
            public void execute(ShardUpsertRequest request, ActionListener<ShardUpsertResponse> listener) {
                ref.set(listener);
            }
        };

        Map<Reference, Symbol> insertAssignments = new HashMap<Reference, Symbol>(){{
            put(idRef, new InputColumn(0, IntegerType.INSTANCE));
            put(fooRef, new InputColumn(1, StringType.INSTANCE));
        }};

        ShardingProjector shardingProjector = new ShardingProjector(
                ImmutableList.of(idRef.ident().columnIdent()),
                ImmutableList.<Symbol>of(new InputColumn(0, IntegerType.INSTANCE)),
                null,
                null
        );

        BulkRetryCoordinator bulkRetryCoordinator = new BulkRetryCoordinator(
                ImmutableSettings.EMPTY
        );
        BulkRetryCoordinatorPool coordinatorPool = mock(BulkRetryCoordinatorPool.class);
        when(coordinatorPool.coordinator(any(ShardId.class))).thenReturn(bulkRetryCoordinator);

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
                new DataType[0],
                Collections.<Integer>emptyList(),
                TimeValue.timeValueMillis(10),
                false,
                false,
                null,
                insertAssignments,
                null,
                UUID.randomUUID()
        );

        final BulkShardProcessor<ShardUpsertRequest, ShardUpsertResponse> bulkShardProcessor = new BulkShardProcessor<>(
                clusterService,
                ImmutableSettings.EMPTY,
                mock(TransportBulkCreateIndicesAction.class),
                shardingProjector,
                false,
                1,
                coordinatorPool,
                builder,
                transportShardBulkActionDelegate,
                UUID.randomUUID()
        );
        try {
            bulkShardProcessor.add("foo", new RowN(new Object[]{1, "bar1"}), null);

            ActionListener<ShardUpsertResponse> listener = ref.get();
            listener.onFailure(new RuntimeException("a random exception"));
            assertFalse(bulkShardProcessor.add("foo", new RowN(new Object[]{2, "bar2"}), null));
            bulkShardProcessor.result().get();
        } catch (ExecutionException e) {
            throw e.getCause();
        } finally {
            bulkRetryCoordinator.close();
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

        // listener will be executed 2 times, once for the successfully added row and once for the failure
        final CountDownLatch listenerLatch = new CountDownLatch(2);
        final AtomicReference<ActionListener<ShardUpsertResponse>> ref = new AtomicReference<>();
        TransportShardUpsertActionDelegate transportShardUpsertActionDelegate = new TransportShardUpsertActionDelegate() {
            @Override
            public void execute(ShardUpsertRequest request, ActionListener<ShardUpsertResponse> listener) {
                ref.set(listener);
                listenerLatch.countDown();
            }
        };

        Map<Reference, Symbol> insertAssignments = new HashMap<Reference, Symbol>(){{
            put(idRef, new InputColumn(0, IntegerType.INSTANCE));
            put(fooRef, new InputColumn(1, StringType.INSTANCE));
        }};

        ShardingProjector shardingProjector = new ShardingProjector(
                ImmutableList.of(idRef.ident().columnIdent()),
                ImmutableList.<Symbol>of(new InputColumn(0, IntegerType.INSTANCE)),
                null,
                null
        );

        TransportActionProvider transportActionProvider = mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get());
        when(transportActionProvider.transportShardUpsertActionDelegate()).thenReturn(transportShardUpsertActionDelegate);
        BulkRetryCoordinator bulkRetryCoordinator = new BulkRetryCoordinator(
                ImmutableSettings.EMPTY
        );
        BulkRetryCoordinatorPool coordinatorPool = mock(BulkRetryCoordinatorPool.class);
        when(coordinatorPool.coordinator(any(ShardId.class))).thenReturn(bulkRetryCoordinator);

        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
                new DataType[0],
                Collections.<Integer>emptyList(),
                TimeValue.timeValueMillis(10),
                false,
                false,
                null,
                insertAssignments,
                null,
                UUID.randomUUID()
        );

        final BulkShardProcessor<ShardUpsertRequest, ShardUpsertResponse> bulkShardProcessor = new BulkShardProcessor<>(
                clusterService,
                ImmutableSettings.EMPTY,
                mock(TransportBulkCreateIndicesAction.class),
                shardingProjector,
                false,
                1,
                coordinatorPool,
                builder,
                transportShardUpsertActionDelegate,
                UUID.randomUUID()
        );
        bulkShardProcessor.add("foo", new RowN(new Object[]{1, "bar1"}), null);
        final ActionListener<ShardUpsertResponse> listener = ref.get();

        listener.onFailure(new EsRejectedExecutionException());
        // wait, failure retry lock is done in decoupled thread
        listenerLatch.await(10, TimeUnit.SECONDS);

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
                    bulkShardProcessor.add("foo", new RowN(new Object[]{2, "bar2"}), null);
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