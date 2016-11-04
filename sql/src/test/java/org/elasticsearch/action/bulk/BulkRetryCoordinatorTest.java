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

package org.elasticsearch.action.bulk;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.transport.ShardResponse;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class BulkRetryCoordinatorTest extends CrateUnitTest {

    private static TableIdent charactersIdent = new TableIdent(null, "foo");
    private static Reference fooRef = new Reference(
        new ReferenceIdent(charactersIdent, "bar"), RowGranularity.DOC, DataTypes.STRING);
    private static ShardId shardId = new ShardId("foo", UUIDs.randomBase64UUID(), 1);

    @Before
    public void prepare() throws Exception {
    }

    private static ShardUpsertRequest shardRequest() {
        return new ShardUpsertRequest.Builder(
            TimeValue.timeValueMillis(10),
            false,
            false,
            null,
            new Reference[]{fooRef},
            UUID.randomUUID()
        ).newRequest(shardId, "node-1");
    }

    @Test
    public void testScheduleRetryAfterRejectedExecution() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        BulkRetryCoordinator coordinator = new BulkRetryCoordinator(threadPool);

        BulkRequestExecutor<ShardUpsertRequest> executor = (request, listener) -> {
            listener.onFailure(new EsRejectedExecutionException("Dummy execution rejected"));
        };
        coordinator.retry(shardRequest(), executor, new ActionListener<ShardResponse>() {
            @Override
            public void onResponse(ShardResponse shardResponse) {
            }

            @Override
            public void onFailure(Exception e) {
            }
        });

        verify(threadPool).schedule(eq(TimeValue.timeValueMillis(0)),
            eq(ThreadPool.Names.SAME),
            any(Runnable.class));
    }

    @Test
    public void testNoPendingOperationsOnFailedExecution() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        BulkRetryCoordinator coordinator = new BulkRetryCoordinator(threadPool);

        BulkRequestExecutor<ShardUpsertRequest> executor = (request, listener) -> {
            listener.onFailure(new InterruptedException("Dummy execution failed"));
        };

        final SettableFuture<ShardResponse> future = SettableFuture.create();
        coordinator.retry(shardRequest(), executor, new ActionListener<ShardResponse>() {
            @Override
            public void onResponse(ShardResponse shardResponse) {
            }

            @Override
            public void onFailure(Exception e) {
                future.set(null);
            }
        });

        ShardResponse response = future.get();
        assertNull(response);
        assertEquals(0, coordinator.numPendingOperations());
    }

    @Test
    public void testParallelSuccessfulExecution() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        final BulkRetryCoordinator coordinator = new BulkRetryCoordinator(threadPool);

        final BulkRequestExecutor<ShardUpsertRequest> executor = (request, listener) -> {
            listener.onResponse(new ShardResponse());
        };

        final CountDownLatch latch = new CountDownLatch(1000);
        ExecutorService executorService = Executors.newFixedThreadPool(10, daemonThreadFactory("DummyThreadPool"));
        for (int i = 0; i < 1000; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    coordinator.retry(shardRequest(), executor, new ActionListener<ShardResponse>() {
                        @Override
                        public void onResponse(ShardResponse shardResponse) {
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                        }
                    });
                }
            });
        }
        latch.await();
        assertEquals(0, coordinator.numPendingOperations());
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        executorService.shutdown();
    }
}
