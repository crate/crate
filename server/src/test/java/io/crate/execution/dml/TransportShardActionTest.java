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

package io.crate.execution.dml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.execution.jobs.kill.KillableCallable;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class TransportShardActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_withActiveOperation_resets_circuit_breaker() throws IOException {
        TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        circuitBreaker.startBreaking();
        CircuitBreakerService circuitBreakerService = mock(CircuitBreakerService.class);
        when(circuitBreakerService.getBreaker(CircuitBreaker.QUERY)).thenReturn(circuitBreaker);

        TransportShardAction transportShardAction = new TransportShardAction(
            Settings.EMPTY,
            "dummy-action",
            mock(TransportService.class),
            clusterService,
            null,
            null,
            null,
            null,
            circuitBreakerService,
            null,
            null
        ) {
            @Override
            protected WritePrimaryResult processRequestItems(IndexShard indexShard, ShardRequest request, AtomicBoolean killed) throws InterruptedException, IOException {
                return null;
            }

            @Override
            protected WriteReplicaResult processRequestItemsOnReplica(IndexShard indexShard, ShardRequest replicaRequest) throws IOException {
                return null;
            }
        };

        long oldUsed = circuitBreaker.getUsed();
        assertThatThrownBy(() -> transportShardAction.withActiveOperation(
            new TestShardRequest(new ShardId("dummy", "dummy", 1), UUID.randomUUID()),
            new KillableCallable(UUID.randomUUID()) {
                @Override
                public Object call() throws Exception {
                    return null;
                }
            }))
            .isExactlyInstanceOf(CircuitBreakingException.class)
            .hasMessage("broken");
        assertThat(circuitBreaker.getUsed()).isEqualTo(oldUsed);
    }

    private static class TestShardRequest extends ShardRequest {

        protected TestShardRequest(ShardId shardId, UUID jobId) {
            super(shardId, jobId);
        }

        @Override
        protected long shallowSize() {
            return 123;
        }
    }
}
