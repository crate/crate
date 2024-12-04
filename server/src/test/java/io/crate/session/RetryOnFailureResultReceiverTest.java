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

package io.crate.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.ConnectTransportException;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;


public class RetryOnFailureResultReceiverTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testRetryOnNodeConnectionError() throws Exception {
        AtomicInteger numRetries = new AtomicInteger(0);
        BaseResultReceiver baseResultReceiver = new BaseResultReceiver();
        ClusterState initialState = clusterService.state();
        RetryOnFailureResultReceiver<?> retryOnFailureResultReceiver = new RetryOnFailureResultReceiver<>(
            3,
            clusterService,
            indexName -> true,
            baseResultReceiver,
            UUID.randomUUID(),
            (newJobId, receiver) -> numRetries.incrementAndGet());

        // Must have a different cluster state then the initial state to trigger a retry
        clusterService.submitStateUpdateTask("dummy", new DummyUpdate());
        assertBusy(() -> assertThat(initialState).isNotSameAs(clusterService.state()));

        retryOnFailureResultReceiver.fail(new ConnectTransportException(null, "node not connected"));

        assertThat(numRetries.get()).isEqualTo(1);
    }

    @Test
    public void testRetryIsInvokedOnIndexNotFoundException() throws Exception {
        AtomicInteger numRetries = new AtomicInteger(0);
        BaseResultReceiver resultReceiver = new BaseResultReceiver();

        ClusterState initialState = clusterService.state();
        RetryOnFailureResultReceiver<?> retryOnFailureResultReceiver = new RetryOnFailureResultReceiver<>(
            3,
            clusterService,
            indexName -> true,
            resultReceiver,
            UUID.randomUUID(),
            (newJobId, receiver) -> numRetries.incrementAndGet());

        // Must have a different cluster state then the initial state to trigger a retry
        clusterService.submitStateUpdateTask("dummy", new DummyUpdate());
        assertBusy(() -> assertThat(initialState).isNotSameAs(clusterService.state()));

        retryOnFailureResultReceiver.fail(new IndexNotFoundException("t1"));

        assertThat(numRetries.get()).isEqualTo(1);
    }

    private static class DummyUpdate extends ClusterStateUpdateTask {
        @Override
        public ClusterState execute(ClusterState currentState) {
            return ClusterState.builder(currentState).build();
        }

        @Override
        public void onFailure(String source, Exception e) {
        }
    }
}
