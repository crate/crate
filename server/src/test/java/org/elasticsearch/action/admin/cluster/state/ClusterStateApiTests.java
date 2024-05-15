/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CompletableFuture;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.common.unit.TimeValue;

public class ClusterStateApiTests extends IntegTestCase {

    @Test
    public void testWaitForMetadataVersion() throws Exception {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForTimeout(TimeValue.timeValueHours(1));
        CompletableFuture<ClusterStateResponse> future1 = client().admin().cluster().state(clusterStateRequest);
        assertBusy(() -> {
            assertThat(future1.isDone()).isTrue();
        });
        assertThat(future1.get().isWaitForTimedOut()).isFalse();
        long metadataVersion = future1.get().getState().metadata().version();

        // Verify that cluster state api returns after the cluster settings have been updated:
        clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForMetadataVersion(metadataVersion + 1);

        CompletableFuture<ClusterStateResponse> future2 = client().admin().cluster().state(clusterStateRequest);
        assertThat(future2.isDone()).isFalse();

        // Pick an arbitrary dynamic cluster setting and change it. Just to get metadata version incremented:
        execute("set global transient cluster.max_shards_per_node = 999");

        assertBusy(() -> {
            assertThat(future2.isDone()).isTrue();
        });
        ClusterStateResponse response = future2.get();
        assertThat(response.isWaitForTimedOut()).isFalse();
        assertThat(response.getState().metadata().version(), equalTo(metadataVersion + 1));

        // Verify that the timed out property has been set"
        metadataVersion = response.getState().metadata().version();
        clusterStateRequest.waitForMetadataVersion(metadataVersion + 1);
        clusterStateRequest.waitForTimeout(TimeValue.timeValueSeconds(1)); // Fail fast
        CompletableFuture<ClusterStateResponse> future3 = client().admin().cluster().state(clusterStateRequest);
        assertBusy(() -> {
            assertThat(future3.isDone()).isTrue();
        });
        response = future3.get();
        assertThat(response.isWaitForTimedOut()).isTrue();
        assertThat(response.getState(), nullValue());

        // Remove transient setting, otherwise test fails with the reason that this test leaves state behind:
        execute("reset global cluster.max_shards_per_node");
    }

}
