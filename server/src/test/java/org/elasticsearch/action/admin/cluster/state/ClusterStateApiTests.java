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

import io.crate.common.unit.TimeValue;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateApiTests extends ESIntegTestCase {

    public void testWaitForMetaDataVersion() throws Exception {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForTimeout(TimeValue.timeValueHours(1));
        ActionFuture<ClusterStateResponse> future1 = client().admin().cluster().state(clusterStateRequest);
        assertBusy(() -> {
            assertThat(future1.isDone(), is(true));
        });
        assertThat(future1.actionGet().isWaitForTimedOut(), is(false));
        long metadataVersion = future1.actionGet().getState().getMetadata().version();

        // Verify that cluster state api returns after the cluster settings have been updated:
        clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForMetadataVersion(metadataVersion + 1);

        ActionFuture<ClusterStateResponse> future2 = client().admin().cluster().state(clusterStateRequest);
        assertThat(future2.isDone(), is(false));

        // Pick an arbitrary dynamic cluster setting and change it. Just to get metadata version incremented:
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(
            "cluster.max_shards_per_node",
            999)).execute().actionGet());

        assertBusy(() -> {
            assertThat(future2.isDone(), is(true));
        });
        ClusterStateResponse response = future2.actionGet();
        assertThat(response.isWaitForTimedOut(), is(false));
        assertThat(response.getState().metadata().version(), equalTo(metadataVersion + 1));

        // Verify that the timed out property has been set"
        metadataVersion = response.getState().getMetadata().version();
        clusterStateRequest.waitForMetadataVersion(metadataVersion + 1);
        clusterStateRequest.waitForTimeout(TimeValue.timeValueSeconds(1)); // Fail fast
        ActionFuture<ClusterStateResponse> future3 = client().admin().cluster().state(clusterStateRequest);
        assertBusy(() -> {
            assertThat(future3.isDone(), is(true));
        });
        response = future3.actionGet();
        assertThat(response.isWaitForTimedOut(), is(true));
        assertThat(response.getState(), nullValue());

        // Remove transient setting, otherwise test fails with the reason that this test leaves state behind:
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(
            "cluster.max_shards_per_node",
            (String) null)).execute().actionGet());
    }

}
