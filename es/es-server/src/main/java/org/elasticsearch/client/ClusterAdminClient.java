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

package org.elasticsearch.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#cluster()
 */
public interface ClusterAdminClient extends ElasticsearchClient {

    /**
     * The health of the cluster.
     *
     * @param request The cluster state request
     * @return The result future
     */
    ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request);

    /**
     * The health of the cluster.
     */
    ClusterHealthRequestBuilder prepareHealth(String... indices);

    /**
     * The state of the cluster.
     *
     * @param request The cluster state request.
     * @return The result future
     */
    ActionFuture<ClusterStateResponse> state(ClusterStateRequest request);

    /**
     * The state of the cluster.
     */
    ClusterStateRequestBuilder prepareState();

    /**
     * Update settings in the cluster.
     */
    ClusterUpdateSettingsRequestBuilder prepareUpdateSettings();

    /**
     * Update settings in the cluster.
     */
    ClusterRerouteRequestBuilder prepareReroute();

    /**
     * Registers a snapshot repository.
     */
    PutRepositoryRequestBuilder preparePutRepository(String name);

    /**
     * Unregisters a repository.
     */
    DeleteRepositoryRequestBuilder prepareDeleteRepository(String name);

    /**
     * Creates a new snapshot.
     */
    CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name);

    /**
     * Get snapshot.
     */
    GetSnapshotsRequestBuilder prepareGetSnapshots(String repository);

    /**
     * Delete snapshot.
     */
    DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String snapshot);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    PendingClusterTasksRequestBuilder preparePendingClusterTasks();

    /**
     * Nodes stats of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#nodesStatsRequest(String...)
     */
    void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener);


}
