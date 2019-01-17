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

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * A handy one stop shop for creating requests (make sure to import static this class).
 */
public class Requests {

    /**
     * The content type used to generate request builders (query / search).
     */
    public static XContentType CONTENT_TYPE = XContentType.SMILE;

    /**
     * The default content type to use to generate source documents when indexing.
     */
    public static XContentType INDEX_CONTENT_TYPE = XContentType.JSON;

    /**
     * Creates a search request against one or more indices. Note, the search source must be set either using the
     * actual JSON search source, or the {@link org.elasticsearch.search.builder.SearchSourceBuilder}.
     *
     * @param indices The indices to search against. Use {@code null} or {@code _all} to execute against all indices
     * @return The search request
     * @see org.elasticsearch.client.Client#search(org.elasticsearch.action.search.SearchRequest)
     */
    public static SearchRequest searchRequest(String... indices) {
        return new SearchRequest(indices);
    }

    /**
     * Creates a search scroll request allowing to continue searching a previous search request.
     *
     * @param scrollId The scroll id representing the scrollable search
     * @return The search scroll request
     * @see org.elasticsearch.client.Client#searchScroll(org.elasticsearch.action.search.SearchScrollRequest)
     */
    public static SearchScrollRequest searchScrollRequest(String scrollId) {
        return new SearchScrollRequest(scrollId);
    }

    /**
     * Creates an indices exists request.
     *
     * @param indices The indices to check if they exists or not.
     * @return The indices exists request
     * @see org.elasticsearch.client.IndicesAdminClient#exists(org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest)
     */
    public static IndicesExistsRequest indicesExistsRequest(String... indices) {
        return new IndicesExistsRequest(indices);
    }

    /**
     * Creates a create index request.
     *
     * @param index The index to create
     * @return The index create request
     * @see org.elasticsearch.client.IndicesAdminClient#create(org.elasticsearch.action.admin.indices.create.CreateIndexRequest)
     */
    public static CreateIndexRequest createIndexRequest(String index) {
        return new CreateIndexRequest(index);
    }

    /**
     * Creates a delete index request.
     *
     * @param index The index to delete
     * @return The delete index request
     * @see org.elasticsearch.client.IndicesAdminClient#delete(org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest)
     */
    public static DeleteIndexRequest deleteIndexRequest(String index) {
        return new DeleteIndexRequest(index);
    }

    /**
     * Creates a close index request.
     *
     * @param index The index to close
     * @return The delete index request
     * @see org.elasticsearch.client.IndicesAdminClient#close(org.elasticsearch.action.admin.indices.close.CloseIndexRequest)
     */
    public static CloseIndexRequest closeIndexRequest(String index) {
        return new CloseIndexRequest(index);
    }

    /**
     * Create a create mapping request against one or more indices.
     *
     * @param indices The indices to create mapping. Use {@code null} or {@code _all} to execute against all indices
     * @return The create mapping request
     * @see org.elasticsearch.client.IndicesAdminClient#putMapping(org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest)
     */
    public static PutMappingRequest putMappingRequest(String... indices) {
        return new PutMappingRequest(indices);
    }

    /**
     * Creates an index aliases request allowing to add and remove aliases.
     *
     * @return The index aliases request
     */
    public static IndicesAliasesRequest indexAliasesRequest() {
        return new IndicesAliasesRequest();
    }

    /**
     * Creates a refresh indices request.
     *
     * @param indices The indices to refresh. Use {@code null} or {@code _all} to execute against all indices
     * @return The refresh request
     * @see org.elasticsearch.client.IndicesAdminClient#refresh(org.elasticsearch.action.admin.indices.refresh.RefreshRequest)
     */
    public static RefreshRequest refreshRequest(String... indices) {
        return new RefreshRequest(indices);
    }

    /**
     * Creates a flush indices request.
     *
     * @param indices The indices to flush. Use {@code null} or {@code _all} to execute against all indices
     * @return The flush request
     * @see org.elasticsearch.client.IndicesAdminClient#flush(org.elasticsearch.action.admin.indices.flush.FlushRequest)
     */
    public static FlushRequest flushRequest(String... indices) {
        return new FlushRequest(indices);
    }

    /**
     * Creates a synced flush indices request.
     *
     * @param indices The indices to sync flush. Use {@code null} or {@code _all} to execute against all indices
     * @return The synced flush request
     * @see org.elasticsearch.client.IndicesAdminClient#syncedFlush(SyncedFlushRequest)
     */
    public static SyncedFlushRequest syncedFlushRequest(String... indices) {
        return new SyncedFlushRequest(indices);
    }

    /**
     * Creates a force merge request.
     *
     * @param indices The indices to force merge. Use {@code null} or {@code _all} to execute against all indices
     * @return The force merge request
     * @see org.elasticsearch.client.IndicesAdminClient#forceMerge(org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest)
     */
    public static ForceMergeRequest forceMergeRequest(String... indices) {
        return new ForceMergeRequest(indices);
    }

    /**
     * Creates an upgrade request.
     *
     * @param indices The indices to upgrade. Use {@code null} or {@code _all} to execute against all indices
     * @return The upgrade request
     * @see org.elasticsearch.client.IndicesAdminClient#upgrade(UpgradeRequest)
     */
    public static UpgradeRequest upgradeRequest(String... indices) {
        return new UpgradeRequest(indices);
    }

    /**
     * A request to update indices settings.
     *
     * @param indices The indices to update the settings for. Use {@code null} or {@code _all} to executed against all indices.
     * @return The request
     */
    public static UpdateSettingsRequest updateSettingsRequest(String... indices) {
        return new UpdateSettingsRequest(indices);
    }

    /**
     * Creates a cluster state request.
     *
     * @return The cluster state request.
     * @see org.elasticsearch.client.ClusterAdminClient#state(org.elasticsearch.action.admin.cluster.state.ClusterStateRequest)
     */
    public static ClusterStateRequest clusterStateRequest() {
        return new ClusterStateRequest();
    }

    public static ClusterRerouteRequest clusterRerouteRequest() {
        return new ClusterRerouteRequest();
    }

    public static ClusterUpdateSettingsRequest clusterUpdateSettingsRequest() {
        return new ClusterUpdateSettingsRequest();
    }

    /**
     * Creates a cluster health request.
     *
     * @param indices The indices to provide additional cluster health information for.
     *                Use {@code null} or {@code _all} to execute against all indices
     * @return The cluster health request
     * @see org.elasticsearch.client.ClusterAdminClient#health(org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest)
     */
    public static ClusterHealthRequest clusterHealthRequest(String... indices) {
        return new ClusterHealthRequest(indices);
    }

    /**
     * List all shards for the give search
     */
    public static ClusterSearchShardsRequest clusterSearchShardsRequest() {
        return new ClusterSearchShardsRequest();
    }

    /**
     * List all shards for the give search
     */
    public static ClusterSearchShardsRequest clusterSearchShardsRequest(String... indices) {
        return new ClusterSearchShardsRequest(indices);
    }

    /**
     * Creates a nodes tasks request against all the nodes.
     *
     * @return The nodes tasks request
     * @see org.elasticsearch.client.ClusterAdminClient#listTasks(ListTasksRequest)
     */
    public static ListTasksRequest listTasksRequest() {
        return new ListTasksRequest();
    }

    /**
     * Creates a get task request.
     *
     * @return The nodes tasks request
     * @see org.elasticsearch.client.ClusterAdminClient#getTask(GetTaskRequest)
     */
    public static GetTaskRequest getTaskRequest() {
        return new GetTaskRequest();
    }

    /**
     * Creates a nodes tasks request against one or more nodes. Pass {@code null} or an empty array for all nodes.
     *
     * @return The nodes tasks request
     * @see org.elasticsearch.client.ClusterAdminClient#cancelTasks(CancelTasksRequest)
     */
    public static CancelTasksRequest cancelTasksRequest() {
        return new CancelTasksRequest();
    }

    /**
     * Registers snapshot repository
     *
     * @param name repository name
     * @return repository registration request
     */
    public static PutRepositoryRequest putRepositoryRequest(String name) {
        return new PutRepositoryRequest(name);
    }

    /**
     * Gets snapshot repository
     *
     * @param repositories names of repositories
     * @return get repository request
     */
    public static GetRepositoriesRequest getRepositoryRequest(String... repositories) {
        return new GetRepositoriesRequest(repositories);
    }

    /**
     * Deletes registration for snapshot repository
     *
     * @param name repository name
     * @return delete repository request
     */
    public static DeleteRepositoryRequest deleteRepositoryRequest(String name) {
        return new DeleteRepositoryRequest(name);
    }


    /**
     * Creates new snapshot
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     * @return create snapshot request
     */
    public static CreateSnapshotRequest createSnapshotRequest(String repository, String snapshot) {
        return new CreateSnapshotRequest(repository, snapshot);
    }

    /**
     * Gets snapshots from repository
     *
     * @param repository repository name
     * @return get snapshot  request
     */
    public static GetSnapshotsRequest getSnapshotsRequest(String repository) {
        return new GetSnapshotsRequest(repository);
    }

    /**
     * Restores new snapshot
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     * @return snapshot creation request
     */
    public static RestoreSnapshotRequest restoreSnapshotRequest(String repository, String snapshot) {
        return new RestoreSnapshotRequest(repository, snapshot);
    }

    /**
     * Deletes a snapshot
     *
     * @param snapshot   snapshot name
     * @param repository repository name
     * @return delete snapshot request
     */
    public static DeleteSnapshotRequest deleteSnapshotRequest(String repository, String snapshot) {
        return new DeleteSnapshotRequest(repository, snapshot);
    }

    /**
     *  Get status of snapshots
     *
     * @param repository repository name
     * @return snapshot status request
     */
    public static SnapshotsStatusRequest snapshotsStatusRequest(String repository) {
        return new SnapshotsStatusRequest(repository);
    }

}
