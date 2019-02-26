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

package io.crate.es.client;

import io.crate.es.action.admin.cluster.health.ClusterHealthRequest;
import io.crate.es.action.admin.cluster.state.ClusterStateRequest;
import io.crate.es.action.admin.indices.create.CreateIndexRequest;
import io.crate.es.action.admin.indices.delete.DeleteIndexRequest;
import io.crate.es.action.admin.indices.flush.FlushRequest;
import io.crate.es.action.admin.indices.flush.SyncedFlushRequest;
import io.crate.es.action.admin.indices.forcemerge.ForceMergeRequest;
import io.crate.es.action.admin.indices.mapping.put.PutMappingRequest;
import io.crate.es.action.admin.indices.refresh.RefreshRequest;
import io.crate.es.action.admin.indices.upgrade.post.UpgradeRequest;
import io.crate.es.common.xcontent.XContentType;

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
     * Creates a create index request.
     *
     * @param index The index to create
     * @return The index create request
     * @see io.crate.es.client.IndicesAdminClient#create(io.crate.es.action.admin.indices.create.CreateIndexRequest)
     */
    public static CreateIndexRequest createIndexRequest(String index) {
        return new CreateIndexRequest(index);
    }

    /**
     * Creates a delete index request.
     *
     * @param index The index to delete
     * @return The delete index request
     * @see io.crate.es.client.IndicesAdminClient#delete(io.crate.es.action.admin.indices.delete.DeleteIndexRequest)
     */
    public static DeleteIndexRequest deleteIndexRequest(String index) {
        return new DeleteIndexRequest(index);
    }

    /**
     * Create a create mapping request against one or more indices.
     *
     * @param indices The indices to create mapping. Use {@code null} or {@code _all} to execute against all indices
     * @return The create mapping request
     * @see io.crate.es.client.IndicesAdminClient#putMapping(io.crate.es.action.admin.indices.mapping.put.PutMappingRequest)
     */
    public static PutMappingRequest putMappingRequest(String... indices) {
        return new PutMappingRequest(indices);
    }

    /**
     * Creates a refresh indices request.
     *
     * @param indices The indices to refresh. Use {@code null} or {@code _all} to execute against all indices
     * @return The refresh request
     * @see io.crate.es.client.IndicesAdminClient#refresh(io.crate.es.action.admin.indices.refresh.RefreshRequest)
     */
    public static RefreshRequest refreshRequest(String... indices) {
        return new RefreshRequest(indices);
    }

    /**
     * Creates a flush indices request.
     *
     * @param indices The indices to flush. Use {@code null} or {@code _all} to execute against all indices
     * @return The flush request
     * @see io.crate.es.client.IndicesAdminClient#flush(io.crate.es.action.admin.indices.flush.FlushRequest)
     */
    public static FlushRequest flushRequest(String... indices) {
        return new FlushRequest(indices);
    }

    /**
     * Creates a synced flush indices request.
     *
     * @param indices The indices to sync flush. Use {@code null} or {@code _all} to execute against all indices
     * @return The synced flush request
     * @see io.crate.es.client.IndicesAdminClient#syncedFlush(SyncedFlushRequest)
     */
    public static SyncedFlushRequest syncedFlushRequest(String... indices) {
        return new SyncedFlushRequest(indices);
    }

    /**
     * Creates a force merge request.
     *
     * @param indices The indices to force merge. Use {@code null} or {@code _all} to execute against all indices
     * @return The force merge request
     * @see io.crate.es.client.IndicesAdminClient#forceMerge(io.crate.es.action.admin.indices.forcemerge.ForceMergeRequest)
     */
    public static ForceMergeRequest forceMergeRequest(String... indices) {
        return new ForceMergeRequest(indices);
    }

    /**
     * Creates an upgrade request.
     *
     * @param indices The indices to upgrade. Use {@code null} or {@code _all} to execute against all indices
     * @return The upgrade request
     * @see io.crate.es.client.IndicesAdminClient#upgrade(UpgradeRequest)
     */
    public static UpgradeRequest upgradeRequest(String... indices) {
        return new UpgradeRequest(indices);
    }

    /**
     * Creates a cluster state request.
     *
     * @return The cluster state request.
     * @see io.crate.es.client.ClusterAdminClient#state(io.crate.es.action.admin.cluster.state.ClusterStateRequest)
     */
    public static ClusterStateRequest clusterStateRequest() {
        return new ClusterStateRequest();
    }

    /**
     * Creates a cluster health request.
     *
     * @param indices The indices to provide additional cluster health information for.
     *                Use {@code null} or {@code _all} to execute against all indices
     * @return The cluster health request
     * @see io.crate.es.client.ClusterAdminClient#health(io.crate.es.action.admin.cluster.health.ClusterHealthRequest)
     */
    public static ClusterHealthRequest clusterHealthRequest(String... indices) {
        return new ClusterHealthRequest(indices);
    }
}

