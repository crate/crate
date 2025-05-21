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


import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealth;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStats;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.state.TransportClusterState;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportRefresh;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettings;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStats;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

public interface Client extends Releasable {

    /**
     * Executes a generic action, denoted by an {@link ActionType}.
     *
     * @param action           The action type to execute.
     * @param request          The action request.
     * @param <Request>        The request type.
     * @param <Response>       The response type.
     */
    <Req extends TransportRequest, Resp extends TransportResponse> CompletableFuture<Resp> execute(ActionType<Resp> action, Req request);


    default CompletableFuture<ClusterHealthResponse> health(final ClusterHealthRequest request) {
        return execute(TransportClusterHealth.ACTION, request);
    }

    default CompletableFuture<ClusterStateResponse> state(final ClusterStateRequest request) {
        return execute(TransportClusterState.ACTION, request);
    }

    default CompletableFuture<NodesStatsResponse> nodesStats(final NodesStatsRequest request) {
        return execute(TransportNodesStats.ACTION, request);
    }

    default CompletableFuture<RefreshResponse> refresh(final RefreshRequest request) {
        return execute(TransportRefresh.ACTION, request);
    }

    default CompletableFuture<IndicesStatsResponse> stats(final IndicesStatsRequest request) {
        return execute(TransportIndicesStats.ACTION, request);
    }

    default CompletableFuture<AcknowledgedResponse> updateSettings(UpdateSettingsRequest request) {
        return execute(TransportUpdateSettings.ACTION, request);
    }
}
