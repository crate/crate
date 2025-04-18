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

package org.elasticsearch.action.admin.indices.refresh;

import java.util.List;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportBroadcastReplicationAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportService;

/**
 * Refresh action.
 */
public class TransportRefresh extends TransportBroadcastReplicationAction<RefreshRequest, RefreshResponse, BasicReplicationRequest, ReplicationResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<RefreshResponse> {
        private static final String NAME = "indices:admin/refresh";

        private Action() {
            super(NAME);
        }
    }

    @Inject
    public TransportRefresh(ClusterService clusterService,
                            TransportService transportService,
                            NodeClient client) {
        super(
            ACTION.name(),
            RefreshRequest::new,
            clusterService,
            transportService,
            client,
            TransportShardRefreshAction.TYPE);
    }

    @Override
    protected ReplicationResponse newShardResponse() {
        return new ReplicationResponse();
    }

    @Override
    protected BasicReplicationRequest newShardRequest(RefreshRequest request, ShardId shardId) {
        BasicReplicationRequest replicationRequest = new BasicReplicationRequest(shardId);
        replicationRequest.waitForActiveShards(ActiveShardCount.NONE);
        return replicationRequest;
    }

    @Override
    protected RefreshResponse newResponse(int successfulShards, int failedShards, int totalNumCopies,
                                          List<DefaultShardOperationFailedException> shardFailures) {
        return new RefreshResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
