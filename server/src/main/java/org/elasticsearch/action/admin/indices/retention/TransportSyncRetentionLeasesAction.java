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

package org.elasticsearch.action.admin.indices.retention;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportSyncRetentionLeasesAction extends TransportBroadcastByNodeAction<
    SyncRetentionLeasesRequest, BroadcastResponse, ReplicationResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportSyncRetentionLeasesAction(ClusterService clusterService,
                                              TransportService transportService,
                                              IndicesService indicesService) {
        super(SyncRetentionLeasesAction.NAME,
            clusterService,
            transportService,
            SyncRetentionLeasesRequest::new,
            ThreadPool.Names.FORCE_MERGE,
            true);
        this.indicesService = indicesService;
    }

    @Override
    protected ReplicationResponse readShardResult(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected BroadcastResponse newResponse(SyncRetentionLeasesRequest request, int totalShards, int successfulShards, int failedShards, List<ReplicationResponse> replicationResponses, List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        return new BroadcastResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected SyncRetentionLeasesRequest readRequestFrom(StreamInput in) throws IOException {
        return new SyncRetentionLeasesRequest(in);
    }

    @Override
    protected void shardOperation(SyncRetentionLeasesRequest request, ShardRouting shardRouting, ActionListener<ReplicationResponse> listener) throws IOException {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id());
        indexShard.runUnderPrimaryPermit(
            () -> indexShard.syncRetentionLeases(true, listener),
            e -> {
                logger.warn("Retention lease sync failed on shard " + indexShard.shardId(), e);
                // Listener here wraps an exception in a BroadcastShardOperationFailedException when handling failure.
                listener.onFailure(e);
            },
            ThreadPool.Names.SAME,
            "retention lease sync"
        );
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, SyncRetentionLeasesRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allPrimaryShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, SyncRetentionLeasesRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, SyncRetentionLeasesRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
