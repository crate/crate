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

package org.elasticsearch.action.admin.indices.forcemerge;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
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

/**
 * ForceMerge index/indices action.
 */
public class TransportForceMergeAction extends TransportBroadcastByNodeAction<ForceMergeRequest, ForceMergeResponse, TransportBroadcastByNodeAction.EmptyResult> {

    private final IndicesService indicesService;

    @Inject
    public TransportForceMergeAction(ClusterService clusterService,
                                     TransportService transportService,
                                     IndicesService indicesService) {
        super(
            ForceMergeAction.NAME,
            clusterService,
            transportService,
            ForceMergeRequest::new,
            ThreadPool.Names.FORCE_MERGE,
            true
        );
        this.indicesService = indicesService;
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.INSTANCE;
    }

    @Override
    protected ForceMergeResponse newResponse(ForceMergeRequest request, int totalShards, int successfulShards, int failedShards, List<EmptyResult> responses, List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        return new ForceMergeResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ForceMergeRequest readRequestFrom(StreamInput in) throws IOException {
        return new ForceMergeRequest(in);
    }

    @Override
    protected void shardOperation(ForceMergeRequest request, ShardRouting shardRouting, ActionListener<EmptyResult> listener) throws IOException {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id());
        indexShard.flush(false);
        indexShard.forceMerge(request);
        listener.onResponse(EmptyResult.INSTANCE);
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, ForceMergeRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ForceMergeRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ForceMergeRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
