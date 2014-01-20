/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.blob.stats;

import org.cratedb.blob.v2.BlobIndices;
import org.cratedb.blob.v2.BlobShard;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

public class TransportBlobStatsAction extends
    TransportBroadcastOperationAction<BlobStatsRequest, BlobStatsResponse, BlobStatsShardRequest, BlobStatsShardResponse> {

    private final BlobIndices blobIndicesService;

    @Inject
    public TransportBlobStatsAction(Settings settings, ThreadPool threadPool,
                                    ClusterService clusterService, TransportService transportService, BlobIndices blobIndices)
    {
        super(settings, threadPool, clusterService, transportService);
        this.blobIndicesService = blobIndices;
    }

    @Override
    protected String transportAction() {
        return BlobStatsAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected BlobStatsRequest newRequest() {
        return new BlobStatsRequest();
    }

    @Override
    protected BlobStatsResponse newResponse(BlobStatsRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {

        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        List<BlobStatsShardResponse> responses = newArrayList();

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                failedShards++;
            } else if (shardResponse instanceof BlobStatsShardResponse) {
                successfulShards++;
                responses.add((BlobStatsShardResponse)shardResponse);
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(
                    new DefaultShardOperationFailedException((BroadcastShardOperationFailedException)shardResponse));
                failedShards++;
            }
        }

        return new BlobStatsResponse(responses.toArray(new BlobStatsShardResponse[responses.size()]),
            failedShards + successfulShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected BlobStatsShardRequest newShardRequest() {
        return new BlobStatsShardRequest();
    }

    @Override
    protected BlobStatsShardRequest newShardRequest(ShardRouting shard, BlobStatsRequest request) {
        return new BlobStatsShardRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected BlobStatsShardResponse newShardResponse() {
        return new BlobStatsShardResponse();
    }

    @Override
    protected BlobStatsShardResponse shardOperation(BlobStatsShardRequest request) throws ElasticSearchException {
        BlobShard blobShard = blobIndicesService.blobShardSafe(request.index(), request.shardId());
        return new BlobStatsShardResponse(request.index(), request.shardId(), blobShard.blobStats(),
            blobShard.shardRouting());
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, BlobStatsRequest request, String[] concreteIndices) {

        List<String> blobIndices = new ArrayList<String>();
        ImmutableOpenMap<String, IndexMetaData> indexMetaDataMap = clusterState.getMetaData().getIndices();
        for (String index : concreteIndices) {
            if (indexMetaDataMap.get(index).getSettings().getAsBoolean(
                BlobIndices.SETTING_BLOBS_ENABLED, false))
            {
                blobIndices.add(index);
            }
        }

        if (blobIndices.isEmpty()) {
            return new GroupShardsIterator(new ArrayList<ShardIterator>());
        }
        return clusterState.routingTable().allAssignedShardsGrouped(blobIndices.toArray(new String[blobIndices.size()]), true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, BlobStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, BlobStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }
}
