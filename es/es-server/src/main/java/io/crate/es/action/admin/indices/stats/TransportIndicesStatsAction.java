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

package io.crate.es.action.admin.indices.stats;

import org.apache.lucene.store.AlreadyClosedException;
import io.crate.es.action.support.DefaultShardOperationFailedException;
import io.crate.es.action.support.broadcast.node.TransportBroadcastByNodeAction;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.cluster.block.ClusterBlockLevel;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.routing.ShardRouting;
import io.crate.es.cluster.routing.ShardsIterator;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.io.stream.StreamInput;
import io.crate.es.common.settings.Settings;
import io.crate.es.index.IndexService;
import io.crate.es.index.engine.CommitStats;
import io.crate.es.index.seqno.SeqNoStats;
import io.crate.es.index.shard.IndexShard;
import io.crate.es.index.shard.ShardNotFoundException;
import io.crate.es.indices.IndicesService;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportIndicesStatsAction extends TransportBroadcastByNodeAction<IndicesStatsRequest, IndicesStatsResponse, ShardStats> {

    private final IndicesService indicesService;

    @Inject
    public TransportIndicesStatsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                       TransportService transportService, IndicesService indicesService,
                                        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, IndicesStatsAction.NAME, threadPool, clusterService, transportService, indexNameExpressionResolver,
                IndicesStatsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    /**
     * Status goes across *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, IndicesStatsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardStats readShardResult(StreamInput in) throws IOException {
        return ShardStats.readShardStats(in);
    }

    @Override
    protected IndicesStatsResponse newResponse(IndicesStatsRequest request, int totalShards, int successfulShards, int failedShards, List<ShardStats> responses, List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        return new IndicesStatsResponse(responses.toArray(new ShardStats[responses.size()]), totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected IndicesStatsRequest readRequestFrom(StreamInput in) throws IOException {
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.readFrom(in);
        return request;
    }

    @Override
    protected ShardStats shardOperation(IndicesStatsRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        // if we don't have the routing entry yet, we need it stats wise, we treat it as if the shard is not ready yet
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        CommonStatsFlags flags = new CommonStatsFlags().clear();

        if (request.docs()) {
            flags.set(CommonStatsFlags.Flag.Docs);
        }
        if (request.store()) {
            flags.set(CommonStatsFlags.Flag.Store);
        }
        if (request.completion()) {
            flags.set(CommonStatsFlags.Flag.Completion);
            flags.completionDataFields(request.completionFields());
        }

        CommitStats commitStats;
        SeqNoStats seqNoStats;
        try {
            commitStats = indexShard.commitStats();
            seqNoStats = indexShard.seqNoStats();
        } catch (AlreadyClosedException e) {
            // shard is closed - no stats is fine
            commitStats = null;
            seqNoStats = null;
        }

        return new ShardStats(
            indexShard.routingEntry(),
            indexShard.shardPath(),
            new CommonStats(indexShard, flags),
            commitStats,
            seqNoStats
        );
    }
}
