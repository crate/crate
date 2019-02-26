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

package io.crate.es.action.admin.indices.recovery;

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
import io.crate.es.index.shard.IndexShard;
import io.crate.es.indices.IndicesService;
import io.crate.es.indices.recovery.RecoveryState;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport action for shard recovery operation. This transport action does not actually
 * perform shard recovery, it only reports on recoveries (both active and complete).
 */
public class TransportRecoveryAction extends TransportBroadcastByNodeAction<RecoveryRequest, RecoveryResponse, RecoveryState> {

    private final IndicesService indicesService;

    @Inject
    public TransportRecoveryAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                   TransportService transportService, IndicesService indicesService,
                                   IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, RecoveryAction.NAME, threadPool, clusterService, transportService, indexNameExpressionResolver,
                RecoveryRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    @Override
    protected RecoveryState readShardResult(StreamInput in) throws IOException {
        return RecoveryState.readRecoveryState(in);
    }


    @Override
    protected RecoveryResponse newResponse(RecoveryRequest request, int totalShards, int successfulShards, int failedShards, List<RecoveryState> responses, List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        Map<String, List<RecoveryState>> shardResponses = new HashMap<>();
        for (RecoveryState recoveryState : responses) {
            if (recoveryState == null) {
                continue;
            }
            String indexName = recoveryState.getShardId().getIndexName();
            if (!shardResponses.containsKey(indexName)) {
                shardResponses.put(indexName, new ArrayList<>());
            }
            if (request.activeOnly()) {
                if (recoveryState.getStage() != RecoveryState.Stage.DONE) {
                    shardResponses.get(indexName).add(recoveryState);
                }
            } else {
                shardResponses.get(indexName).add(recoveryState);
            }
        }
        return new RecoveryResponse(totalShards, successfulShards, failedShards, shardResponses, shardFailures);
    }

    @Override
    protected RecoveryRequest readRequestFrom(StreamInput in) throws IOException {
        final RecoveryRequest recoveryRequest = new RecoveryRequest();
        recoveryRequest.readFrom(in);
        return recoveryRequest;
    }

    @Override
    protected RecoveryState shardOperation(RecoveryRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        return indexShard.recoveryState();
    }

    @Override
    protected ShardsIterator shards(ClusterState state, RecoveryRequest request, String[] concreteIndices) {
        return state.routingTable().allShardsIncludingRelocationTargets(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, RecoveryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, RecoveryRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }
}
