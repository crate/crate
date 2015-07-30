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

package io.crate.blob;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPutChunkAction extends TransportShardReplicationOperationAction<PutChunkRequest, PutChunkReplicaRequest, PutChunkResponse> {

    private final BlobTransferTarget transferTarget;

    @Inject
    public TransportPutChunkAction(Settings settings,
                                   TransportService transportService,
                                   ClusterService clusterService,
                                   IndicesService indicesService,
                                   ThreadPool threadPool,
                                   ShardStateAction shardStateAction,
                                   BlobTransferTarget transferTarget,
                                   ActionFilters actionFilters) {
        super(settings, PutChunkAction.NAME, transportService, clusterService,
                indicesService, threadPool, shardStateAction, actionFilters);
        this.transferTarget = transferTarget;
    }

    @Override
    protected PutChunkRequest newRequestInstance() {
        return new PutChunkRequest();
    }

    @Override
    protected PutChunkReplicaRequest newReplicaRequestInstance() {
        return new PutChunkReplicaRequest();
    }

    @Override
    protected PutChunkResponse newResponseInstance() {
        return new PutChunkResponse();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected Tuple<PutChunkResponse, PutChunkReplicaRequest> shardOperationOnPrimary(ClusterState clusterState,
                                                                                      PrimaryOperationRequest shardRequest) {
        final PutChunkRequest request = shardRequest.request;
        PutChunkResponse response = newResponseInstance();
        transferTarget.continueTransfer(request, response);

        final PutChunkReplicaRequest replicaRequest = newReplicaRequestInstance();
        replicaRequest.transferId = request.transferId();
        replicaRequest.sourceNodeId = clusterState.getNodes().localNode().getId();
        replicaRequest.currentPos = request.currentPos();
        replicaRequest.content = request.content();
        replicaRequest.isLast = request.isLast();
        replicaRequest.index(request.index());
        return new Tuple<>(response, replicaRequest);
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        final PutChunkReplicaRequest request = shardRequest.request;
        PutChunkResponse response = newResponseInstance();
        transferTarget.continueTransfer(request, response, shardRequest.shardId.id());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) throws ElasticsearchException {
        return clusterService.operationRouting()
            .indexShards(clusterService.state(),
                    request.concreteIndex(),
                    null,
                    null,
                    request.request().digest());
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }
}

