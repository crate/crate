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

import io.crate.es.action.support.replication.TransportReplicationAction;
import io.crate.es.cluster.action.shard.ShardStateAction;
import io.crate.es.cluster.metadata.IndexMetaData;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.routing.ShardIterator;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.settings.Settings;
import io.crate.es.index.shard.IndexShard;
import io.crate.es.indices.IndicesService;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

public class TransportPutChunkAction extends TransportReplicationAction<PutChunkRequest, PutChunkReplicaRequest, PutChunkResponse> {

    private final BlobTransferTarget transferTarget;

    @Inject
    public TransportPutChunkAction(Settings settings,
                                   TransportService transportService,
                                   ClusterService clusterService,
                                   IndicesService indicesService,
                                   ThreadPool threadPool,
                                   ShardStateAction shardStateAction,
                                   BlobTransferTarget transferTarget,
                                   IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutChunkAction.NAME, transportService, clusterService,
            indicesService, threadPool, shardStateAction,
            indexNameExpressionResolver, PutChunkRequest::new, PutChunkReplicaRequest::new, ThreadPool.Names.WRITE);

        this.transferTarget = transferTarget;
    }

    @Override
    protected PutChunkResponse newResponseInstance() {
        return new PutChunkResponse();
    }

    @Override
    protected void resolveRequest(IndexMetaData indexMetaData, PutChunkRequest request) {
        ShardIterator shardIterator = clusterService.operationRouting().indexShards(
            clusterService.state(), request.index(), null, request.digest());
        request.setShardId(shardIterator.shardId());
        super.resolveRequest(indexMetaData, request);
    }

    @Override
    protected PrimaryResult<PutChunkReplicaRequest, PutChunkResponse> shardOperationOnPrimary(PutChunkRequest request, IndexShard primary) {
        PutChunkResponse response = newResponseInstance();
        transferTarget.continueTransfer(request, response);

        final PutChunkReplicaRequest replicaRequest = new PutChunkReplicaRequest();
        replicaRequest.setShardId(request.shardId());
        replicaRequest.transferId = request.transferId();
        replicaRequest.sourceNodeId = clusterService.localNode().getId();
        replicaRequest.currentPos = request.currentPos();
        replicaRequest.content = request.content();
        replicaRequest.isLast = request.isLast();
        replicaRequest.index(request.index());
        return new PrimaryResult<>(replicaRequest, response);
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(PutChunkReplicaRequest shardRequest, IndexShard replica) {
        PutChunkResponse response = newResponseInstance();
        transferTarget.continueTransfer(shardRequest, response);
        return new ReplicaResult();
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }
}

