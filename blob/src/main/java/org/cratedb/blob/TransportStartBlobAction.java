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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.blob;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportStartBlobAction extends TransportShardReplicationOperationAction<StartBlobRequest, StartBlobRequest,
        StartBlobResponse> {

    private final BlobTransferTarget transferTarget;

    @Inject
    public TransportStartBlobAction(Settings settings,
            TransportService transportService,
            ClusterService clusterService,
            IndicesService indicesService,
            ThreadPool threadPool,
            ShardStateAction shardStateAction,
            BlobTransferTarget transferTarget) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.transferTarget = transferTarget;
        logger.info("Constructor");
    }

    @Override
    protected StartBlobRequest newRequestInstance() {
        logger.info("newRequestInstance");
        return new StartBlobRequest();
    }

    @Override
    protected StartBlobRequest newReplicaRequestInstance() {
        logger.info("newReplicaRequestInstance");
        return new StartBlobRequest();
    }

    @Override
    protected StartBlobResponse newResponseInstance() {
        logger.info("newResponseInstance");
        return new StartBlobResponse();
    }

    @Override
    protected String transportAction() {
        return StartBlobAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected PrimaryResponse<StartBlobResponse, StartBlobRequest> shardOperationOnPrimary(ClusterState clusterState,
            PrimaryOperationRequest shardRequest) {
        logger.info("shardOperationOnPrimary {}", shardRequest);
        final StartBlobRequest request = shardRequest.request;
        final StartBlobResponse response = newResponseInstance();
        transferTarget.startTransfer(shardRequest.shardId, request, response);
        return new PrimaryResponse<StartBlobResponse, StartBlobRequest>(
                request, response, null);

    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        logger.trace("shardOperationOnReplica operating on replica {}", shardRequest);
        final StartBlobRequest request = shardRequest.request;
        final StartBlobResponse response = newResponseInstance();
        transferTarget.startTransfer(shardRequest.shardId, request, response);
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, StartBlobRequest request) throws ElasticSearchException {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(),
                        request.index(),
                        null,
                        null, request.id());
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, StartBlobRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, StartBlobRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }
}

