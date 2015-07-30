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

public class TransportStartBlobAction
        extends TransportShardReplicationOperationAction<StartBlobRequest, StartBlobRequest, StartBlobResponse> {

    private final BlobTransferTarget transferTarget;

    @Inject
    public TransportStartBlobAction(Settings settings,
                                    TransportService transportService,
                                    ClusterService clusterService,
                                    IndicesService indicesService,
                                    ThreadPool threadPool,
                                    ShardStateAction shardStateAction,
                                    BlobTransferTarget transferTarget,
                                    ActionFilters actionFilters) {
        super(settings, StartBlobAction.NAME, transportService, clusterService,
                indicesService, threadPool, shardStateAction, actionFilters);
        this.transferTarget = transferTarget;
        logger.trace("Constructor");
    }

    @Override
    protected StartBlobRequest newRequestInstance() {
        logger.trace("newRequestInstance");
        return new StartBlobRequest();
    }

    @Override
    protected StartBlobRequest newReplicaRequestInstance() {
        logger.trace("newReplicaRequestInstance");
        return new StartBlobRequest();
    }

    @Override
    protected StartBlobResponse newResponseInstance() {
        logger.trace("newResponseInstance");
        return new StartBlobResponse();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected Tuple<StartBlobResponse, StartBlobRequest> shardOperationOnPrimary(ClusterState clusterState,
                                                                                 PrimaryOperationRequest shardRequest) {
        logger.trace("shardOperationOnPrimary {}", shardRequest);
        final StartBlobRequest request = shardRequest.request;
        final StartBlobResponse response = newResponseInstance();
        transferTarget.startTransfer(shardRequest.shardId.id(), request, response);
        return new Tuple<>(response, request);

    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        logger.trace("shardOperationOnReplica operating on replica {}", shardRequest);
        final StartBlobRequest request = shardRequest.request;
        final StartBlobResponse response = newResponseInstance();
        transferTarget.startTransfer(shardRequest.shardId.id(), request, response);
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) throws ElasticsearchException {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(),
                        request.concreteIndex(),
                        null,
                        request.request().id(),
                        null);
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

