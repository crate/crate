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

import io.crate.blob.v2.BlobIndices;
import io.crate.blob.v2.BlobShard;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteBlobAction extends TransportReplicationAction<DeleteBlobRequest, DeleteBlobRequest,
        DeleteBlobResponse> {

    private final BlobIndices blobIndices;

    @Inject
    public TransportDeleteBlobAction(Settings settings,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     IndicesService indicesService,
                                     ThreadPool threadPool,
                                     ShardStateAction shardStateAction,
                                     BlobIndices blobIndices,
                                     MappingUpdatedAction mappingUpdatedAction,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteBlobAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction,
                mappingUpdatedAction, actionFilters, indexNameExpressionResolver, DeleteBlobRequest.class, DeleteBlobRequest.class, ThreadPool.Names.INDEX);
        this.blobIndices = blobIndices;
        logger.trace("Constructor");
    }

    @Override
    protected DeleteBlobResponse newResponseInstance() {
        return new DeleteBlobResponse();
    }

    @Override
    protected Tuple<DeleteBlobResponse, DeleteBlobRequest> shardOperationOnPrimary(ClusterState clusterState,
                                                                                   PrimaryOperationRequest shardRequest) {
        logger.trace("shardOperationOnPrimary {}", shardRequest);
        final DeleteBlobRequest request = shardRequest.request;
        BlobShard blobShard = blobIndices.blobShardSafe(shardRequest.request.index(), shardRequest.shardId.id());
        boolean deleted = blobShard.delete(request.id());
        final DeleteBlobResponse response = new DeleteBlobResponse(deleted);
        return new Tuple<>(response, request);
    }

    @Override
    protected void shardOperationOnReplica(ShardId shardId, DeleteBlobRequest shardRequest) {
        logger.warn("shardOperationOnReplica operating on replica but relocation is not implemented {}", shardRequest);
        BlobShard blobShard = blobIndices.blobShardSafe(shardRequest.index(), shardId.id());
        blobShard.delete(shardRequest.id());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) throws ElasticsearchException {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(),
                        request.concreteIndex(),
                        null,
                        request.request().id(),
                        null
                );
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

