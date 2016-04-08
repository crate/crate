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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
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
    protected Tuple<DeleteBlobResponse, DeleteBlobRequest> shardOperationOnPrimary(MetaData metaData,
                                                                                   DeleteBlobRequest request) throws Throwable {
        logger.trace("shardOperationOnPrimary {}", request);
        BlobShard blobShard = blobIndices.blobShardSafe(request.index(), request.shardId().id());
        boolean deleted = blobShard.delete(request.id());
        final DeleteBlobResponse response = new DeleteBlobResponse(deleted);
        return new Tuple<>(response, request);
    }

    @Override
    protected void shardOperationOnReplica(DeleteBlobRequest request) {
        logger.warn("shardOperationOnReplica operating on replica but relocation is not implemented {}", request);
        BlobShard blobShard = blobIndices.blobShardSafe(request.index(), request.shardId().id());
        blobShard.delete(request.id());
    }

    @Override
    protected void resolveRequest(MetaData metaData, String concreteIndex, DeleteBlobRequest request) {
        ShardIterator shardIterator = clusterService.operationRouting()
                .indexShards(clusterService.state(), concreteIndex, null, request.id(), null);
        request.setShardId(shardIterator.shardId());
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

