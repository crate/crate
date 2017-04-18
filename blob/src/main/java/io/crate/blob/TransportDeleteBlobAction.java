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

import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteBlobAction extends TransportReplicationAction<DeleteBlobRequest, DeleteBlobRequest,
    DeleteBlobResponse> {

    private final BlobIndicesService blobIndicesService;

    @Inject
    public TransportDeleteBlobAction(Settings settings,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     IndicesService indicesService,
                                     ThreadPool threadPool,
                                     ShardStateAction shardStateAction,
                                     BlobIndicesService blobIndicesService,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteBlobAction.NAME, transportService, clusterService, indicesService,
            threadPool, shardStateAction, actionFilters, indexNameExpressionResolver, DeleteBlobRequest::new,
            DeleteBlobRequest::new, ThreadPool.Names.INDEX);
        this.blobIndicesService = blobIndicesService;
        logger.trace("Constructor");
    }

    @Override
    protected DeleteBlobResponse newResponseInstance() {
        return new DeleteBlobResponse();
    }

    @Override
    protected void resolveRequest(MetaData metaData, IndexMetaData indexMetaData, DeleteBlobRequest request) {
        ShardIterator shardIterator = clusterService.operationRouting()
            .indexShards(clusterService.state(), request.index(), request.id(), null);
        request.setShardId(shardIterator.shardId());
        super.resolveRequest(metaData, indexMetaData, request);
    }

    @Override
    protected PrimaryResult<DeleteBlobRequest, DeleteBlobResponse> shardOperationOnPrimary(DeleteBlobRequest request,
                                                                                           IndexShard primary) throws Exception {
        logger.trace("shardOperationOnPrimary {}", request);
        BlobShard blobShard = blobIndicesService.blobShardSafe(request.shardId());
        boolean deleted = blobShard.delete(request.id());
        final DeleteBlobResponse response = new DeleteBlobResponse(deleted);
        return new PrimaryResult(request, response);
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(DeleteBlobRequest request, IndexShard replica) throws Exception {
        logger.warn("shardOperationOnReplica operating on replica but relocation is not implemented {}", request);
        BlobShard blobShard = blobIndicesService.blobShardSafe(request.shardId());
        blobShard.delete(request.id());
        return new ReplicaResult();
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }
}

