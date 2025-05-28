/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;

public class TransportDeleteBlob extends TransportReplicationAction<DeleteBlobRequest, DeleteBlobRequest, DeleteBlobResponse> {

    public static final Action ACTION = new Action();
    private final BlobIndicesService blobIndicesService;

    public static class Action extends ActionType<DeleteBlobResponse> {
        private static final String NAME = "internal:crate:blob/delete_blob";

        private Action() {
            super(NAME);
        }
    }

    @Inject
    public TransportDeleteBlob(Settings settings,
                               TransportService transportService,
                               ClusterService clusterService,
                               IndicesService indicesService,
                               ThreadPool threadPool,
                               ShardStateAction shardStateAction,
                               BlobIndicesService blobIndicesService) {
        super(
            settings,
            ACTION.name(),
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            DeleteBlobRequest::new,
            DeleteBlobRequest::new,
            ThreadPool.Names.WRITE
        );
        this.blobIndicesService = blobIndicesService;
        logger.trace("Constructor");
    }

    @Override
    protected DeleteBlobResponse newResponseInstance(StreamInput in) throws IOException {
        return new DeleteBlobResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(DeleteBlobRequest shardRequest,
                                           IndexShard primary,
                                           ActionListener<PrimaryResult<DeleteBlobRequest, DeleteBlobResponse>> listener) {
        try {
            logger.trace("shardOperationOnPrimary {}", shardRequest);
            BlobShard blobShard = blobIndicesService.blobShardSafe(shardRequest.shardId());
            boolean deleted = blobShard.delete(shardRequest.id());
            final DeleteBlobResponse response = new DeleteBlobResponse(deleted);
            listener.onResponse(new PrimaryResult<>(shardRequest, response));
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(DeleteBlobRequest request, IndexShard replica) {
        logger.warn("shardOperationOnReplica operating on replica but relocation is not implemented {}", request);
        BlobShard blobShard = blobIndicesService.blobShardSafe(request.shardId());
        blobShard.delete(request.id());
        return new ReplicaResult();
    }
}

