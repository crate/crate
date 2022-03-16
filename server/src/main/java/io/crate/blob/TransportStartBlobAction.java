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

public class TransportStartBlobAction extends TransportReplicationAction<StartBlobRequest, StartBlobRequest, StartBlobResponse> {

    private final BlobTransferTarget transferTarget;

    @Inject
    public TransportStartBlobAction(Settings settings,
                                    TransportService transportService,
                                    ClusterService clusterService,
                                    IndicesService indicesService,
                                    ThreadPool threadPool,
                                    ShardStateAction shardStateAction,
                                    BlobTransferTarget transferTarget) {
        super(
            settings,
            StartBlobAction.NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            StartBlobRequest::new,
            StartBlobRequest::new,
            ThreadPool.Names.WRITE
        );
        this.transferTarget = transferTarget;
        logger.trace("Constructor");
    }

    @Override
    protected StartBlobResponse newResponseInstance(StreamInput in) throws IOException {
        logger.trace("newResponseInstance");
        return new StartBlobResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(StartBlobRequest request,
                                           IndexShard primary,
                                           ActionListener<PrimaryResult<StartBlobRequest, StartBlobResponse>> listener) {
        ActionListener.completeWith(listener, () -> {
            logger.trace("shardOperationOnPrimary {}", request);
            final StartBlobResponse response = new StartBlobResponse();
            transferTarget.startTransfer(request, response);
            return new PrimaryResult<>(request, response);
        });
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(StartBlobRequest request, IndexShard replica) {
        logger.trace("shardOperationOnReplica operating on replica {}", request);
        final StartBlobResponse response = new StartBlobResponse();
        transferTarget.startTransfer(request, response);
        return new ReplicaResult();
    }
}

