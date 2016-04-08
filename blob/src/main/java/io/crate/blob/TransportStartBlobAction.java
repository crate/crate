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
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportStartBlobAction
        extends TransportReplicationAction<StartBlobRequest, StartBlobRequest, StartBlobResponse> {

    private final BlobTransferTarget transferTarget;

    @Inject
    public TransportStartBlobAction(Settings settings,
                                    TransportService transportService,
                                    ClusterService clusterService,
                                    IndicesService indicesService,
                                    ThreadPool threadPool,
                                    ShardStateAction shardStateAction,
                                    BlobTransferTarget transferTarget,
                                    MappingUpdatedAction mappingUpdatedAction,
                                    ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, StartBlobAction.NAME, transportService, clusterService,
                indicesService, threadPool, shardStateAction, mappingUpdatedAction, actionFilters,
                indexNameExpressionResolver, StartBlobRequest.class, StartBlobRequest.class, ThreadPool.Names.INDEX);

        this.transferTarget = transferTarget;
        logger.trace("Constructor");
    }

    @Override
    protected StartBlobResponse newResponseInstance() {
        logger.trace("newResponseInstance");
        return new StartBlobResponse();
    }

    @Override
    protected Tuple<StartBlobResponse, StartBlobRequest> shardOperationOnPrimary(MetaData metaData,
                                                                                 StartBlobRequest request) throws Throwable {
        logger.trace("shardOperationOnPrimary {}", request);
        final StartBlobResponse response = newResponseInstance();
        transferTarget.startTransfer(request.shardId().id(), request, response);
        return new Tuple<>(response, request);
    }

    @Override
    protected void shardOperationOnReplica(StartBlobRequest request) {
        logger.trace("shardOperationOnReplica operating on replica {}", request);
        final StartBlobResponse response = newResponseInstance();
        transferTarget.startTransfer(request.shardId().id(), request, response);
    }

    @Override
    protected void resolveRequest(MetaData metaData, String concreteIndex, StartBlobRequest request) {
        ShardIterator shardIterator = clusterService.operationRouting().indexShards(
                clusterService.state(), concreteIndex, null, request.id(), null);
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

