/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport;

import io.crate.exceptions.JobKilledException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class TransportShardDeleteAction extends TransportShardAction<ShardDeleteRequest, ShardDeleteRequest.Item> {

    private final static String ACTION_NAME = "indices:crate/data/write/delete";
    private final IndicesService indicesService;

    @Inject
    public TransportShardDeleteAction(Settings settings,
                                      TransportService transportService,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      ClusterService clusterService,
                                      IndicesService indicesService,
                                      ThreadPool threadPool,
                                      ShardStateAction shardStateAction,
                                      ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, indexNameExpressionResolver,
            clusterService, indicesService, threadPool, shardStateAction, actionFilters, ShardDeleteRequest::new);
        this.indicesService = indicesService;
    }

    @Override
    protected WriteResult<ShardResponse> processRequestItems(ShardId shardId, ShardDeleteRequest request, AtomicBoolean killed) throws InterruptedException {
        ShardResponse shardResponse = new ShardResponse();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        Translog.Location translogLocation = null;
        for (int i = 0; i < request.itemIndices().size(); i++) {
            int location = request.itemIndices().get(i);
            ShardDeleteRequest.Item item = request.items().get(i);
            if (killed.get()) {
                // set failure on response, mark current item and skip all next items.
                // this way replica operation will be executed, but only items already processed here
                // will be processed on the replica
                request.skipFromLocation(location);
                shardResponse.failure(new InterruptedException(JobKilledException.MESSAGE));
                break;
            }
            try {
                DeleteResult deleteResult = shardDeleteOperationOnPrimary(request, item, indexShard);
                translogLocation = deleteResult.location;
                if (deleteResult.found) {
                    logger.debug("{} successfully deleted [{}]/[{}]", request.shardId(), request.type(), item.id());
                    shardResponse.add(location);
                } else {
                    logger.debug("{} failed to execute delete for [{}]/[{}], doc not found",
                        request.shardId(), request.type(), item.id());
                    shardResponse.add(location,
                        new ShardResponse.Failure(
                            item.id(),
                            "Document not found while deleting",
                            false));

                }
            } catch (Throwable t) {
                if (!TransportActions.isShardNotAvailableException(t)) {
                    throw t;
                } else {
                    logger.debug("{} failed to execute delete for [{}]/[{}]",
                        t, request.shardId(), request.type(), item.id());
                    shardResponse.add(location,
                        new ShardResponse.Failure(
                            item.id(),
                            ExceptionsHelper.detailedMessage(t),
                            (t instanceof VersionConflictEngineException)));
                }
            }
        }

        return new WriteResult<>(shardResponse, translogLocation);
    }

    @Override
    protected Translog.Location processRequestItemsOnReplica(ShardId shardId, ShardDeleteRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        Translog.Location translogLocation = null;
        for (int i = 0; i < request.itemIndices().size(); i++) {
            int location = request.itemIndices().get(i);
            if (request.skipFromLocation() == location) {
                // skipping this and all next items, the primary did not processed them (mostly due to a kill request)
                break;
            }

            ShardDeleteRequest.Item item = request.items().get(i);
            Engine.Delete delete = indexShard.prepareDeleteOnReplica(request.type(), item.id(), item.version(), item.versionType());
            indexShard.delete(delete);
            logger.trace("{} REPLICA: successfully deleted [{}]/[{}]", request.shardId(), request.type(), item.id());
            translogLocation = delete.getTranslogLocation();
        }
        return translogLocation;
    }

    private DeleteResult shardDeleteOperationOnPrimary(ShardDeleteRequest request, ShardDeleteRequest.Item item, IndexShard indexShard) {
        Engine.Delete delete = indexShard.prepareDeleteOnPrimary(request.type(), item.id(), item.version(), item.versionType());
        indexShard.delete(delete);
        // update the request with the version so it will go to the replicas
        item.versionType(delete.versionType().versionTypeForReplicationAndRecovery());
        item.version(delete.version());

        assert item.versionType().validateVersionForWrites(item.version()) : "item.version() must be valid";

        return new DeleteResult(delete.found(), delete.getTranslogLocation());
    }

    private static class DeleteResult {
        private final boolean found;
        private final Translog.Location location;

        DeleteResult(boolean found, Translog.Location location) {
            this.found = found;
            this.location = location;
        }
    }
}
