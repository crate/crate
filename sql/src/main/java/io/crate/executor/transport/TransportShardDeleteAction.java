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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.crate.exceptions.Exceptions.userFriendlyMessage;

@Singleton
public class TransportShardDeleteAction extends TransportShardAction<ShardDeleteRequest, ShardDeleteRequest.Item> {

    private final static String ACTION_NAME = "indices:crate/data/write/delete";

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
    }

    @Override
    protected WritePrimaryResult<ShardDeleteRequest, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                        ShardDeleteRequest request,
                                                                                        AtomicBoolean killed) throws IOException {
        ShardResponse shardResponse = new ShardResponse();
        Translog.Location translogLocation = null;
        for (ShardDeleteRequest.Item item : request.items) {
            int location = item.location();
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
            } catch (Exception e) {
                if (!TransportActions.isShardNotAvailableException(e)) {
                    throw e;
                } else {
                    logger.debug("{} failed to execute delete for [{}]/[{}]",
                        e, request.shardId(), request.type(), item.id());
                    shardResponse.add(location,
                        new ShardResponse.Failure(
                            item.id(),
                            userFriendlyMessage(e),
                            (e instanceof VersionConflictEngineException)));
                }
            }
        }

        return new WritePrimaryResult<>(request, shardResponse, translogLocation, null, indexShard, logger);
    }

    @Override
    protected WriteReplicaResult<ShardDeleteRequest> processRequestItemsOnReplica(IndexShard indexShard, ShardDeleteRequest request) throws IOException {
        Translog.Location translogLocation = null;
        for (ShardDeleteRequest.Item item : request.items) {
            int location = item.location();
            if (request.skipFromLocation() == location) {
                // skipping this and all next items, the primary did not processed them (mostly due to a kill request)
                break;
            }

            Engine.Delete delete = indexShard.prepareDeleteOnReplica(request.type(), item.id(), item.version(), item.versionType());
            Engine.DeleteResult deleteResult = indexShard.delete(delete);
            translogLocation = deleteResult.getTranslogLocation();
            logger.trace("{} REPLICA: successfully deleted [{}]/[{}]", request.shardId(), request.type(), item.id());
        }
        return new WriteReplicaResult<>(request, translogLocation, null, indexShard, logger);
    }

    private DeleteResult shardDeleteOperationOnPrimary(ShardDeleteRequest request, ShardDeleteRequest.Item item, IndexShard indexShard) throws IOException {
        Engine.Delete delete = indexShard.prepareDeleteOnPrimary(request.type(), item.id(), item.version(), item.versionType());
        Engine.DeleteResult deleteResult = indexShard.delete(delete);
        // update the request with the version so it will go to the replicas
        item.versionType(delete.versionType().versionTypeForReplicationAndRecovery());
        item.version(deleteResult.getVersion());

        assert item.versionType().validateVersionForWrites(item.version()) : "item.version() must be valid";

        return new DeleteResult(deleteResult.isFound(), deleteResult.getTranslogLocation());
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
