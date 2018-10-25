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

package io.crate.execution.dml.delete;

import io.crate.exceptions.JobKilledException;
import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.TransportShardAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.crate.exceptions.Exceptions.userFriendlyMessageInclNested;

@Singleton
public class TransportShardDeleteAction extends TransportShardAction<ShardDeleteRequest, ShardDeleteRequest.Item> {

    private static final String ACTION_NAME = "indices:crate/data/write/delete";

    @Inject
    public TransportShardDeleteAction(Settings settings,
                                      TransportService transportService,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      ClusterService clusterService,
                                      IndicesService indicesService,
                                      ThreadPool threadPool,
                                      ShardStateAction shardStateAction,
                                      ActionFilters actionFilters,
                                      SchemaUpdateClient schemaUpdateClient) {
        super(settings, ACTION_NAME, transportService, indexNameExpressionResolver,
            clusterService, indicesService, threadPool, shardStateAction, actionFilters, ShardDeleteRequest::new,
            schemaUpdateClient);
    }

    @Override
    protected WritePrimaryResult<ShardDeleteRequest, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                        ShardDeleteRequest request,
                                                                                        AtomicBoolean killed) throws IOException {
        ShardResponse shardResponse = new ShardResponse();
        Translog.Location translogLocation = null;
        for (ShardDeleteRequest.Item item : request.items()) {
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
                Engine.DeleteResult deleteResult = shardDeleteOperationOnPrimary(request, item, indexShard);
                translogLocation = deleteResult.getTranslogLocation();
                Exception failure = deleteResult.getFailure();
                if (failure == null) {
                    if (deleteResult.isFound()) {
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
                } else {
                    logger.debug("{} failed to execute delete for [{}]/[{}]",
                        failure, request.shardId(), request.type(), item.id());
                    shardResponse.add(location,
                        new ShardResponse.Failure(
                            item.id(),
                            userFriendlyMessageInclNested(failure),
                            (failure instanceof VersionConflictEngineException)));
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
                            userFriendlyMessageInclNested(e),
                            (e instanceof VersionConflictEngineException)));
                }
            }
        }

        return new WritePrimaryResult<>(request, shardResponse, translogLocation, null, indexShard, logger);
    }

    @Override
    protected WriteReplicaResult<ShardDeleteRequest> processRequestItemsOnReplica(IndexShard indexShard, ShardDeleteRequest request) throws IOException {
        Translog.Location translogLocation = null;
        for (ShardDeleteRequest.Item item : request.items()) {
            int location = item.location();
            if (request.skipFromLocation() == location) {
                // skipping this and all next items, the primary did not processed them (mostly due to a kill request)
                break;
            }

            // Only execute delete operation on replica if the sequence number was applied from primary.
            // If that's not the case, the delete on primary didn't succeed. Note that we still need to
            // process the other items in case of a bulk request.
            if (item.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                Engine.DeleteResult deleteResult = indexShard.applyDeleteOperationOnReplica(
                    item.seqNo(), item.version(), request.type(), item.id(), VersionType.EXTERNAL);

                translogLocation = deleteResult.getTranslogLocation();
                if (logger.isTraceEnabled()) {
                    logger.trace("{} REPLICA: successfully deleted [{}]/[{}]", request.shardId(), request.type(), item.id());
                }
            }
        }
        return new WriteReplicaResult<>(request, translogLocation, null, indexShard, logger);
    }

    private Engine.DeleteResult shardDeleteOperationOnPrimary(ShardDeleteRequest request, ShardDeleteRequest.Item item, IndexShard indexShard) throws IOException {
        Engine.DeleteResult deleteResult = indexShard.applyDeleteOperationOnPrimary(
            item.version(), request.type(), item.id(), VersionType.INTERNAL);

        // set version and sequence number for replica
        item.version(deleteResult.getVersion());
        item.seqNo(deleteResult.getSeqNo());

        return deleteResult;
    }
}
