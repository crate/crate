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

package io.crate.execution.dml.delete;

import io.crate.exceptions.JobKilledException;
import io.crate.execution.ddl.SchemaUpdateClient;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.TransportShardAction;
import io.crate.execution.jobs.TasksService;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
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

    private static final String ACTION_NAME = "internal:crate:sql/data/delete";

    @Inject
    public TransportShardDeleteAction(Settings settings,
                                      TransportService transportService,
                                      ClusterService clusterService,
                                      IndicesService indicesService,
                                      TasksService tasksService,
                                      ThreadPool threadPool,
                                      ShardStateAction shardStateAction,
                                      SchemaUpdateClient schemaUpdateClient) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            tasksService,
            threadPool,
            shardStateAction,
            ShardDeleteRequest::new,
            schemaUpdateClient
        );
    }

    @Override
    protected WritePrimaryResult<ShardDeleteRequest, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                        ShardDeleteRequest request,
                                                                                        AtomicBoolean killed) throws IOException {
        ShardResponse shardResponse = new ShardResponse();
        Translog.Location translogLocation = null;
        boolean debugEnabled = logger.isDebugEnabled();
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
                Engine.DeleteResult deleteResult = shardDeleteOperationOnPrimary(item, indexShard);
                translogLocation = deleteResult.getTranslogLocation();
                Exception failure = deleteResult.getFailure();
                if (failure == null) {
                    if (deleteResult.isFound()) {
                        if (debugEnabled) {
                            logger.debug("shardId={} successfully deleted id={}", request.shardId(), item.id());
                        }
                        shardResponse.add(location);
                    } else {
                        if (debugEnabled) {
                            logger.debug("shardId={} failed to execute delete for id={}, doc not found",
                                request.shardId(), item.id());
                        }
                        shardResponse.add(location,
                            new ShardResponse.Failure(
                                item.id(),
                                "Document not found while deleting",
                                false));
                    }
                } else {
                    if (debugEnabled) {
                        logger.debug("shardId={} failed to execute delete for id={}: {}",
                            request.shardId(), item.id(), failure);
                    }
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
                    if (debugEnabled) {
                        logger.debug("shardId={} failed to execute delete for id={}: {}",
                                     request.shardId(), item.id(), e);
                    }
                    shardResponse.add(location,
                        new ShardResponse.Failure(
                            item.id(),
                            userFriendlyMessageInclNested(e),
                            (e instanceof VersionConflictEngineException)));
                }
            }
        }

        return new WritePrimaryResult<>(request, shardResponse, translogLocation, null, indexShard);
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
                    item.seqNo(),
                    item.primaryTerm(),
                    item.version(),
                    item.id()
                );

                translogLocation = deleteResult.getTranslogLocation();
                if (logger.isTraceEnabled()) {
                    logger.trace("shardId={} REPLICA: successfully deleted id={}", request.shardId(), item.id());
                }
            }
        }
        return new WriteReplicaResult<>(request, translogLocation, null, indexShard, logger);
    }

    private Engine.DeleteResult shardDeleteOperationOnPrimary(ShardDeleteRequest.Item item, IndexShard indexShard) throws IOException {
        Engine.DeleteResult deleteResult = indexShard.applyDeleteOperationOnPrimary(
            item.version(), item.id(), VersionType.INTERNAL, item.seqNo(), item.primaryTerm());

        // set version and sequence number for replica
        item.version(deleteResult.getVersion());
        item.seqNo(deleteResult.getSeqNo());
        item.primaryTerm(deleteResult.getTerm());

        return deleteResult;
    }
}
