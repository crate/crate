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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.Constants;
import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.JobKilledException;
import io.crate.execution.dml.ShardResponse;
import io.crate.execution.dml.TransportShardAction;
import io.crate.execution.dml.delete.ShardDeleteRequest.Item;
import io.crate.execution.jobs.TasksService;

@Singleton
public class TransportShardDeleteAction extends TransportShardAction<
        ShardDeleteRequest,
        ShardDeleteRequest,
        ShardDeleteRequest.Item,
        ShardDeleteRequest.Item> {

    @Inject
    public TransportShardDeleteAction(Settings settings,
                                      TransportService transportService,
                                      ClusterService clusterService,
                                      IndicesService indicesService,
                                      TasksService tasksService,
                                      ThreadPool threadPool,
                                      ShardStateAction shardStateAction,
                                      CircuitBreakerService circuitBreakerService) {
        super(
            settings,
            ShardDeleteAction.NAME,
            transportService,
            clusterService,
            indicesService,
            tasksService,
            threadPool,
            shardStateAction,
            circuitBreakerService,
            ShardDeleteRequest::new,
            ShardDeleteRequest::new
        );
    }

    @Override
    protected WritePrimaryResult<ShardDeleteRequest, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                        ShardDeleteRequest request,
                                                                                        AtomicBoolean killed) throws IOException {
        ShardResponse shardResponse = new ShardResponse();
        Translog.Location translogLocation = null;
        boolean debugEnabled = logger.isDebugEnabled();
        ShardDeleteRequest replicaRequest = new ShardDeleteRequest(request.shardId(), request.jobId());
        for (ShardDeleteRequest.Item item : request.items()) {
            int location = item.location();
            if (killed.get()) {
                // set failure on response, mark current item and skip all next items.
                // this way replica operation will be executed, but only items already processed here
                // will be processed on the replica
                replicaRequest.skipFromLocation(location);
                shardResponse.failure(new InterruptedException(JobKilledException.MESSAGE));
                break;
            }
            try {
                Engine.DeleteResult deleteResult = indexShard.applyDeleteOperationOnPrimary(
                    item.version(),
                    item.id(),
                    VersionType.INTERNAL,
                    item.seqNo(),
                    item.primaryTerm()
                );
                translogLocation = deleteResult.getTranslogLocation();
                Exception failure = deleteResult.getFailure();
                if (debugEnabled) {
                    logResult("primary", request.shardId(), item.id(), deleteResult);
                }
                if (failure == null) {
                    Item resultItem = new Item(
                        item.id(),
                        deleteResult.getSeqNo(),
                        deleteResult.getTerm(),
                        deleteResult.getVersion()
                    );
                    replicaRequest.add(location, resultItem);
                    if (deleteResult.isFound()) {
                        shardResponse.add(location);
                    } else {
                        var ex = new DocumentMissingException(indexShard.shardId(), Constants.DEFAULT_MAPPING_TYPE, item.id());
                        shardResponse.add(location, item.id(), ex, false);
                    }
                } else {
                    shardResponse.add(
                        location,
                        item.id(),
                        failure,
                        (failure instanceof VersionConflictEngineException)
                    );
                }
            } catch (Exception e) {
                if (retryPrimaryException(e)) {
                    throw Exceptions.toRuntimeException(e);
                } else {
                    if (debugEnabled) {
                        logger.debug("shardId={} failed to execute delete for id={}: {}",
                                     request.shardId(), item.id(), e);
                    }
                    shardResponse.add(
                        location,
                        item.id(),
                        e,
                        (e instanceof VersionConflictEngineException)
                    );
                }
            }
        }
        return new WritePrimaryResult<>(replicaRequest, shardResponse, translogLocation, null, indexShard);
    }

    private void logResult(String origin,
                           ShardId shardId,
                           String id,
                           Engine.DeleteResult deleteResult) {
        logger.debug(
            "shardId={} delete {} op id={} failure={} seqNo={} found={}",
            shardId,
            origin,
            id,
            deleteResult.getFailure(),
            deleteResult.getSeqNo(),
            deleteResult.isFound()
        );
    }

    @Override
    protected WriteReplicaResult processRequestItemsOnReplica(IndexShard indexShard, ShardDeleteRequest request) throws IOException {
        Translog.Location translogLocation = null;
        boolean traceEnabled = logger.isTraceEnabled();
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
                if (traceEnabled) {
                    logResult("replica", request.shardId(), item.id(), deleteResult);
                }
            }
        }
        return new WriteReplicaResult(translogLocation, null, indexShard);
    }
}
