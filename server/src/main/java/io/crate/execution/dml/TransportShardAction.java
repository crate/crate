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

package io.crate.execution.dml;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.JobKilledException;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillAllListener;
import io.crate.execution.jobs.kill.KillableCallable;

/**
 * Base class for performing Crate-specific TransportWriteActions like Delete or Upsert.
 * @param <Request> The ShardRequest implementation including a replicated write request
 */
public abstract class TransportShardAction<
            Request extends ShardRequest<Request, Item>,
            ReplicaReq extends ShardRequest<ReplicaReq, ReplicaItem>,
            Item extends ShardRequest.Item,
            ReplicaItem extends ShardRequest.Item>
        extends TransportWriteAction<Request, ReplicaReq, ShardResponse>
        implements KillAllListener {

    private final ConcurrentHashMap<TaskId, KillableCallable<?>> activeOperations = new ConcurrentHashMap<>();
    private final TasksService tasksService;
    private final CircuitBreakerService circuitBreakerService;

    protected TransportShardAction(Settings settings,
                                   String actionName,
                                   TransportService transportService,
                                   ClusterService clusterService,
                                   IndicesService indicesService,
                                   TasksService tasksService,
                                   ThreadPool threadPool,
                                   ShardStateAction shardStateAction,
                                   CircuitBreakerService circuitBreakerService,
                                   Writeable.Reader<Request> reader,
                                   Writeable.Reader<ReplicaReq> replicaReader) {
        super(
            settings,
            actionName,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            reader,
            replicaReader,
            ThreadPool.Names.WRITE,
            false
        );
        this.circuitBreakerService = circuitBreakerService;
        this.tasksService = tasksService;
    }

    @Override
    protected ShardResponse newResponseInstance(StreamInput in) throws IOException {
        return new ShardResponse(in);
    }


    @Override
    protected void shardOperationOnPrimary(Request request,
                                           IndexShard primary,
                                           ActionListener<PrimaryResult<ReplicaReq, ShardResponse>> listener) {
        if (tasksService.recentlyFailed(request.jobId())) {
            listener.onFailure(JobKilledException.of(JobKilledException.MESSAGE));
            return;
        }
        try {
            KillableCallable<WritePrimaryResult<ReplicaReq, ShardResponse>> callable = new KillableCallable<>(request.jobId()) {

                @Override
                public WritePrimaryResult<ReplicaReq, ShardResponse> call() throws Exception {
                    return processRequestItems(primary, request, killed);
                }
            };
            listener.onResponse(withActiveOperation(request, callable, true));
        } catch (Throwable t) {
            listener.onFailure(Exceptions.toRuntimeException(t));
        }
    }

    @Override
    protected WriteReplicaResult shardOperationOnReplica(ReplicaReq replicaRequest, IndexShard indexShard) {
        KillableCallable<WriteReplicaResult> callable = new KillableCallable<>(replicaRequest.jobId()) {

            @Override
            public WriteReplicaResult call() throws Exception {
                return processRequestItemsOnReplica(indexShard, replicaRequest);
            }
        };
        return withActiveOperation(replicaRequest, callable, false);
    }

    <WrapperResponse> WrapperResponse withActiveOperation(ShardRequest<?, ?> request,
                                                          KillableCallable<WrapperResponse> callable,
                                                          boolean primary) {
        CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.QUERY);
        // Request is already accounted by the transport layer, but we account extra to account for the replica request copy
        long ramBytesUsed = request.ramBytesUsed();
        if (primary == false) {
            // Let's throw it before addEstimateBytesAndMaybeBreak, so that we don't falsely add bytes
            // and don't get false positive "CB not reset" errors.
            throw new CircuitBreakingException("dummy");
        }
        breaker.addEstimateBytesAndMaybeBreak(ramBytesUsed, "upsert request");


        TaskId id = request.getParentTask();
        activeOperations.put(id, callable);
        try {
            return callable.call();
        } catch (Throwable t) {
            throw Exceptions.toRuntimeException(t);
        } finally {
            activeOperations.remove(id, callable);
            breaker.addWithoutBreaking(- ramBytesUsed);
        }
    }

    @Override
    public void killAllJobs() {
        synchronized (activeOperations) {
            for (KillableCallable<?> callable : activeOperations.values()) {
                callable.kill(new InterruptedException(JobKilledException.MESSAGE));
            }
            activeOperations.clear();
        }
    }

    @Override
    public void killJob(UUID jobId) {
        synchronized (activeOperations) {
            Iterator<Entry<TaskId, KillableCallable<?>>> iterator = activeOperations.entrySet().iterator();
            while (iterator.hasNext()) {
                var entry = iterator.next();
                KillableCallable<?> killable = entry.getValue();
                if (killable.jobId().equals(jobId)) {
                    iterator.remove();
                    killable.kill(new InterruptedException(JobKilledException.MESSAGE));
                }
            }
        }
    }

    protected abstract WritePrimaryResult<ReplicaReq, ShardResponse> processRequestItems(IndexShard indexShard, Request request, AtomicBoolean killed) throws InterruptedException, IOException;

    protected abstract WriteReplicaResult processRequestItemsOnReplica(IndexShard indexShard, ReplicaReq replicaRequest) throws IOException;
}
