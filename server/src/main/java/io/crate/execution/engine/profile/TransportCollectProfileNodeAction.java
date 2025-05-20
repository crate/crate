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

package io.crate.execution.engine.profile;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.NodeRequest;
import io.crate.execution.support.Transports;

/**
 * Transport action to collect profiling results from the {@link RootTask}.
 *
 * Profiling of the Task is enabled when performing an <code>EXPLAIN ANALYZE</code> statements.
 * EXPLAIN ANALYZE is done in a 2-phase execution:
 *
 *   * In the first step the profiled statement is executed and the measurements are written to the Task.
 *   * In the second step, these measurements are collected from the JobExecutionContexts on each node. This transport
 *     action is used to perform the collect operation.
 *
 */
@Singleton
public class TransportCollectProfileNodeAction extends TransportAction<NodeRequest<CollectProfileRequest>, NodeCollectProfileResponse> {

    private final Transports transports;
    private final TasksService tasksService;

    @Inject
    public TransportCollectProfileNodeAction(TransportService transportService,
                                             Transports transports,
                                             TasksService tasksService) {
        super(CollectProfileNodeAction.NAME);
        this.transports = transports;
        this.tasksService = tasksService;

        transportService.registerRequestHandler(
            CollectProfileNodeAction.NAME,
            ThreadPool.Names.SEARCH,
            true,
            false,
            CollectProfileRequest::new,
            new NodeActionRequestHandler<>(this::nodeOperation)
        );
    }

    private CompletableFuture<NodeCollectProfileResponse> nodeOperation(CollectProfileRequest request) {
        return collectExecutionTimesAndFinishContext(request.jobId()).thenApply(NodeCollectProfileResponse::new);
    }

    /**
     * @return a future that is completed with a map of unique subcontext names (id+name) and their execution times in ms
     */
    public CompletableFuture<Map<String, Object>> collectExecutionTimesAndFinishContext(UUID jobId) {
        RootTask rootTask = tasksService.getTaskOrNull(jobId);
        if (rootTask == null) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        } else {
            return rootTask.finishProfiling();
        }
    }

    @Override
    public void doExecute(NodeRequest<CollectProfileRequest> request, ActionListener<NodeCollectProfileResponse> listener) {
        transports.sendRequest(
            CollectProfileNodeAction.NAME,
            request.nodeId(),
            request.innerRequest(),
            listener,
            new ActionListenerResponseHandler<>(CollectProfileNodeAction.NAME, listener, NodeCollectProfileResponse::new)
        );
    }
}
