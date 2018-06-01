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

package io.crate.execution.engine.profile;

import io.crate.action.FutureActionListener;
import io.crate.execution.jobs.JobContextService;
import io.crate.execution.jobs.JobExecutionContext;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.Transports;
import io.crate.profile.ProfilingResult;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Transport action to collect profiling results from the {@link JobExecutionContext}.
 *
 * Profiling of the JobExecutionContext is enabled when performing an <code>EXPLAIN ANALYZE</code> statements.
 * EXPLAIN ANALYZE is done in a 2-phase execution:
 *
 *   * In the first step the profiled statement is executed and the measurements are written to the JobExecutionContext.
 *   * In the second step, these measurements are collected from the JobExecutionContexts on each node. This transport
 *     action is used to perform the collect operation.
 *
 */
@Singleton
public class TransportCollectProfileNodeAction implements NodeAction<NodeCollectProfileRequest, NodeCollectProfileResponse> {

    private static final String TRANSPORT_ACTION = "crate/sql/node/profile/collect";
    private static final String EXECUTOR = ThreadPool.Names.SEARCH;

    private final Transports transports;
    private final JobContextService jobContextService;

    @Inject
    public TransportCollectProfileNodeAction(TransportService transportService,
                                             Transports transports,
                                             JobContextService jobContextService) {
        this.transports = transports;
        this.jobContextService = jobContextService;

        transportService.registerRequestHandler(
            TRANSPORT_ACTION,
            NodeCollectProfileRequest::new,
            EXECUTOR,
            true,
            false,
            new NodeActionRequestHandler<>(this)
        );
    }

    @Override
    public CompletableFuture<NodeCollectProfileResponse> nodeOperation(NodeCollectProfileRequest request) {
        return collectExecutionTimesAndFinishContext(request.jobId()).thenApply(NodeCollectProfileResponse::new);
    }

    /**
     * @return a future that is completed with a map of unique subcontext names (id+name) and their execution times in ms
     */
    public CompletableFuture<ProfilingResult> collectExecutionTimesAndFinishContext(UUID jobId) {
        JobExecutionContext context = jobContextService.getContextOrNull(jobId);
        if (context == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return context.finishProfiling();
        }
    }

    public void execute(String nodeId,
                        NodeCollectProfileRequest request,
                        FutureActionListener<NodeCollectProfileResponse, ProfilingResult> listener) {
        transports.sendRequest(TRANSPORT_ACTION, nodeId, request, listener,
            new ActionListenerResponseHandler<>(listener, NodeCollectProfileResponse::new));
    }
}
