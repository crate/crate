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

package io.crate.execution.jobs.transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.common.concurrent.CompletableFutures;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.execution.jobs.InstrumentedIndexSearcher;
import io.crate.execution.jobs.JobSetup;
import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.NodeRequest;
import io.crate.execution.support.Transports;
import io.crate.profile.ProfilingContext;

@Singleton
public class TransportJobAction extends TransportAction<NodeRequest<JobRequest>, JobResponse> {

    private final IndicesService indicesService;
    private final Transports transports;
    private final TasksService tasksService;
    private final JobSetup jobSetup;

    @Inject
    public TransportJobAction(TransportService transportService,
                              IndicesService indicesService,
                              Transports transports,
                              TasksService tasksService,
                              JobSetup jobSetup) {
        super(JobAction.NAME);
        this.indicesService = indicesService;
        this.transports = transports;
        this.tasksService = tasksService;
        this.jobSetup = jobSetup;
        transportService.registerRequestHandler(
            JobAction.NAME,
            ThreadPool.Names.SEARCH,
            JobRequest::new,
            new NodeActionRequestHandler<>(this::nodeOperation));
    }

    @Override
    public void doExecute(NodeRequest<JobRequest> request, ActionListener<JobResponse> listener) {
        transports.sendRequest(
            JobAction.NAME,
            request.nodeId(),
            request.innerRequest(),
            listener,
            new ActionListenerResponseHandler<>(listener, JobResponse::new)
        );
    }

    private CompletableFuture<JobResponse> nodeOperation(final JobRequest request) {
        RootTask.Builder contextBuilder = tasksService.newBuilder(
            request.jobId(),
            request.sessionSettings().userName(),
            request.coordinatorNodeId(),
            Collections.emptySet()
        );

        SharedShardContexts sharedShardContexts = maybeInstrumentProfiler(request.enableProfiling(), contextBuilder);

        List<CompletableFuture<StreamBucket>> directResponseFutures = jobSetup.prepareOnRemote(
            request.sessionSettings(),
            request.nodeOperations(),
            contextBuilder,
            sharedShardContexts
        );

        CompletableFuture<Void> startFuture;
        try {
            RootTask context = tasksService.createTask(contextBuilder);
            startFuture = context.start();
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }

        if (directResponseFutures.size() == 0) {
            return startFuture.thenApply(ignored -> new JobResponse(List.of()));
        } else {
            return startFuture.thenCompose(ignored -> CompletableFutures.allAsList(directResponseFutures)).thenApply(JobResponse::new);
        }
    }

    private SharedShardContexts maybeInstrumentProfiler(boolean enableProfiling, RootTask.Builder contextBuilder) {
        if (enableProfiling) {
            var profilers = new ArrayList<QueryProfiler>();
            ProfilingContext profilingContext = new ProfilingContext(profilers);
            contextBuilder.profilingContext(profilingContext);

            return new SharedShardContexts(
                indicesService,
                indexSearcher -> {
                    var queryProfiler = new QueryProfiler();
                    profilers.add(queryProfiler);
                    return new InstrumentedIndexSearcher(indexSearcher, queryProfiler);
                }
            );
        } else {
            return new SharedShardContexts(indicesService, UnaryOperator.identity());
        }
    }
}
