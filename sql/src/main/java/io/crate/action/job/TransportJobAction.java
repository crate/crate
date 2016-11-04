/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.action.job;

import io.crate.concurrent.CompletableFutures;
import io.crate.data.Bucket;
import io.crate.exceptions.SQLExceptions;
import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Singleton
public class TransportJobAction implements NodeAction<JobRequest, JobResponse> {

    public static final String ACTION_NAME = "crate/sql/job";
    private static final String EXECUTOR = ThreadPool.Names.SEARCH;

    private final IndicesService indicesService;
    private final Transports transports;
    private final JobContextService jobContextService;
    private final ContextPreparer contextPreparer;

    @Inject
    public TransportJobAction(TransportService transportService,
                              IndicesService indicesService,
                              Transports transports,
                              JobContextService jobContextService,
                              ContextPreparer contextPreparer) {
        this.indicesService = indicesService;
        this.transports = transports;
        this.jobContextService = jobContextService;
        this.contextPreparer = contextPreparer;
        transportService.registerRequestHandler(
            ACTION_NAME,
            JobRequest::new,
            EXECUTOR,
            new NodeActionRequestHandler<JobRequest, JobResponse>(this) {});
    }

    public void execute(String node, final JobRequest request, final ActionListener<JobResponse> listener) {
        transports.sendRequest(ACTION_NAME, node, request, listener,
            new DefaultTransportResponseHandler<JobResponse>(listener) {
                @Override
                public JobResponse newInstance() {
                    return new JobResponse();
                }
            });
    }

    @Override
    public void nodeOperation(final JobRequest request, final ActionListener<JobResponse> actionListener) {
        JobExecutionContext.Builder contextBuilder = jobContextService.newBuilder(request.jobId(), request.coordinatorNodeId());

        SharedShardContexts sharedShardContexts = new SharedShardContexts(indicesService);
        List<CompletableFuture<Bucket>> directResponseFutures = contextPreparer.prepareOnRemote(
            request.nodeOperations(), contextBuilder, sharedShardContexts);

        try {
            JobExecutionContext context = jobContextService.createContext(contextBuilder);
            context.start();
        } catch (Throwable t) {
            actionListener.onFailure((Exception) t);
            return;
        }

        if (directResponseFutures.size() == 0) {
            actionListener.onResponse(new JobResponse());
        } else {
            CompletableFutures.allAsList(directResponseFutures).whenComplete((buckets, t) -> {
                if (t == null) {
                    actionListener.onResponse(new JobResponse(buckets));
                } else {
                    actionListener.onFailure((Exception) SQLExceptions.unwrap(t));
                }
            });
        }
    }
}
