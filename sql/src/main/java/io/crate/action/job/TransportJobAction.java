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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.planner.node.ExecutionNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class TransportJobAction implements NodeAction<JobRequest, JobResponse> {

    public static final String ACTION_NAME = "crate/sql/job";
    private static final String EXECUTOR = ThreadPool.Names.SEARCH;

    private final Transports transports;
    private final JobContextService jobContextService;
    private final ContextPreparer contextPreparer;

    @Inject
    public TransportJobAction(TransportService transportService,
                              Transports transports,
                              JobContextService jobContextService,
                              ContextPreparer contextPreparer) {
        this.transports = transports;
        this.jobContextService = jobContextService;
        this.contextPreparer = contextPreparer;

        transportService.registerHandler(ACTION_NAME, new NodeActionRequestHandler<JobRequest, JobResponse>(this) {
            @Override
            public JobRequest newInstance() {
                return new JobRequest();
            }
        });
    }

    public void execute(String node, final JobRequest request, final ActionListener<JobResponse> listener) {
        transports.executeLocalOrWithTransport(this, node, request, listener,
                new DefaultTransportResponseHandler<JobResponse>(listener, EXECUTOR) {
                    @Override
                    public JobResponse newInstance() {
                        return new JobResponse();
                    }
                });
    }

    @Override
    public void nodeOperation(final JobRequest request, final ActionListener<JobResponse> actionListener) {
        JobExecutionContext.Builder contextBuilder = jobContextService.newBuilder(request.jobId());

        List<ListenableFuture<Bucket>> directResponseFutures = new ArrayList<>();
        for (ExecutionNode executionNode : request.executionNodes()) {
            ListenableFuture<Bucket> responseFuture = contextPreparer.prepare(request.jobId(), executionNode, contextBuilder);
            if (responseFuture != null) {
                directResponseFutures.add(responseFuture);
            }
        }

        JobExecutionContext context = jobContextService.createContext(contextBuilder);
        context.start();

        if (directResponseFutures.size() == 0) {
            actionListener.onResponse(new JobResponse());
        } else {
            Futures.addCallback(Futures.allAsList(directResponseFutures), new FutureCallback<List<Bucket>>() {
                @Override
                public void onSuccess(List<Bucket> buckets) {
                    actionListener.onResponse(new JobResponse(buckets));
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    actionListener.onFailure(t);
                }
            });
        }
    }

    @Override
    public String actionName() {
        return ACTION_NAME;
    }

    @Override
    public String executorName() {
        return EXECUTOR;
    }
}
