/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.merge;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.executor.transport.Transports;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.PageDownstreamContext;
import io.crate.operation.PageConsumeListener;
import io.crate.planner.node.ExecutionNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;


public class TransportDistributedResultAction implements NodeAction<DistributedResultRequest, DistributedResultResponse> {

    private static final ESLogger LOGGER = Loggers.getLogger(TransportDistributedResultAction.class);

    public final static String DISTRIBUTED_RESULT_ACTION = "crate/sql/node/merge/add_rows";
    private final static String EXECUTOR_NAME = ThreadPool.Names.SEARCH;

    private final Transports transports;
    private final JobContextService jobContextService;

    @Inject
    public TransportDistributedResultAction(Transports transports,
                                            JobContextService jobContextService,
                                            TransportService transportService) {
        this.transports = transports;
        this.jobContextService = jobContextService;
        transportService.registerHandler(DISTRIBUTED_RESULT_ACTION, new NodeActionRequestHandler<DistributedResultRequest, DistributedResultResponse>(this) {
                    @Override
                    public DistributedResultRequest newInstance() {
                        return new DistributedResultRequest();
                    }
                });
    }

    public void pushResult(DiscoveryNode node, DistributedResultRequest request, ActionListener<DistributedResultResponse> listener) {
        transports.executeLocalOrWithTransport(this, node.id(), request, listener,
                new DefaultTransportResponseHandler<DistributedResultResponse>(listener, EXECUTOR_NAME) {
                    @Override
                    public DistributedResultResponse newInstance() {
                        return new DistributedResultResponse();
                    }
                });
    }

    @Override
    public String actionName() {
        return DISTRIBUTED_RESULT_ACTION;
    }

    @Override
    public String executorName() {
        return EXECUTOR_NAME;
    }

    @Override
    public void nodeOperation(DistributedResultRequest request, ActionListener<DistributedResultResponse> listener) {
        if (request.executionNodeId() == ExecutionNode.NO_EXECUTION_NODE) {
            listener.onFailure(new IllegalStateException("request must contain a valid executionNodeId"));
            return;
        }
        JobExecutionContext context = jobContextService.getOrCreateContext(request.jobId());
        ListenableFuture<PageDownstreamContext> pageDownstreamContextFuture = context.getPageDownstreamContext(request.executionNodeId());
        Futures.addCallback(pageDownstreamContextFuture, new PageDownstreamContextFutureCallback(request, listener));
    }

    private static class PageDownstreamContextFutureCallback implements FutureCallback<PageDownstreamContext> {
        private final DistributedResultRequest request;
        private final ActionListener<DistributedResultResponse> listener;

        public PageDownstreamContextFutureCallback(DistributedResultRequest request, ActionListener<DistributedResultResponse> listener) {
            this.request = request;
            this.listener = listener;
        }

        @Override
        public void onSuccess(final PageDownstreamContext pageDownstreamContext) {
            Throwable throwable = request.throwable();
            if (throwable != null) {
                listener.onResponse(new DistributedResultResponse(false));
                pageDownstreamContext.failure(throwable);
            } else {
                request.streamers(pageDownstreamContext.streamer());
                pageDownstreamContext.setBucket(
                        request.bucketIdx(),
                        request.rows(),
                        request.isLast(),
                        new DownstreamContextPageConsumeListener(pageDownstreamContext));
            }
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
            listener.onFailure(t);
        }

        private class DownstreamContextPageConsumeListener implements PageConsumeListener {
            private final PageDownstreamContext pageDownstreamContext;

            public DownstreamContextPageConsumeListener(PageDownstreamContext pageDownstreamContext) {
                this.pageDownstreamContext = pageDownstreamContext;
            }

            @Override
            public void needMore() {
                if (pageDownstreamContext.allExhausted()) {
                    LOGGER.trace("sending needMore response, upstream don't have more!");
                    listener.onResponse(new DistributedResultResponse(false));
                    pageDownstreamContext.finish();
                } else {
                    boolean needMore = !pageDownstreamContext.isExhausted(request.bucketIdx());
                    LOGGER.trace("sending needMore response, need more? {}", needMore);
                    listener.onResponse(new DistributedResultResponse(needMore));
                }
            }

            @Override
            public void finish() {
                LOGGER.trace("sending finish response");
                listener.onResponse(new DistributedResultResponse(false));
                pageDownstreamContext.finish();
            }
        }
    }
}
