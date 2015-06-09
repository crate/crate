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

package io.crate.executor.transport.distributed;

import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.executor.transport.Transports;
import io.crate.jobs.*;
import io.crate.operation.PageResultListener;
import io.crate.planner.node.ExecutionNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class TransportDistributedResultAction implements NodeAction<DistributedResultRequest, DistributedResultResponse> {

    private static final ESLogger LOGGER = Loggers.getLogger(TransportDistributedResultAction.class);

    public final static String DISTRIBUTED_RESULT_ACTION = "crate/sql/node/merge/add_rows";
    private final static String EXECUTOR_NAME = ThreadPool.Names.SEARCH;

    private final Transports transports;
    private final JobContextService jobContextService;
    private final ScheduledExecutorService scheduler;

    @Inject
    public TransportDistributedResultAction(Transports transports,
                                            JobContextService jobContextService,
                                            ThreadPool threadPool,
                                            TransportService transportService) {
        this.transports = transports;
        this.jobContextService = jobContextService;
        scheduler = threadPool.scheduler();
        transportService.registerHandler(DISTRIBUTED_RESULT_ACTION, new NodeActionRequestHandler<DistributedResultRequest, DistributedResultResponse>(this) {
            @Override
            public DistributedResultRequest newInstance() {
                return new DistributedResultRequest();
            }
        });
    }

    public void pushResult(String node, DistributedResultRequest request, ActionListener<DistributedResultResponse> listener) {
        transports.executeLocalOrWithTransport(this, node, request, listener,
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
    public void nodeOperation(DistributedResultRequest request,
                              ActionListener<DistributedResultResponse> listener) {
        nodeOperation(request, listener, 0);
    }

    private void nodeOperation(final DistributedResultRequest request,
                               final ActionListener<DistributedResultResponse> listener,
                               final int retry) {
        if (request.executionNodeId() == ExecutionNode.NO_EXECUTION_NODE) {
            listener.onFailure(new IllegalStateException("request must contain a valid executionNodeId"));
            return;
        }
        JobExecutionContext context = jobContextService.getContextOrNull(request.jobId());
        if (context == null) {
            retryOrFailureResponse(request, listener, retry);
            return;
        }

        DownstreamExecutionSubContext executionContext;
        try {
            executionContext = context.getSubContextOrNull(request.executionNodeId());
        } catch (ClassCastException e) {
            listener.onFailure(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Found execution context for %d but it's not a downstream context", request.executionNodeId())));
            return;
        }
        if (executionContext == null) {
            // this is currently sometimes the case when upstreams send failures more than once
            listener.onFailure(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Couldn't find execution context for %d", request.executionNodeId())));
            return;
        }

        PageDownstreamContext pageDownstreamContext = executionContext.pageDownstreamContext(request.executionNodeInputId());
        if (pageDownstreamContext == null) {
            listener.onFailure(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Couldn't find pageDownstreamContext for input %d", request.executionNodeInputId())));
            return;
        }

        Throwable throwable = request.throwable();
        if (throwable == null) {
            request.streamers(pageDownstreamContext.streamer());
            pageDownstreamContext.setBucket(
                    request.bucketIdx(),
                    request.rows(),
                    request.isLast(),
                    new SendResponsePageResultListener(listener, request));
        } else {
            pageDownstreamContext.failure(request.bucketIdx(), throwable);
            listener.onResponse(new DistributedResultResponse(false));
        }
    }

    private void retryOrFailureResponse(DistributedResultRequest request,
                                        ActionListener<DistributedResultResponse> listener,
                                        int retry) {
        if (retry > 20) {
            listener.onFailure(new IllegalStateException(
                    String.format("Couldn't find JobExecutionContext for %s", request.jobId())));
        } else {
            scheduler.schedule(new NodeOperationRunnable(request, listener, retry), (retry + 1) * 2, TimeUnit.MILLISECONDS);
        }
    }

    private static class SendResponsePageResultListener implements PageResultListener {
        private final ActionListener<DistributedResultResponse> listener;
        private final DistributedResultRequest request;

        public SendResponsePageResultListener(ActionListener<DistributedResultResponse> listener, DistributedResultRequest request) {
            this.listener = listener;
            this.request = request;
        }

        @Override
        public void needMore(boolean needMore) {
            LOGGER.trace("sending needMore response, need more? {}", needMore);
            listener.onResponse(new DistributedResultResponse(needMore));
        }

        @Override
        public int buckedIdx() {
            return request.bucketIdx();
        }
    }

    private class NodeOperationRunnable implements Runnable {
        private final DistributedResultRequest request;
        private final ActionListener<DistributedResultResponse> listener;
        private final int retry;

        public NodeOperationRunnable(DistributedResultRequest request, ActionListener<DistributedResultResponse> listener, int retry) {
            this.request = request;
            this.listener = listener;
            this.retry = retry;
        }

        @Override
        public void run() {
            nodeOperation(request, listener, retry + 1);
        }
    }
}
