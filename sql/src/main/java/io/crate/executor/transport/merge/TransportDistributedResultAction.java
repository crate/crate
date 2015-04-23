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
import io.crate.operation.PageResultListener;
import io.crate.planner.node.ExecutionNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
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
    public void nodeOperation(final DistributedResultRequest request,
                              final ActionListener<DistributedResultResponse> listener) {
        if (request.executionNodeId() == ExecutionNode.NO_EXECUTION_NODE) {
            listener.onFailure(new IllegalStateException("request must contain a valid executionNodeId"));
            return;
        }
        JobExecutionContext context = jobContextService.getContext(request.jobId());
        if (context == null) {
            // TODO: stop retry at some point and quit
            scheduler.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            nodeOperation(request, listener);
                        }
                    }, 5, TimeUnit.MILLISECONDS);
            return;
        }

        PageDownstreamContext pageDownstreamContext = context.getPageDownstreamContext(request.executionNodeId());
        if (pageDownstreamContext == null) {
            listener.onFailure(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Couldn't find pageDownstreamContext for %d", request.executionNodeId())));
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
}
