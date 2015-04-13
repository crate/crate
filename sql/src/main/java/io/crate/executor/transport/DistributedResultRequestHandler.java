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

package io.crate.executor.transport;

import io.crate.executor.transport.distributed.DistributedRequestContextManager;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.PageDownstreamContext;
import io.crate.operation.PageConsumeListener;
import io.crate.planner.node.ExecutionNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;

import javax.annotation.Nullable;
import java.io.IOException;

public class DistributedResultRequestHandler extends BaseTransportRequestHandler<DistributedResultRequest> {

    private static final ESLogger LOGGER = Loggers.getLogger(DistributedResultRequestHandler.class);

    private final JobContextService jobContextService;

    @Deprecated
    private final DistributedRequestContextManager contextManager;

    public DistributedResultRequestHandler(JobContextService jobContextService, DistributedRequestContextManager contextManager) {
        this.jobContextService = jobContextService;
        this.contextManager = contextManager;
    }

    @Override
    public DistributedResultRequest newInstance() {
        return new DistributedResultRequest();
    }

    @Override
    public void messageReceived(final DistributedResultRequest request, final TransportChannel channel) throws Exception {
        LOGGER.trace("DistributedResultRequestHandler: messageReceived");

        if (request.executionNodeId() == ExecutionNode.NO_EXECUTION_NODE) {
            fallback(request, channel);
            return;
        }
        final PageDownstreamContext pageDownstreamContext = getPageDownstreamContext(request, channel);
        if (pageDownstreamContext == null) return;

        Throwable throwable = request.throwable();
        if (throwable != null) {
            channel.sendResponse(new DistributedResultResponse(false));
            pageDownstreamContext.failure(throwable);
        } else {
            request.streamers(pageDownstreamContext.streamer());
            pageDownstreamContext.setBucket(request.bucketIdx(), request.rows(), request.isLast(), new PageConsumeListener() {
                @Override
                public void needMore() {
                    try {
                        if (pageDownstreamContext.allExhausted()) {
                            pageDownstreamContext.finish();
                            channel.sendResponse(new DistributedResultResponse(false));
                        } else {
                            channel.sendResponse(new DistributedResultResponse(!pageDownstreamContext.isExhausted(request.bucketIdx())));
                        }
                    } catch (IOException e) {
                        LOGGER.error("Could not send DistributedResultResponse", e);
                    }
                }

                @Override
                public void finish() {
                    try {
                        channel.sendResponse(new DistributedResultResponse(false));
                        pageDownstreamContext.finish();
                    } catch (IOException e) {
                        LOGGER.error("Could not send DistributedResultResponse", e);
                    }
                }
            });
        }
    }

    @Nullable
    private PageDownstreamContext getPageDownstreamContext(DistributedResultRequest request, TransportChannel channel) throws IOException {
        JobExecutionContext context = jobContextService.getContext(request.jobId());
        if (context == null) {
            channel.sendResponse(new IllegalStateException("JobContext is missing"));
            return null;
        }
        final PageDownstreamContext pageDownstreamContext = context.getPageDownstreamContext(request.executionNodeId());
        if (pageDownstreamContext == null) {
            channel.sendResponse(new IllegalStateException("JobExecutionContext is missing"));
            return null;
        }
        return pageDownstreamContext;
    }

    @Deprecated
    private void fallback(DistributedResultRequest request, TransportChannel channel) throws IOException {
        try {
            contextManager.addToContext(request);
            channel.sendResponse(new DistributedResultResponse(false));
        } catch (Exception ex) {
            channel.sendResponse(ex);
        }
    }

    @Override
    public String executor() {
        return ThreadPool.Names.SEARCH;
    }
}
