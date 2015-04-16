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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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

import javax.annotation.Nonnull;
import java.io.IOException;

public class DistributedResultRequestHandler extends BaseTransportRequestHandler<DistributedResultRequest> {

    private static final ESLogger LOGGER = Loggers.getLogger(DistributedResultRequestHandler.class);

    private final JobContextService jobContextService;

    public DistributedResultRequestHandler(JobContextService jobContextService) {
        this.jobContextService = jobContextService;
    }

    @Override
    public DistributedResultRequest newInstance() {
        return new DistributedResultRequest();
    }

    @Override
    public void messageReceived(final DistributedResultRequest request, final TransportChannel channel) throws Exception {
        LOGGER.trace("[{}] DistributedResultRequestHandler: messageReceived for executionNodeId: {}",
                request.jobId(), request.executionNodeId());

        if (request.executionNodeId() == ExecutionNode.NO_EXECUTION_NODE) {
            channel.sendResponse(new IllegalStateException("request must contain a valid executionNodeId"));
            return;
        }
        JobExecutionContext context = jobContextService.getOrCreateContext(request.jobId());
        ListenableFuture<PageDownstreamContext> pageDownstreamContextFuture = context.getPageDownstreamContext(request.executionNodeId());
        Futures.addCallback(pageDownstreamContextFuture, new PageDownstreamContextFutureCallback(request, channel));
    }

    @Override
    public String executor() {
        return ThreadPool.Names.SEARCH;
    }

    private static class PageDownstreamContextFutureCallback implements FutureCallback<PageDownstreamContext> {
        private final DistributedResultRequest request;
        private final TransportChannel channel;

        public PageDownstreamContextFutureCallback(DistributedResultRequest request, TransportChannel channel) {
            this.request = request;
            this.channel = channel;
        }

        @Override
        public void onSuccess(final PageDownstreamContext pageDownstreamContext) {
            Throwable throwable = request.throwable();
            if (throwable != null) {
                try {
                    channel.sendResponse(new DistributedResultResponse(false));
                } catch (IOException e) {
                    LOGGER.error("Could not send DistributedResultResponse", e);
                }
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
            try {
                channel.sendResponse(t);
            } catch (IOException e) {
                LOGGER.error("Could not send DistributedResultResponse", e);
            }
        }

        private class DownstreamContextPageConsumeListener implements PageConsumeListener {
            private final PageDownstreamContext pageDownstreamContext;

            public DownstreamContextPageConsumeListener(PageDownstreamContext pageDownstreamContext) {
                this.pageDownstreamContext = pageDownstreamContext;
            }

            @Override
            public void needMore() {
                try {
                    if (pageDownstreamContext.allExhausted()) {
                        LOGGER.trace("sending needMore response, upstream don't have more!");
                        channel.sendResponse(new DistributedResultResponse(false));
                        pageDownstreamContext.finish();
                    } else {
                        boolean needMore = !pageDownstreamContext.isExhausted(request.bucketIdx());
                        LOGGER.trace("sending needMore response, need more? {}", needMore);
                        channel.sendResponse(new DistributedResultResponse(needMore));
                    }
                } catch (IOException e) {
                    LOGGER.error("Could not send DistributedResultResponse", e);
                }
            }

            @Override
            public void finish() {
                LOGGER.trace("sending finish response");
                try {
                    channel.sendResponse(new DistributedResultResponse(false));
                    pageDownstreamContext.finish();
                } catch (IOException e) {
                    LOGGER.error("Could not send DistributedResultResponse", e);
                }
            }
        }
    }
}
