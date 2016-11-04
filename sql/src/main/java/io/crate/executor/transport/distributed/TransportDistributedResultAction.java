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

import io.crate.exceptions.ContextMissingException;
import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.executor.transport.Transports;
import io.crate.jobs.DownstreamExecutionSubContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.PageBucketReceiver;
import io.crate.operation.PageResultListener;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class TransportDistributedResultAction extends AbstractComponent implements NodeAction<DistributedResultRequest, DistributedResultResponse> {

    public static final String DISTRIBUTED_RESULT_ACTION = "crate/sql/node/merge/add_rows";

    private static final String EXECUTOR_NAME = ThreadPool.Names.SEARCH;

    private final Transports transports;
    private final JobContextService jobContextService;
    private final ScheduledExecutorService scheduler;

    @Inject
    public TransportDistributedResultAction(Transports transports,
                                            JobContextService jobContextService,
                                            ThreadPool threadPool,
                                            TransportService transportService,
                                            Settings settings) {
        super(settings);
        this.transports = transports;
        this.jobContextService = jobContextService;
        scheduler = threadPool.scheduler();

        transportService.registerRequestHandler(DISTRIBUTED_RESULT_ACTION,
            DistributedResultRequest::new,
            ThreadPool.Names.GENERIC,
            new NodeActionRequestHandler<DistributedResultRequest, DistributedResultResponse>(this) {});
    }

    public void pushResult(String node, DistributedResultRequest request, ActionListener<DistributedResultResponse> listener) {
        transports.sendRequest(DISTRIBUTED_RESULT_ACTION, node, request, listener,
            new DefaultTransportResponseHandler<DistributedResultResponse>(listener, EXECUTOR_NAME) {
                @Override
                public DistributedResultResponse newInstance() {
                    return new DistributedResultResponse();
                }
            });
    }

    @Override
    public void nodeOperation(DistributedResultRequest request,
                              ActionListener<DistributedResultResponse> listener) {
        nodeOperation(request, listener, 0);
    }

    private void nodeOperation(final DistributedResultRequest request,
                               final ActionListener<DistributedResultResponse> listener,
                               final int retry) {
        JobExecutionContext context = jobContextService.getContextOrNull(request.jobId());
        if (context == null) {
            retryOrFailureResponse(request, listener, retry);
            return;
        }

        DownstreamExecutionSubContext executionContext;
        try {
            executionContext = context.getSubContext(request.executionPhaseId());
        } catch (ClassCastException e) {
            listener.onFailure(new IllegalStateException(String.format(Locale.ENGLISH,
                "Found execution context for %d but it's not a downstream context", request.executionPhaseId()), e));
            return;
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        PageBucketReceiver pageBucketReceiver = executionContext.getBucketReceiver(request.executionPhaseInputId());
        if (pageBucketReceiver == null) {
            listener.onFailure(new IllegalStateException(String.format(Locale.ENGLISH,
                "Couldn't find BucketReciever for input %d", request.executionPhaseInputId())));
            return;
        }

        Throwable throwable = request.throwable();
        if (throwable == null) {
            request.streamers(pageBucketReceiver.streamers());
            pageBucketReceiver.setBucket(
                request.bucketIdx(),
                request.rows(),
                request.isLast(),
                new SendResponsePageResultListener(listener));
        } else {
            if (request.isKilled()) {
                pageBucketReceiver.killed(request.bucketIdx(), throwable);
            } else {
                pageBucketReceiver.failure(request.bucketIdx(), throwable);
            }
            listener.onResponse(new DistributedResultResponse(false));
        }
    }

    private void retryOrFailureResponse(DistributedResultRequest request,
                                        ActionListener<DistributedResultResponse> listener,
                                        int retry) {

        if (retry > 20) {
            listener.onFailure(new ContextMissingException(ContextMissingException.ContextType.JOB_EXECUTION_CONTEXT, request.jobId()));
        } else {
            int delay = (retry + 1) * 2;
            if (logger.isTraceEnabled()) {
                logger.trace("scheduling retry #{} to start node operation for jobId: {} in {}ms",
                    retry, request.jobId(), delay);
            }
            scheduler.schedule(new NodeOperationRunnable(request, listener, retry), delay, TimeUnit.MILLISECONDS);
        }
    }

    private class SendResponsePageResultListener implements PageResultListener {
        private final ActionListener<DistributedResultResponse> listener;

        public SendResponsePageResultListener(ActionListener<DistributedResultResponse> listener) {
            this.listener = listener;
        }

        @Override
        public void needMore(boolean needMore) {
            logger.trace("sending needMore response, need more? {}", needMore);
            listener.onResponse(new DistributedResultResponse(needMore));
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
