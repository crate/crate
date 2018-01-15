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

package io.crate.execution.engine.distribution;

import io.crate.concurrent.CompletableFutures;
import io.crate.exceptions.ContextMissingException;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.Transports;
import io.crate.execution.jobs.DownstreamExecutionSubContext;
import io.crate.execution.jobs.JobContextService;
import io.crate.execution.jobs.JobExecutionContext;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.operation.PageResultListener;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class TransportDistributedResultAction extends AbstractComponent implements NodeAction<DistributedResultRequest, DistributedResultResponse> {

    private static final String DISTRIBUTED_RESULT_ACTION = "crate/sql/node/merge/add_rows";

    private static final String EXECUTOR_NAME = ThreadPool.Names.SEARCH;

    private final Transports transports;
    private final JobContextService jobContextService;
    private final ScheduledExecutorService scheduler;
    private final Executor executor;

    @Inject
    public TransportDistributedResultAction(Transports transports,
                                            JobContextService jobContextService,
                                            ThreadPool threadPool,
                                            TransportService transportService,
                                            Settings settings) {
        super(settings);
        this.transports = transports;
        this.jobContextService = jobContextService;
        this.executor = threadPool.executor(EXECUTOR_NAME);
        scheduler = threadPool.scheduler();

        transportService.registerRequestHandler(DISTRIBUTED_RESULT_ACTION,
            DistributedResultRequest::new,
            ThreadPool.Names.SAME, // <- we will dispatch later at the nodeOperation on non failures
            new NodeActionRequestHandler<>(this));
    }

    void pushResult(String node, DistributedResultRequest request, ActionListener<DistributedResultResponse> listener) {
        transports.sendRequest(DISTRIBUTED_RESULT_ACTION, node, request, listener,
            new ActionListenerResponseHandler<>(listener, DistributedResultResponse::new));
    }

    @Override
    public CompletableFuture<DistributedResultResponse> nodeOperation(DistributedResultRequest request) {
        return nodeOperation(request, 0);
    }

    private CompletableFuture<DistributedResultResponse> nodeOperation(final DistributedResultRequest request, int retry) {
        JobExecutionContext context = jobContextService.getContextOrNull(request.jobId());
        if (context == null) {
            return retryOrFailureResponse(request, retry);
        }

        DownstreamExecutionSubContext executionContext;
        try {
            executionContext = context.getSubContext(request.executionPhaseId());
        } catch (ClassCastException e) {
            return CompletableFutures.failedFuture(
                new IllegalStateException(String.format(Locale.ENGLISH,
                    "Found execution context for %d but it's not a downstream context", request.executionPhaseId()), e));
        } catch (Throwable t) {
            return CompletableFutures.failedFuture(t);
        }

        PageBucketReceiver pageBucketReceiver = executionContext.getBucketReceiver(request.executionPhaseInputId());
        if (pageBucketReceiver == null) {
            return CompletableFutures.failedFuture(new IllegalStateException(String.format(Locale.ENGLISH,
                "Couldn't find BucketReciever for input %d", request.executionPhaseInputId())));
        }

        Throwable throwable = request.throwable();
        if (throwable == null) {
            request.streamers(pageBucketReceiver.streamers());
            SendResponsePageResultListener pageResultListener = new SendResponsePageResultListener();
            try {
                executor.execute(() -> pageBucketReceiver.setBucket(
                    request.bucketIdx(),
                    request.rows(),
                    request.isLast(),
                    pageResultListener));
                return pageResultListener.future;
            } catch (EsRejectedExecutionException e) {
                pageBucketReceiver.failure(request.bucketIdx(), e);
                return CompletableFuture.completedFuture(new DistributedResultResponse(false));
            }
        } else {
            if (request.isKilled()) {
                pageBucketReceiver.killed(request.bucketIdx(), throwable);
            } else {
                pageBucketReceiver.failure(request.bucketIdx(), throwable);
            }
            return CompletableFuture.completedFuture(new DistributedResultResponse(false));
        }
    }

    private CompletableFuture<DistributedResultResponse> retryOrFailureResponse(DistributedResultRequest request,
                                                                                int retry) {

        if (retry > 20) {
            return CompletableFutures.failedFuture(
                new ContextMissingException(ContextMissingException.ContextType.JOB_EXECUTION_CONTEXT, request.jobId()));
        } else {
            int delay = (retry + 1) * 2;
            if (logger.isTraceEnabled()) {
                logger.trace("scheduling retry #{} to start node operation for jobId: {} in {}ms",
                    retry, request.jobId(), delay);
            }
            NodeOperationRunnable operationRunnable = new NodeOperationRunnable(request, retry);
            scheduler.schedule(operationRunnable::run, delay, TimeUnit.MILLISECONDS);
            return operationRunnable;
        }
    }

    private class SendResponsePageResultListener implements PageResultListener {
        private final CompletableFuture<DistributedResultResponse> future = new CompletableFuture<>();

        @Override
        public void needMore(boolean needMore) {
            logger.trace("sending needMore response, need more? {}", needMore);
            future.complete(new DistributedResultResponse(needMore));
        }
    }

    private class NodeOperationRunnable extends CompletableFuture<DistributedResultResponse> {
        private final DistributedResultRequest request;
        private final int retry;

        NodeOperationRunnable(DistributedResultRequest request, int retry) {
            this.request = request;
            this.retry = retry;
        }

        public CompletableFuture<DistributedResultResponse> run() {
            return nodeOperation(request, retry + 1).whenComplete((r, f) -> {
                if (f == null) {
                    complete(r);
                } else {
                    completeExceptionally(f);
                }
            });
        }
    }
}
