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

import com.google.common.annotations.VisibleForTesting;
import io.crate.concurrent.CompletableFutures;
import io.crate.exceptions.ContextMissingException;
import io.crate.execution.jobs.DownstreamExecutionSubContext;
import io.crate.execution.jobs.JobContextService;
import io.crate.execution.jobs.JobExecutionContext;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.PageResultListener;
import io.crate.execution.jobs.kill.KillJobsRequest;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.Transports;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
    private final ClusterService clusterService;
    private final TransportKillJobsNodeAction killJobsAction;
    private final BackoffPolicy backoffPolicy;
    private final Executor executor;

    @Inject
    public TransportDistributedResultAction(Transports transports,
                                            JobContextService jobContextService,
                                            ThreadPool threadPool,
                                            TransportService transportService,
                                            ClusterService clusterService,
                                            TransportKillJobsNodeAction killJobsAction,
                                            Settings settings) {
        this(transports,
            jobContextService,
            threadPool,
            transportService,
            clusterService,
            killJobsAction,
            settings,
            BackoffPolicy.exponentialBackoff());
    }

    @VisibleForTesting
    TransportDistributedResultAction(Transports transports,
                                     JobContextService jobContextService,
                                     ThreadPool threadPool,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     TransportKillJobsNodeAction killJobsAction,
                                     Settings settings,
                                     BackoffPolicy backoffPolicy) {
        super(settings);
        this.transports = transports;
        this.jobContextService = jobContextService;
        this.executor = threadPool.executor(EXECUTOR_NAME);
        scheduler = threadPool.scheduler();
        this.clusterService = clusterService;
        this.killJobsAction = killJobsAction;
        this.backoffPolicy = backoffPolicy;

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
        return nodeOperation(request, null);
    }

    private CompletableFuture<DistributedResultResponse> nodeOperation(final DistributedResultRequest request,
                                                                       @Nullable Iterator<TimeValue> retryDelay) {
        JobExecutionContext context = jobContextService.getContextOrNull(request.jobId());
        if (context == null) {
            return retryOrFailureResponse(request, retryDelay);
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
                                                                                @Nullable Iterator<TimeValue> retryDelay) {

        if (retryDelay == null) {
            retryDelay = backoffPolicy.iterator();
        }
        if (retryDelay.hasNext()) {
            TimeValue delay = retryDelay.next();
            if (logger.isTraceEnabled()) {
                logger.trace("scheduling retry to start node operation for jobId: {} in {}ms",
                    request.jobId(), delay.getMillis());
            }
            NodeOperationRunnable operationRunnable = new NodeOperationRunnable(request, retryDelay);
            scheduler.schedule(operationRunnable::run, delay.getMillis(), TimeUnit.MILLISECONDS);
            return operationRunnable;
        } else {
            List<String> excludedNodeIds = Collections.singletonList(clusterService.localNode().getId());
            /* The upstream (DistributingConsumer) forwards failures to other downstreams and eventually considers its job done.
             * But it cannot inform the handler-merge about a failure because the JobResponse is sent eagerly.
             *
             * The handler local-merge would get stuck if not all its upstreams send their requests, so we need to invoke
             * a kill to make sure that doesn't happen.
             */
            killJobsAction.broadcast(new KillJobsRequest(Collections.singletonList(request.jobId())), new ActionListener<Long>() {
                @Override
                public void onResponse(Long numKilled) {
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("Could not kill " + request.jobId(), e);
                }
            }, excludedNodeIds);
            return CompletableFutures.failedFuture(
                new ContextMissingException(ContextMissingException.ContextType.JOB_EXECUTION_CONTEXT, request.jobId()));
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
        private final Iterator<TimeValue> retryDelay;

        NodeOperationRunnable(DistributedResultRequest request, Iterator<TimeValue> retryDelay) {
            this.request = request;
            this.retryDelay = retryDelay;
        }

        public CompletableFuture<DistributedResultResponse> run() {
            return nodeOperation(request, retryDelay).whenComplete((r, f) -> {
                if (f == null) {
                    complete(r);
                } else {
                    completeExceptionally(f);
                }
            });
        }
    }
}
