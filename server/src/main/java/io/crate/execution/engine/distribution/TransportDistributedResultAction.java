/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.distribution;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.JobKilledException;
import io.crate.exceptions.TaskMissing;
import io.crate.execution.jobs.DownstreamRXTask;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.PageResultListener;
import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillJobsNodeAction;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.support.ActionExecutor;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.NodeRequest;
import io.crate.execution.support.Transports;
import io.crate.role.Role;


public class TransportDistributedResultAction extends TransportAction<NodeRequest<DistributedResultRequest>, DistributedResultResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportDistributedResultAction.class);

    private final Transports transports;
    private final TasksService tasksService;
    private final ScheduledExecutorService scheduler;
    private final ClusterService clusterService;
    private final ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction;
    private final BackoffPolicy backoffPolicy;

    @Inject
    public TransportDistributedResultAction(Transports transports,
                                            TasksService tasksService,
                                            ThreadPool threadPool,
                                            TransportService transportService,
                                            ClusterService clusterService,
                                            Node node) {
        this(transports,
            tasksService,
            threadPool,
            transportService,
            clusterService,
            req -> node.client().execute(KillJobsNodeAction.INSTANCE, req),
            BackoffPolicy.exponentialBackoff());
    }

    @VisibleForTesting
    TransportDistributedResultAction(Transports transports,
                                     TasksService tasksService,
                                     ThreadPool threadPool,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction,
                                     BackoffPolicy backoffPolicy) {
        super(DistributedResultAction.NAME);
        this.transports = transports;
        this.tasksService = tasksService;
        scheduler = threadPool.scheduler();
        this.clusterService = clusterService;
        this.killNodeAction = killNodeAction;
        this.backoffPolicy = backoffPolicy;

        NodeAction<DistributedResultRequest, DistributedResultResponse> nodeAction = this::nodeOperation;
        transportService.registerRequestHandler(
            DistributedResultAction.NAME,
            ThreadPool.Names.SAME, // <- we will dispatch later at the nodeOperation on non failures
            true,
            // Don't trip breaker on transport layer, but instead depend on ram-accounting in PageBucketReceivers
            // We need to always handle requests to avoid jobs from getting stuck.
            // (If we receive a request, but don't handle it, a task would remain open indefinitely)
            false,
            DistributedResultRequest::new,
            new NodeActionRequestHandler<>(nodeAction));
    }

    @Override
    protected void doExecute(NodeRequest<DistributedResultRequest> request, ActionListener<DistributedResultResponse> listener) {
        transports.sendRequest(
            DistributedResultAction.NAME,
            request.nodeId(),
            request.innerRequest(),
            listener,
            new ActionListenerResponseHandler<>(listener, DistributedResultResponse::new)
        );
    }

    CompletableFuture<DistributedResultResponse> nodeOperation(DistributedResultRequest request) {
        return nodeOperation(request, null);
    }

    private CompletableFuture<DistributedResultResponse> nodeOperation(final DistributedResultRequest request,
                                                                       @Nullable Iterator<TimeValue> retryDelay) {
        RootTask rootTask = tasksService.getTaskOrNull(request.jobId());
        if (rootTask == null) {
            if (tasksService.recentlyFailed(request.jobId())) {
                return CompletableFuture.failedFuture(JobKilledException.of(
                    "Received result for job=" + request.jobId() + " but there is no context for this job due to a failure during the setup."));
            } else {
                return retryOrFailureResponse(request, retryDelay);
            }
        }

        DownstreamRXTask rxTask;
        try {
            rxTask = rootTask.getTask(request.executionPhaseId());
        } catch (ClassCastException e) {
            return CompletableFuture.failedFuture(
                new IllegalStateException(
                    String.format(
                        Locale.ENGLISH,
                        "Found execution rootTask for %d but it's not a downstream rootTask",
                        request.executionPhaseId()), e));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }

        PageBucketReceiver pageBucketReceiver = rxTask.getBucketReceiver(request.executionPhaseInputId());
        if (pageBucketReceiver == null) {
            return CompletableFuture.failedFuture(
                new IllegalStateException(
                    String.format(
                        Locale.ENGLISH,
                        "Couldn't find BucketReceiver for input %d",
                        request.executionPhaseInputId())));
        }

        Throwable throwable = request.throwable();
        if (throwable == null) {
            SendResponsePageResultListener pageResultListener = new SendResponsePageResultListener();
            pageBucketReceiver.setBucket(
                request.bucketIdx(),
                request.readRows(pageBucketReceiver.streamers()),
                request.isLast(),
                pageResultListener
            );
            return pageResultListener.future;
        } else {
            pageBucketReceiver.kill(throwable);
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
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("scheduling retry to start node operation for jobId: {} in {}ms",
                             request.jobId(), delay.millis());
            }
            NodeOperationRunnable operationRunnable = new NodeOperationRunnable(request, retryDelay);
            scheduler.schedule(operationRunnable::run, delay.millis(), TimeUnit.MILLISECONDS);
            return operationRunnable;
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Received a result for job={} but couldn't find a RootTask for it", request.jobId());
            }
            List<String> excludedNodeIds = Collections.singletonList(clusterService.localNode().getId());
            /* The upstream (DistributingConsumer) forwards failures to other downstreams and eventually considers its job done.
             * But it cannot inform the handler-merge about a failure because the JobResponse is sent eagerly.
             *
             * The handler local-merge would get stuck if not all its upstreams send their requests, so we need to invoke
             * a kill to make sure that doesn't happen.
             */
            KillJobsNodeRequest killRequest = new KillJobsNodeRequest(
                excludedNodeIds,
                List.of(request.jobId()),
                Role.CRATE_USER.name(),
                "Received data for job=" + request.jobId() + " but there is no job context present. " +
                "This can happen due to bad network latency or if individual nodes are unresponsive due to high load"
            );
            killNodeAction
                .execute(killRequest)
                .whenComplete(
                    (resp, t) -> {
                        if (t != null) {
                            LOGGER.debug("Could not kill " + request.jobId(), t);
                        }
                    }
                );
            return CompletableFuture.failedFuture(new TaskMissing(TaskMissing.Type.ROOT, request.jobId()));
        }
    }

    private static class SendResponsePageResultListener implements PageResultListener {
        private final CompletableFuture<DistributedResultResponse> future = new CompletableFuture<>();

        @Override
        public void needMore(boolean needMore) {
            LOGGER.trace("sending needMore response, need more? {}", needMore);
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
