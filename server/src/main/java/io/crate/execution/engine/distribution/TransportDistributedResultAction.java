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

import io.crate.user.User;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.JobKilledException;
import io.crate.exceptions.TaskMissing;
import io.crate.execution.jobs.DownstreamRXTask;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.PageResultListener;
import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillJobsRequest;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.Transports;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class TransportDistributedResultAction implements NodeAction<DistributedResultRequest, DistributedResultResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportDistributedResultAction.class);
    private static final String DISTRIBUTED_RESULT_ACTION = "internal:crate:sql/node/merge";

    private final Transports transports;
    private final TasksService tasksService;
    private final ScheduledExecutorService scheduler;
    private final ClusterService clusterService;
    private final TransportKillJobsNodeAction killJobsAction;
    private final BackoffPolicy backoffPolicy;

    @Inject
    public TransportDistributedResultAction(Transports transports,
                                            TasksService tasksService,
                                            ThreadPool threadPool,
                                            TransportService transportService,
                                            ClusterService clusterService,
                                            TransportKillJobsNodeAction killJobsAction,
                                            Settings settings) {
        this(transports,
            tasksService,
            threadPool,
            transportService,
            clusterService,
            killJobsAction,
            BackoffPolicy.exponentialBackoff());
    }

    @VisibleForTesting
    TransportDistributedResultAction(Transports transports,
                                     TasksService tasksService,
                                     ThreadPool threadPool,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     TransportKillJobsNodeAction killJobsAction,
                                     BackoffPolicy backoffPolicy) {
        this.transports = transports;
        this.tasksService = tasksService;
        scheduler = threadPool.scheduler();
        this.clusterService = clusterService;
        this.killJobsAction = killJobsAction;
        this.backoffPolicy = backoffPolicy;

        transportService.registerRequestHandler(
            DISTRIBUTED_RESULT_ACTION,
            ThreadPool.Names.SAME, // <- we will dispatch later at the nodeOperation on non failures
            DistributedResultRequest::new,
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
            return CompletableFuture.failedFuture(new IllegalStateException(String.format(Locale.ENGLISH,
                                                                                          "Found execution rootTask for %d but it's not a downstream rootTask", request.executionPhaseId()), e));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }

        PageBucketReceiver pageBucketReceiver = rxTask.getBucketReceiver(request.executionPhaseInputId());
        if (pageBucketReceiver == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(String.format(Locale.ENGLISH,
                                                                                          "Couldn't find BucketReceiver for input %d", request.executionPhaseInputId())));
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
                             request.jobId(), delay.getMillis());
            }
            NodeOperationRunnable operationRunnable = new NodeOperationRunnable(request, retryDelay);
            scheduler.schedule(operationRunnable::run, delay.getMillis(), TimeUnit.MILLISECONDS);
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
            KillJobsRequest killRequest = new KillJobsRequest(
                List.of(request.jobId()),
                User.CRATE_USER.name(),
                "Received data for job=" + request.jobId() + " but there is no job context present. " +
                "This can happen due to bad network latency or if individual nodes are unresponsive due to high load"
            );
            killJobsAction.broadcast(killRequest, new ActionListener<>() {
                @Override
                public void onResponse(Long numKilled) {
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.debug("Could not kill " + request.jobId(), e);
                }
            }, excludedNodeIds);
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
