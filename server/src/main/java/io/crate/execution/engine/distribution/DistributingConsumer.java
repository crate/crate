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

import static io.crate.execution.engine.distribution.TransportDistributedResultAction.broadcastKill;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.unit.TimeValue;
import io.crate.data.BatchIterator;
import io.crate.data.Paging;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.JobKilledException;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.support.ActionExecutor;
import io.crate.execution.support.NodeRequest;

/**
 * Consumer which sends requests to downstream nodes every {@link #pageSize} rows.
 *
 * The rows from the source {@link BatchIterator} are "bucketed" using a {@link MultiBucketBuilder}. So a downstream
 * can either receive a part of the data or all data.
 *
 * Every time requests to the downstreams are made consumption of the source BatchIterator is stopped until a response
 * from all downstreams is received.
 */
public class DistributingConsumer implements RowConsumer {

    private static final Logger LOGGER = LogManager.getLogger(DistributingConsumer.class);
    private final Executor responseExecutor;
    private final UUID jobId;
    private final int targetPhaseId;
    private final byte inputId;
    private final int bucketIdx;
    private final ActionExecutor<NodeRequest<DistributedResultRequest>, DistributedResultResponse> distributedResultAction;
    private final int pageSize;
    private final StreamBucket[] buckets;
    private final List<Downstream> downstreams;
    private final boolean traceEnabled;
    private final CompletableFuture<Void> completionFuture;
    private final ThreadPool threadPool;

    @VisibleForTesting
    final long maxBytes;

    @VisibleForTesting
    final MultiBucketBuilder multiBucketBuilder;

    private volatile Throwable failure;

    private final ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction;
    private final String localNodeId;

    public DistributingConsumer(Executor responseExecutor,
                                UUID jobId,
                                MultiBucketBuilder multiBucketBuilder,
                                int targetPhaseId,
                                byte inputId,
                                int bucketIdx,
                                Collection<String> downstreamNodeIds,
                                ActionExecutor<NodeRequest<DistributedResultRequest>, DistributedResultResponse> distributedResultAction,
                                ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction,
                                String localNodeId,
                                int pageSize,
                                ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.traceEnabled = LOGGER.isTraceEnabled();
        this.responseExecutor = responseExecutor;
        this.jobId = jobId;
        this.multiBucketBuilder = multiBucketBuilder;
        this.targetPhaseId = targetPhaseId;
        this.inputId = inputId;
        this.bucketIdx = bucketIdx;
        this.distributedResultAction = distributedResultAction;
        this.killNodeAction = killNodeAction;
        this.localNodeId = localNodeId;
        this.pageSize = pageSize;
        this.buckets = new StreamBucket[downstreamNodeIds.size()];
        this.completionFuture = new CompletableFuture<>();
        downstreams = new ArrayList<>(downstreamNodeIds.size());
        for (String downstreamNodeId : downstreamNodeIds) {
            downstreams.add(new Downstream(downstreamNodeId));
        }
        assert downstreams.size() > 0 : "Must always have at least one downstream";
        this.maxBytes = Math.max(Paging.MAX_PAGE_BYTES / downstreams.size(), 2 * 1024 * 1024);
    }

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        if (failure == null) {
            consumeIt(iterator);
        } else {
            completionFuture.completeExceptionally(failure);
            forwardFailure(null, failure);
        }
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return completionFuture;
    }

    private void consumeIt(BatchIterator<Row> it) {
        try {
            while (it.moveNext()) {
                multiBucketBuilder.add(it.currentElement());
                if (multiBucketBuilder.size() >= pageSize || multiBucketBuilder.ramBytesUsed() >= maxBytes) {
                    forwardResults(it, false);
                    return;
                }
            }
            if (it.allLoaded()) {
                forwardResults(it, true);
            } else {
                it.loadNextBatch().whenComplete((_, t) -> {
                    if (t == null) {
                        consumeIt(it);
                    } else {
                        forwardFailure(it, t);
                    }
                });
            }
        } catch (Throwable t) {
            forwardFailure(it, t);
        }
    }

    private void forwardFailure(@Nullable final BatchIterator<Row> it, final Throwable f) {
        Throwable failure = SQLExceptions.unwrap(f); // make sure it's streamable
        AtomicInteger numActiveRequests = new AtomicInteger(downstreams.size());
        var builder = new DistributedResultRequest.Builder(jobId, targetPhaseId, inputId, bucketIdx, failure, false);
        for (int i = 0; i < downstreams.size(); i++) {
            Downstream downstream = downstreams.get(i);
            if (downstream.needsMoreData == false) {
                countdownAndMaybeCloseIt(numActiveRequests, it);
            } else {
                if (traceEnabled) {
                    LOGGER.trace("forwardFailure targetNode={} jobId={} targetPhase={}/{} bucket={} failure={}",
                                 downstream.nodeId, jobId, targetPhaseId, inputId, bucketIdx, failure);
                }
                NodeRequest<DistributedResultRequest> request = builder.build(downstream.nodeId);
                var responseHandler = new ResponseHandler(
                    downstream,
                    request,
                    it,
                    numActiveRequests,
                    true
                );
                distributedResultAction.execute(request).whenComplete(responseHandler);
            }
        }
    }


    private void countdownAndMaybeCloseIt(AtomicInteger numActiveRequests, @Nullable BatchIterator<?> it) {
        if (numActiveRequests.decrementAndGet() == 0) {
            if (it != null) {
                it.close();
                completionFuture.complete(null);
            }
        }
    }

    private void forwardResults(BatchIterator<Row> it, boolean isLast) {
        multiBucketBuilder.build(buckets);
        AtomicInteger numActiveRequests = new AtomicInteger(downstreams.size());
        for (int i = 0; i < downstreams.size(); i++) {
            Downstream downstream = downstreams.get(i);
            if (downstream.needsMoreData == false) {
                countdownAndMaybeContinue(it, numActiveRequests, true);
                continue;
            }
            if (traceEnabled) {
                LOGGER.trace("forwardResults targetNode={} jobId={} targetPhase={}/{} bucket={} isLast={}",
                             downstream.nodeId, jobId, targetPhaseId, inputId, bucketIdx, isLast);
            }
            NodeRequest<DistributedResultRequest> request = DistributedResultRequest.of(
                downstream.nodeId,
                jobId,
                targetPhaseId,
                inputId,
                bucketIdx,
                buckets[i],
                isLast
            );
            var responseHandler = new ResponseHandler(
                downstream,
                request,
                it,
                numActiveRequests,
                false
            );
            distributedResultAction.execute(request).whenComplete(responseHandler);
        }
    }

    class ResponseHandler implements BiConsumer<DistributedResultResponse, Throwable> {

        private final Downstream downstream;
        private final NodeRequest<DistributedResultRequest> request;
        private final BatchIterator<Row> it;
        private final AtomicInteger numActiveRequests;
        private final boolean isFailureReq;
        private final Iterator<TimeValue> retries;

        public ResponseHandler(Downstream downstream,
                               NodeRequest<DistributedResultRequest> request,
                               BatchIterator<Row> it,
                               AtomicInteger numActiveRequests,
                               boolean isFailureReq) {
            this.downstream = downstream;
            this.request = request;
            this.it = it;
            this.numActiveRequests = numActiveRequests;
            this.isFailureReq = isFailureReq;
            this.retries = BackoffPolicy.exponentialBackoff().iterator();
        }

        @Override
        public void accept(DistributedResultResponse resp, Throwable err) {
            if (err == null) {
                if (isFailureReq) {
                    downstream.needsMoreData = false;
                    countdownAndMaybeCloseIt(numActiveRequests, it);
                } else {
                    downstream.needsMoreData = resp.needMore();
                    countdownAndMaybeContinue(it, numActiveRequests, false);
                }
                return;
            }
            err = SQLExceptions.unwrap(err);
            LOGGER.trace(
                "Failure from downstream while sending result. job={} targetNode={} failure={}",
                jobId,
                downstream.nodeId,
                err
            );
            if (err instanceof ConnectTransportException && retries.hasNext()) {
                LOGGER.trace("Retry sending result", err);
                TimeValue delay = retries.next();
                try {
                    var cancellable = threadPool.scheduleUnlessShuttingDown(
                        delay,
                        ThreadPool.Names.SAME,
                        () -> {
                            try {
                                responseExecutor.execute(() -> distributedResultAction.execute(request).whenComplete(this));
                            } catch (RejectedExecutionException ex) {
                                handleFailure(ex);
                            }
                        }
                    );

                    // shutting down; no retry
                    if (cancellable == Cancellable.CANCELLED_NOOP) {
                        handleFailure(err);
                    }
                } catch (EsRejectedExecutionException ex) {
                    handleFailure(err);
                }
                return;
            }
            handleFailure(err);
        }

        private void handleFailure(Throwable err) {
            // Downstream can receive kill from other nodes and send `JobKilledException` back due to it
            // We want to preserve original errors that led to the kill
            if (failure == null || !(err instanceof JobKilledException)) {
                failure = err;
            }
            downstream.needsMoreData = false;
            // continue because it's necessary to send something to the other downstreams still waiting for data
            if (isFailureReq) {
                countdownAndMaybeCloseIt(numActiveRequests, it);
            } else {
                // If we get a JobKilled from downstream it was already broadcast
                if (!(err instanceof JobKilledException)) {
                    String reason = "An error was encountered: " + err;
                    broadcastKill(killNodeAction, jobId, localNodeId, reason);
                }
                it.close();
                completionFuture.completeExceptionally(failure);
            }
        }
    }

    private void countdownAndMaybeContinue(BatchIterator<Row> it,
                                           AtomicInteger numActiveRequests,
                                           boolean sameExecutor) {
        if (numActiveRequests.decrementAndGet() == 0) {
            if (downstreams.stream().anyMatch(Downstream::needsMoreData)) {
                if (failure == null) {
                    if (sameExecutor) {
                        consumeIt(it);
                    } else {
                        // try to dispatch to different executor, if it fails, forward the error in the same thread
                        try {
                            responseExecutor.execute(() -> consumeIt(it));
                        } catch (EsRejectedExecutionException e) {
                            failure = e;
                            forwardFailure(it, failure);
                        }
                    }
                } else {
                    forwardFailure(it, failure);
                }
            } else {
                // If we've a failure we either communicated it to the other downstreams already,
                // or were able to send results to all downstreams. In either case, *this* operation succeeded and the
                // downstreams need to deal with failures.

                // The TasksService takes care of node disconnects, so we don't have to manage
                // that scenario.
                it.close();
                completionFuture.complete(null);
            }
        }
    }

    private static class Downstream {

        private final String nodeId;
        private boolean needsMoreData = true;

        Downstream(String nodeId) {
            this.nodeId = nodeId;
        }

        boolean needsMoreData() {
            return needsMoreData;
        }

        @Override
        public String toString() {
            return "Downstream{" +
                   nodeId + '\'' +
                   ", needsMoreData=" + needsMoreData +
                   '}';
        }
    }

    @Override
    public String toString() {
        return "DistributingConsumer{" +
               "jobId=" + jobId +
               ", targetPhaseId=" + targetPhaseId +
               ", inputId=" + inputId +
               ", bucketIdx=" + bucketIdx +
               ", downstreams=" + downstreams +
               ", failure=" + failure +
               '}';
    }
}
