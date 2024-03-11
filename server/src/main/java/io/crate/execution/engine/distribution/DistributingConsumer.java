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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.BatchIterator;
import io.crate.data.Paging;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.SQLExceptions;
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

    @VisibleForTesting
    final MultiBucketBuilder multiBucketBuilder;

    private volatile Throwable failure;

    public DistributingConsumer(Executor responseExecutor,
                                UUID jobId,
                                MultiBucketBuilder multiBucketBuilder,
                                int targetPhaseId,
                                byte inputId,
                                int bucketIdx,
                                Collection<String> downstreamNodeIds,
                                ActionExecutor<NodeRequest<DistributedResultRequest>, DistributedResultResponse> distributedResultAction,
                                int pageSize) {
        this.traceEnabled = LOGGER.isTraceEnabled();
        this.responseExecutor = responseExecutor;
        this.jobId = jobId;
        this.multiBucketBuilder = multiBucketBuilder;
        this.targetPhaseId = targetPhaseId;
        this.inputId = inputId;
        this.bucketIdx = bucketIdx;
        this.distributedResultAction = distributedResultAction;
        this.pageSize = pageSize;
        this.buckets = new StreamBucket[downstreamNodeIds.size()];
        this.completionFuture = new CompletableFuture<>();
        downstreams = new ArrayList<>(downstreamNodeIds.size());
        for (String downstreamNodeId : downstreamNodeIds) {
            downstreams.add(new Downstream(downstreamNodeId));
        }
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
                if (multiBucketBuilder.size() >= pageSize || multiBucketBuilder.ramBytesUsed() >= Paging.MAX_PAGE_BYTES) {
                    forwardResults(it, false);
                    return;
                }
            }
            if (it.allLoaded()) {
                forwardResults(it, true);
            } else {
                it.loadNextBatch().whenComplete((r, t) -> {
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

    private void forwardFailure(@Nullable final BatchIterator<?> it, final Throwable f) {
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
                distributedResultAction
                    .execute(builder.build(downstream.nodeId))
                    .whenComplete(
                        (resp, t) -> {
                            if (t == null) {
                                downstream.needsMoreData = false;
                                countdownAndMaybeCloseIt(numActiveRequests, it);
                            } else {
                                if (traceEnabled) {
                                    LOGGER.trace(
                                        "Error sending failure to downstream={} jobId={} targetPhase={}/{} bucket={} failure={}",
                                        downstream.nodeId,
                                        jobId,
                                        targetPhaseId,
                                        inputId,
                                        bucketIdx,
                                        t
                                    );
                                }
                                countdownAndMaybeCloseIt(numActiveRequests, it);
                            }
                        }
                    );
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
            distributedResultAction
                .execute(
                    DistributedResultRequest.of(
                        downstream.nodeId,
                        jobId,
                        targetPhaseId,
                        inputId,
                        bucketIdx,
                        buckets[i],
                        isLast))
                .whenComplete(
                    (resp, t) -> {
                        if (t == null) {
                            downstream.needsMoreData = resp.needMore();
                            countdownAndMaybeContinue(it, numActiveRequests, false);
                        } else {
                            LOGGER.trace(
                                "Failure from downstream while sending result. job={} targetNode={} failure={}",
                                jobId,
                                downstream.nodeId,
                                t
                            );
                            failure = t;
                            downstream.needsMoreData = false;
                            // continue because it's necessary to send something to downstreams still waiting for data
                            countdownAndMaybeContinue(it, numActiveRequests, false);
                        }
                    }
                );
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

                // The NodeDisconnectJobMonitorService takes care of node disconnects, so we don't have to manage
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
               '}';
    }
}
