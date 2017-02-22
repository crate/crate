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

package io.crate.executor.transport.distributed;

import io.crate.Streamer;
import io.crate.data.BatchConsumer;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.operation.projectors.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RowReceiver that sends rows as paged requests to other hosts.
 *
 * How the data is "bucketed" depends on the {@code MultiBucketBuilder} implementation.
 *
 * (Simplified) Workflow:
 *
 * <pre>
 *      [upstream-loop]:
 *
 *          +--> upstream:
 *          |     |
 *          |     | emits rows
 *          |     |
 *          |     v
 *          |   setNextRow
 *          |     |
 *          |     | add to BucketBuilder
 *          |     v
 *          |     pageSizeReached?
 *          |         /    \
 *          |        /      \    [request-loop]:
 *          +--------        \
 *    [continue|pause]        \
 *                           trySendRequests
 *                          /   |
 *                         /    |
 *                  +------     |
 *                  |           v
 *                  |       Downstream
 *                  |           |
 *                  |  response v
 *                  +<----------+
 * </pre>
 *
 * Note that the upstream-loop will *not* suspend if the pageSize is reached
 * but there are no "in-flight" requests.
 *
 * Both, the upstream-loop and request-loop can be running at the same time.
 * {@code trySendRequests} has some synchronization to make sure that there are
 * never two pages send concurrently.
 *
 */
public class DistributingDownstream implements RowReceiver {

    private final ESLogger logger;
    final MultiBucketBuilder multiBucketBuilder;
    private final UUID jobId;
    private final int targetPhaseId;
    private final byte inputId;
    private final int bucketIdx;
    private final int pageSize;
    private final AtomicInteger inFlightRequests = new AtomicInteger(0);
    private final Object lock = new Object();
    private final Downstream[] downstreams;
    private final TransportDistributedResultAction distributedResultAction;
    private final Streamer<?>[] streamers;
    private final Bucket[] buckets;
    private final boolean traceEnabled;
    private final Collection<String> downstreamNodeIds;
    private boolean upstreamFinished;
    private boolean isKilled = false;
    private final AtomicReference<ResumeHandle> resumeHandleRef = new AtomicReference<>(ResumeHandle.INVALID);
    private volatile boolean stop = false;
    private final AtomicReference<Throwable> failure = new AtomicReference<>(null);
    private final CompletableFuture<Void> finishFuture = new CompletableFuture<>();

    public DistributingDownstream(ESLogger logger,
                                  UUID jobId,
                                  MultiBucketBuilder multiBucketBuilder,
                                  int targetPhaseId,
                                  byte inputId,
                                  int bucketIdx,
                                  Collection<String> downstreamNodeIds,
                                  TransportDistributedResultAction distributedResultAction,
                                  Streamer<?>[] streamers,
                                  int pageSize) {
        this.logger = logger;
        this.multiBucketBuilder = multiBucketBuilder;
        this.jobId = jobId;
        this.targetPhaseId = targetPhaseId;
        this.inputId = inputId;
        this.bucketIdx = bucketIdx;
        this.pageSize = pageSize;
        this.streamers = streamers;
        this.downstreamNodeIds = downstreamNodeIds;

        buckets = new Bucket[downstreamNodeIds.size()];
        downstreams = new Downstream[downstreamNodeIds.size()];
        this.distributedResultAction = distributedResultAction;
        int i = 0;
        for (String downstreamNodeId : downstreamNodeIds) {
            downstreams[i] = new Downstream(downstreamNodeId);
            i++;
        }
        traceEnabled = logger.isTraceEnabled();
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return finishFuture;
    }

    @Override
    public Result setNextRow(Row row) {
        if (stop) {
            return Result.STOP;
        }
        multiBucketBuilder.add(row);
        if (multiBucketBuilder.size() >= pageSize) {
            if (inFlightRequests.get() == 0) {
                trySendRequests();
            } else {
                /*
                 * trySendRequests is called after pause has been processed to avoid race condition:
                 * ( trySendRequest -> return PAUSE -> onResponse before pauseProcessed)
                 */
                return Result.PAUSE;
            }
        }
        return Result.CONTINUE;
    }

    private void traceLog(String msg) {
        if (traceEnabled) {
            logger.trace("targetPhase={}/{} bucketIdx={} " + msg, targetPhaseId, inputId, bucketIdx);
        }
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
        if (resumeHandleRef.compareAndSet(ResumeHandle.INVALID, resumeable)) {
            trySendRequests();
        } else {
            throw new IllegalStateException("Invalid resumeHandle was set");
        }
    }

    @Override
    public void finish(RepeatHandle repeatable) {
        traceLog("action=finish");
        upstreamFinished = true;
        trySendRequests();

        // even if there are still requests pending we can trigger the future because
        // the resources of the upstream are no longer required
        finishFuture.complete(null);
    }

    @Override
    public void fail(Throwable throwable) {
        traceLog("action=fail");
        stop = true;
        failure.compareAndSet(null, throwable);
        upstreamFinished = true;
        trySendRequests();

        finishFuture.completeExceptionally(throwable);
    }

    private void trySendRequests() {
        boolean isLastRequest;
        Throwable error;
        boolean resumeWithoutSendingRequests = false;
        synchronized (lock) {
            int numInFlightRequests = inFlightRequests.get();
            if (numInFlightRequests > 0) {
                traceLog("action=trySendRequests numInFlightRequests > 0");
                return;
            }
            isLastRequest = upstreamFinished;
            error = failure.get();
            if (isLastRequest || multiBucketBuilder.size() >= pageSize) {
                multiBucketBuilder.build(buckets);
                inFlightRequests.addAndGet(downstreams.length);
                if (traceEnabled) {
                    logger.trace("targetPhase={}/{} bucketIdx={} action=trySendRequests isLastRequest={} ",
                        targetPhaseId, inputId, bucketIdx, isLastRequest);
                }
            } else {
                resumeWithoutSendingRequests = true;
            }
        }

        if (resumeWithoutSendingRequests) {
            // do resume outside of the lock
            doResume();
            return;
        }
        boolean allDownstreamsFinished = doSendRequests(isLastRequest, error);
        if (isLastRequest) {
            return;
        }
        if (allDownstreamsFinished) {
            stop = true;
        }
        doResume();
    }

    private boolean doSendRequests(boolean isLastRequest, Throwable error) {
        boolean allDownstreamsFinished = true;
        for (int i = 0; i < downstreams.length; i++) {
            Downstream downstream = downstreams[i];
            allDownstreamsFinished &= downstream.isFinished;
            if (downstream.isFinished) {
                inFlightRequests.decrementAndGet();
            } else {
                if (error == null) {
                    downstream.sendRequest(buckets[i], isLastRequest);
                } else {
                    downstream.sendRequest(error, isKilled);
                }
            }
        }
        return allDownstreamsFinished;
    }

    private void doResume() {
        ResumeHandle resumeHandle = resumeHandleRef.get();
        if (resumeHandle != ResumeHandle.INVALID) {
            resumeHandleRef.set(ResumeHandle.INVALID);
            resumeHandle.resume(true);
        }
    }

    @Override
    public void kill(Throwable throwable) {
        stop = true;
        // forward kill reason to downstream if not already done, otherwise downstream may wait forever for a final result
        if (failure.compareAndSet(null, throwable)) {
            upstreamFinished = true;
            isKilled = true;
            trySendRequests();
        }
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    private class Downstream implements ActionListener<DistributedResultResponse> {

        private final String downstreamNodeId;
        boolean isFinished = false;

        Downstream(String downstreamNodeId) {
            this.downstreamNodeId = downstreamNodeId;
        }

        void sendRequest(Throwable t, boolean isKilled) {
            distributedResultAction.pushResult(
                downstreamNodeId,
                new DistributedResultRequest(jobId, targetPhaseId, inputId, bucketIdx, t, isKilled),
                NO_OP_ACTION_LISTENER
            );
        }

        void sendRequest(Bucket bucket, boolean isLast) {
            distributedResultAction.pushResult(
                downstreamNodeId,
                new DistributedResultRequest(jobId, targetPhaseId, inputId, bucketIdx, streamers, bucket, isLast),
                this
            );
        }

        @Override
        public void onResponse(DistributedResultResponse distributedResultResponse) {
            isFinished = !distributedResultResponse.needMore();
            inFlightRequests.decrementAndGet();
            trySendRequests();
        }

        @Override
        public void onFailure(Throwable e) {
            stop = true;
            isFinished = true;
            inFlightRequests.decrementAndGet();
            trySendRequests(); // still need to resume upstream if it was paused
        }
    }

    private static final ActionListener<DistributedResultResponse> NO_OP_ACTION_LISTENER = new ActionListener<DistributedResultResponse>() {

        private final ESLogger LOGGER = Loggers.getLogger(DistributingDownstream.class);

        @Override
        public void onResponse(DistributedResultResponse distributedResultResponse) {
        }

        @Override
        public void onFailure(Throwable e) {
            LOGGER.trace("Received failure from downstream", e);
        }
    };


    @Nullable
    @Override
    public BatchConsumer asConsumer() {
        failure.set(new IllegalStateException("RowReceiver invalidated"));
        return new DistributingConsumer(
            logger,
            jobId,
            multiBucketBuilder,
            targetPhaseId,
            inputId,
            bucketIdx,
            downstreamNodeIds,
            distributedResultAction,
            streamers,
            pageSize,
            finishFuture
        );
    }
}
