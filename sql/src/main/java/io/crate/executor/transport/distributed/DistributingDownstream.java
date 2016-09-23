/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.annotations.VisibleForTesting;
import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.operation.projectors.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DistributingDownstream implements RowReceiver {

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

    @VisibleForTesting
    final MultiBucketBuilder multiBucketBuilder;

    private final ESLogger logger;
    private final UUID jobId;
    private final int targetExecutionPhaseId;
    private final byte inputId;
    private final int bucketIdx;
    private final TransportDistributedResultAction transportDistributedResultAction;
    private final Streamer<?>[] streamers;
    private final int pageSize;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicInteger requestsPending = new AtomicInteger(0);
    private final Downstream[] downstreams;
    private final Object lock = new Object();
    private final AtomicInteger finishedDownstreams = new AtomicInteger(0);
    private final Bucket[] buckets;

    private volatile Result setNextRowResult = Result.CONTINUE;
    private volatile boolean killed = false;
    private boolean hasUpstreamFinished = false;

    private final AtomicInteger resumeLatch = new AtomicInteger(2);
    private ResumeHandle resumeable = ResumeHandle.INVALID;

    public DistributingDownstream(ESLogger logger,
                                  UUID jobId,
                                  MultiBucketBuilder multiBucketBuilder,
                                  int targetExecutionPhaseId,
                                  byte inputId,
                                  int bucketIdx,
                                  Collection<String> downstreamNodeIds,
                                  TransportDistributedResultAction transportDistributedResultAction,
                                  Streamer<?>[] streamers,
                                  int pageSize) {
        this.logger = logger;
        this.jobId = jobId;
        this.multiBucketBuilder = multiBucketBuilder;
        this.targetExecutionPhaseId = targetExecutionPhaseId;
        this.inputId = inputId;
        this.bucketIdx = bucketIdx;
        this.transportDistributedResultAction = transportDistributedResultAction;
        this.streamers = streamers;
        this.pageSize = pageSize;

        buckets = new Bucket[downstreamNodeIds.size()];
        downstreams = new Downstream[downstreamNodeIds.size()];
        int i = 0;
        for (String downstreamNodeId : downstreamNodeIds) {
            downstreams[i] = new Downstream(downstreamNodeId);
            i++;
        }
    }

    @Override
    public Result setNextRow(Row row) {
        if (killed) {
            return Result.STOP;
        }
        multiBucketBuilder.add(row);
        synchronized (lock) {
            if (multiBucketBuilder.size() >= pageSize) {
                resumeLatch.set(2);
                if (requestsPending.get() > 0) {
                    traceLog("page is full and requests are pending");
                    return Result.PAUSE;
                } else {
                    traceLog("page is full - sending request");
                    multiBucketBuilder.build(buckets);
                    sendRequests(false);
                }
            }
        }
        return setNextRowResult;
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
        if (resumeLatch.decrementAndGet() == 0) {
            resumeable.resume(true);
        } else {
            this.resumeable = resumeable;
        }
    }

    private void resume() {
        if (resumeLatch.decrementAndGet() == 0) {
            ResumeHandle resumeable = this.resumeable;
            this.resumeable = ResumeHandle.INVALID;
            resumeable.resume(true);
        }
    }

    private void traceLog(String msg) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}. targetPhase={}/{} bucket={}", msg, targetExecutionPhaseId, inputId, bucketIdx);
        }
    }

    private void sendRequests(boolean isLast) {
        requestsPending.addAndGet(downstreams.length);
        for (int i = 0; i < buckets.length; i++) {
            downstreams[i].sendRequest(buckets[i], isLast);
        }
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        upstreamFinished();
    }

    @Override
    public void fail(Throwable throwable) {
        failure.compareAndSet(null, throwable);
        setNextRowResult = Result.STOP;
        upstreamFinished();
    }

    @Override
    public void kill(Throwable throwable) {
        killed = true;
        // downstream will also receive a kill request
    }

    @Override
    public void prepare() {
    }

    private void upstreamFinished() {
        if (killed) {
            return;
        }
        hasUpstreamFinished = true;
        final Throwable throwable = failure.get();
        if (throwable == null) {
            if (requestsPending.get() == 0) {
                traceLog("all upstreams finished. Sending last requests");
                multiBucketBuilder.build(buckets);
                sendRequests(true);
            } else if (logger.isTraceEnabled()) {
                traceLog("all upstreams finished. Doing nothing since there are pending requests");
            }
        } else {
            traceLog("all upstreams finished; forwarding failure");
            for (Downstream downstream : downstreams) {
                downstream.forwardFailure(throwable);
            }
        }
    }

    private class Downstream implements ActionListener<DistributedResultResponse> {

        private final String targetNode;
        private boolean finished = false;


        public Downstream(String targetNode) {
            this.targetNode = targetNode;
        }

        private void traceLog(String msg) {
            if (logger.isTraceEnabled()) {
                logger.trace("{} targetNode={} targetPhase={}/{} bucket={}", msg, targetNode, targetExecutionPhaseId, inputId, bucketIdx);
            }
        }

        public void forwardFailure(Throwable throwable) {
            traceLog("Forwarding failure");
            transportDistributedResultAction.pushResult(
                targetNode,
                new DistributedResultRequest(jobId, targetExecutionPhaseId, inputId, bucketIdx, streamers, throwable),
                NO_OP_ACTION_LISTENER
            );
        }

        public void sendRequest(Bucket bucket, boolean isLast) {
            if (finished) {
                requestsPending.decrementAndGet();
                return;
            }
            traceLog("Sending result");
            transportDistributedResultAction.pushResult(
                targetNode,
                new DistributedResultRequest(jobId, targetExecutionPhaseId, inputId, bucketIdx, streamers, bucket, isLast),
                this
            );
        }

        @Override
        public void onResponse(DistributedResultResponse distributedResultResponse) {
            onResponse(distributedResultResponse.needMore());
        }

        @Override
        public void onFailure(Throwable e) {
            setNextRowResult = Result.STOP;
            onResponse(false);
        }

        private void onResponse(boolean needMore) {
            finished = !needMore;
            final int numPending = requestsPending.decrementAndGet();

            if (logger.isTraceEnabled()) {
                logger.trace("Received response fromNode={} phase={}/{} bucket={} requiresMore={} pendingRequests={} finished={}",
                    targetNode, targetExecutionPhaseId, inputId, bucketIdx, needMore, numPending, hasUpstreamFinished);
            }
            if (numPending > 0) {
                return;
            }
            if (hasUpstreamFinished) {
                // upstreams (e.g. collector(s)) finished (after the requests have been sent)
                // send request with isLast=true with remaining buckets to downstream nodes
                multiBucketBuilder.build(buckets);
                sendRequests(true); // only sends to nodes that aren't finished already
            } else if (needMore) {
                boolean resume = false;
                synchronized (lock) {
                    if (multiBucketBuilder.size() >= pageSize) {
                        multiBucketBuilder.build(buckets);
                        sendRequests(false);
                    } else {
                        resume = true;
                    }
                }
                if (resume) {
                    resume();
                }
            } else {
                if (finishedDownstreams.incrementAndGet() == downstreams.length) {
                    setNextRowResult = Result.STOP;
                }
                resume();
            }
        }
    }
}
