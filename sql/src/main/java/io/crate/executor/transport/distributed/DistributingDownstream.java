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
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.Requirements;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DistributingDownstream implements RowReceiver {

    private final static ESLogger LOGGER = Loggers.getLogger(DistributingDownstream.class);

    private static final ActionListener<DistributedResultResponse> NO_OP_ACTION_LISTENER = new ActionListener<DistributedResultResponse>() {
        @Override
        public void onResponse(DistributedResultResponse distributedResultResponse) {
        }

        @Override
        public void onFailure(Throwable e) {
            LOGGER.trace("Received failure from downstream: {}", e);
        }
    };

    @VisibleForTesting
    final MultiBucketBuilder multiBucketBuilder;

    private final UUID jobId;
    private final int targetExecutionPhaseId;
    private final byte inputId;
    private final int bucketIdx;
    private final TransportDistributedResultAction transportDistributedResultAction;
    private final Streamer<?>[] streamers;
    private final int pageSize;
    private RowUpstream upstream;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicInteger requestsPending = new AtomicInteger(0);
    private final Downstream[] downstreams;
    private final Object lock = new Object();
    private final AtomicInteger finishedDownstreams = new AtomicInteger(0);
    private final Bucket[] buckets;

    private volatile boolean gatherMoreRows = true;
    private boolean hasUpstreamFinished = false;

    public DistributingDownstream(UUID jobId,
                                  MultiBucketBuilder multiBucketBuilder,
                                  int targetExecutionPhaseId,
                                  byte inputId,
                                  int bucketIdx,
                                  Collection<String> downstreamNodeIds,
                                  TransportDistributedResultAction transportDistributedResultAction,
                                  Streamer<?>[] streamers,
                                  int pageSize) {
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
    public boolean setNextRow(Row row) {
        multiBucketBuilder.add(row);
        synchronized (lock) {
            if (multiBucketBuilder.size() >= pageSize) {
                if (requestsPending.get() > 0) {
                    LOGGER.trace("page is full and requests are pending.. pausing upstream");
                    pause();
                } else {
                    LOGGER.trace("page is full. Sending request");
                    multiBucketBuilder.build(buckets);
                    sendRequests(false);
                }
            }
        }
        return gatherMoreRows;
    }

    private void sendRequests(boolean isLast) {
        requestsPending.addAndGet(downstreams.length);
        for (int i = 0; i < buckets.length; i++) {
            downstreams[i].sendRequest(buckets[i], isLast);
        }
    }

    private void pause() {
        upstream.pause();
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    private void resume() {
        upstream.resume(true);
    }

    @Override
    public void finish() {
        upstreamFinished();
    }

    @Override
    public void fail(Throwable throwable) {
        if (throwable instanceof CancellationException) {
            failure.set(throwable);
        } else {
            failure.compareAndSet(null, throwable);
        }
        gatherMoreRows = false;
        upstreamFinished();
    }

    private void upstreamFinished() {
        hasUpstreamFinished = true;
        final Throwable throwable = failure.get();
        if (throwable == null) {
            if (requestsPending.get() == 0) {
                LOGGER.trace("all upstreams finished. Sending last requests");
                multiBucketBuilder.build(buckets);
                sendRequests(true);
            } else if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("all upstreams finished. Doing nothing since there are pending requests");
            }
        } else if (!(throwable instanceof CancellationException)) { // no need to forward kill - downstream will receive it too
            LOGGER.trace("all upstreams finished; forwarding failure");
            for (Downstream downstream : downstreams) {
                downstream.forwardFailure(throwable);
            }
        }
    }

    @Override
    public void prepare(ExecutionState executionState) {

    }

    @Override
    public void setUpstream(RowUpstream rowUpstream) {
        upstream = rowUpstream;
    }

    private class Downstream implements ActionListener<DistributedResultResponse> {

        private final String node;
        private boolean finished = false;

        public Downstream(String node) {
            this.node = node;
        }

        public void forwardFailure(Throwable throwable) {
            LOGGER.trace("Sending failure to {}", node);
            transportDistributedResultAction.pushResult(
                    node,
                    new DistributedResultRequest(jobId, targetExecutionPhaseId, inputId, bucketIdx, streamers, throwable),
                    NO_OP_ACTION_LISTENER
            );
        }

        public void sendRequest(Bucket bucket, boolean isLast) {
            if (finished) {
                return;
            }
            LOGGER.trace("Sending request to {}", node);
            transportDistributedResultAction.pushResult(
                    node,
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
            gatherMoreRows = false;
            onResponse(false);
        }

        private void onResponse(boolean needMore) {
            finished = !needMore;
            final int numPending = requestsPending.decrementAndGet();

            LOGGER.trace("Received response from downstream: {}; requires more: {}, other pending requests: {}, finished: {}",
                    node, needMore, numPending, hasUpstreamFinished);
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
                    gatherMoreRows = false;
                }
                resume();
            }
        }
    }
}
