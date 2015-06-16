/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.Constants;
import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Collection;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributingDownstream extends ResultProviderBase {

    private static final ESLogger LOGGER = Loggers.getLogger(DistributingDownstream.class);

    private final TransportDistributedResultAction transportDistributedResultAction;
    private final MultiBucketBuilder bucketBuilder;
    private Downstream[] downstreams;
    private final AtomicInteger finishedDownstreams = new AtomicInteger(0);

    public DistributingDownstream(UUID jobId,
                                  int targetExecutionNodeId,
                                  byte inputId,
                                  int bucketIdx,
                                  Collection<String> downstreamNodeIds,
                                  TransportDistributedResultAction transportDistributedResultAction,
                                  Streamer<?>[] streamers) {
        this.transportDistributedResultAction = transportDistributedResultAction;

        downstreams = new Downstream[downstreamNodeIds.size()];
        bucketBuilder = new MultiBucketBuilder(streamers, downstreams.length);

        int idx = 0;
        for (String downstreamNodeId : downstreamNodeIds) {
            downstreams[idx] = new Downstream(downstreamNodeId, jobId, targetExecutionNodeId, inputId, bucketIdx, streamers);
            idx++;
        }
    }

    @Override
    public boolean setNextRow(Row row) {
        if (allDownstreamsFinished()) {
            return false;
        }
        try {
            int downstreamIdx = bucketBuilder.getBucket(row);
            // only collect if downstream want more rows, otherwise just ignore the row
            if (downstreams[downstreamIdx].wantMore.get()) {
                bucketBuilder.setNextRow(downstreamIdx, row);
                sendRequestIfNeeded(downstreamIdx);
            }
        } catch (IOException e) {
            fail(e);
            return false;
        }
        return true;
    }

    private void sendRequestIfNeeded(int downstreamIdx) {
        int size = bucketBuilder.size(downstreamIdx);
        if (size >= Constants.PAGE_SIZE || remainingUpstreams.get() <= 0) {
            Downstream downstream = downstreams[downstreamIdx];
            downstream.bucketQueue.add(bucketBuilder.build(downstreamIdx));
            downstream.sendRequest(remainingUpstreams.get() <= 0);
        }
    }

    private void onAllUpstreamsFinished() {
        for (int i = 0; i < downstreams.length; i++) {
            sendRequestIfNeeded(i);
        }
    }

    private void forwardFailures(Throwable throwable) {
        for (Downstream downstream : downstreams) {
            downstream.sendRequest(throwable);
        }
    }

    private boolean allDownstreamsFinished() {
        return finishedDownstreams.get() == downstreams.length;
    }

    @Override
    public Bucket doFinish() {
        onAllUpstreamsFinished();
        return null;
    }

    @Override
    public Throwable doFail(Throwable t) {
        if (t instanceof CancellationException) {
            // fail without sending anything
            LOGGER.debug("{} killed", getClass().getSimpleName());
        } else {
            forwardFailures(t);
        }
        return t;
    }

    private class Downstream implements ActionListener<DistributedResultResponse> {

        final AtomicBoolean wantMore = new AtomicBoolean(true);
        final AtomicBoolean requestPending = new AtomicBoolean(false);
        final Deque<Bucket> bucketQueue = new ConcurrentLinkedDeque<>();
        final Deque<DistributedResultRequest> requestQueue = new ConcurrentLinkedDeque<>();
        final String node;

        final UUID jobId;
        final int targetExecutionNodeId;
        private final byte inputId;
        final int bucketIdx;
        final Streamer<?>[] streamers;

        public Downstream(String node,
                          UUID jobId,
                          int targetExecutionNodeId,
                          byte inputId,
                          int bucketIdx,
                          Streamer<?>[] streamers) {
            this.node = node;
            this.jobId = jobId;
            this.targetExecutionNodeId = targetExecutionNodeId;
            this.inputId = inputId;
            this.bucketIdx = bucketIdx;
            this.streamers = streamers;
        }

        public void sendRequest(Throwable t) {
            DistributedResultRequest request = new DistributedResultRequest(
                    jobId, targetExecutionNodeId, inputId, bucketIdx, streamers, t);
            sendRequest(request);
        }

        public void sendRequest(boolean isLast) {
            Bucket bucket = bucketQueue.poll();
            DistributedResultRequest request = new DistributedResultRequest(
                    jobId, targetExecutionNodeId, inputId, bucketIdx,
                    streamers, bucket != null ? bucket : Bucket.EMPTY, isLast);
            synchronized(this.requestQueue) {
                if (!requestPending.compareAndSet(false, true)) {
                    requestQueue.add(request);
                    return;
                }
            }
            sendRequest(request);
        }

        private void sendRequest(final DistributedResultRequest request) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[{}] sending distributing collect request to {}, isLast? {} ...",
                        jobId.toString(),
                        node, request.isLast());
            }
            try {
                transportDistributedResultAction.pushResult(
                        node,
                        request,
                        this
                );
            } catch (IllegalArgumentException e) {
                LOGGER.error(e.getMessage(), e);
                wantMore.set(false);
            }
        }

        @Override
        public void onResponse(DistributedResultResponse response) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[{}] successfully sent distributing collect request to {}, needMore? {}",
                        jobId,
                        node,
                        response.needMore());
            }

            wantMore.set(response.needMore());
            if (!response.needMore()) {
                finishedDownstreams.incrementAndGet();
                requestQueue.clear();
                // clean-up queue because no more rows are wanted
                bucketQueue.clear();
            } else {
                DistributedResultRequest request;
                synchronized (requestQueue) {
                    if (requestQueue.size() > 0) {
                        request = requestQueue.poll();
                    } else {
                        requestPending.set(false);
                        return;
                    }
                }
                sendRequest(request);
            }
        }

        @Override
        public void onFailure(Throwable exp) {
            LOGGER.error("[{}] Exception sending distributing collect request to {}", exp, jobId, node);
            wantMore.set(false);
            bucketQueue.clear();
            finishedDownstreams.incrementAndGet();
        }
    }
}
