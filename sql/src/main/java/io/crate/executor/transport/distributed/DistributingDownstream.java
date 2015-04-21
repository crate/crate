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
import io.crate.executor.transport.merge.TransportDistributedResultAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributingDownstream extends ResultProviderBase {

    private static final ESLogger LOGGER = Loggers.getLogger(DistributingDownstream.class);

    private final UUID jobId;
    private final TransportDistributedResultAction transportDistributedResultAction;
    private final MultiBucketBuilder bucketBuilder;
    private Downstream[] downstreams;
    private final AtomicInteger finishedDownstreams = new AtomicInteger(0);

    public DistributingDownstream(UUID jobId,
                                  int targetExecutionNodeId,
                                  int bucketIdx,
                                  List<DiscoveryNode> downstreams,
                                  TransportDistributedResultAction transportDistributedResultAction,
                                  Streamer<?>[] streamers) {
        this.jobId = jobId;
        this.transportDistributedResultAction = transportDistributedResultAction;
        this.downstreams = new Downstream[downstreams.size()];
        bucketBuilder = new MultiBucketBuilder(streamers, downstreams.size());
        for (int i = 0; i < downstreams.size(); i++) {
            this.downstreams[i] = new Downstream(
                    downstreams.get(i), jobId, targetExecutionNodeId, bucketIdx, streamers);
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

    protected void sendRequestIfNeeded(int downstreamIdx) {
        int size = bucketBuilder.size(downstreamIdx);
        if (size >= Constants.PAGE_SIZE || remainingUpstreams.get() <= 0) {
            Downstream downstream = downstreams[downstreamIdx];
            downstream.bucketQueue.add(bucketBuilder.build(downstreamIdx));
            sendRequest(downstream);
        }
    }

    protected void onAllUpstreamsFinished() {
        for (int i = 0; i < downstreams.length; i++) {
            sendRequestIfNeeded(i);
        }
    }

    private void forwardFailures(Throwable throwable) {
        for (Downstream downstream : downstreams) {
            downstream.request.throwable(throwable);
            sendRequest(downstream.request, downstream);
        }
    }

    private boolean allDownstreamsFinished() {
        return finishedDownstreams.get() == downstreams.length;
    }

    private void sendRequest(Downstream downstream) {
        if (downstream.requestPending.compareAndSet(false, true)) {
            DistributedResultRequest request = downstream.request;
            Deque<Bucket> queue = downstream.bucketQueue;
            int size = queue.size();
            if (size > 0) {
                request.rows(queue.poll());
            } else {
                request.rows(Bucket.EMPTY);
            }
            request.isLast(!(size > 1 || remainingUpstreams.get() > 0));
            sendRequest(request, downstream);
        }
    }

    private void sendRequest(final DistributedResultRequest request, final Downstream downstream) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[{}] sending distributing collect request to {}, isLast? {} ...",
                    jobId.toString(),
                    downstream.node.id(), request.isLast());
        }
        transportDistributedResultAction.pushResult(
                downstream.node,
                request,
                new ActionListener<DistributedResultResponse>() {
                    @Override
                    public void onResponse(DistributedResultResponse response) {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("[{}] successfully sent distributing collect request to {}, needMore? {}",
                                    jobId.toString(),
                                    downstream.node.id(),
                                    response.needMore());
                        }
                        downstream.wantMore.set(response.needMore());
                        if (!response.needMore()) {
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("downstream {} don't want more, clearing queue",
                                        downstream.node.id());
                            }
                            finishedDownstreams.incrementAndGet();
                            // clean-up queue because no more rows are wanted
                            downstream.bucketQueue.clear();
                        } else {
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("downstream {} want more",
                                        downstream.node.id());
                            }
                            // send next request or final empty closing one
                            downstream.requestPending.set(false);
                            sendRequest(downstream);
                        }
                    }

                    @Override
                    public void onFailure(Throwable exp) {
                        Throwable cause = exp.getCause();
                        LOGGER.error("[{}] Exception sending distributing collect request to {}", exp, jobId, downstream.node.id());
                        if (cause == null) {
                            fail(exp);
                        } else {
                            fail(cause);
                        }
                    }
                }
        );
    }

    @Override
    public Bucket doFinish() {
        onAllUpstreamsFinished();
        return null;
    }

    @Override
    public Throwable doFail(Throwable t) {
        forwardFailures(t);
        return t;
    }

    static class Downstream {

        final AtomicBoolean wantMore = new AtomicBoolean(true);
        final AtomicBoolean requestPending = new AtomicBoolean(false);
        final Deque<Bucket> bucketQueue = new ConcurrentLinkedDeque<>();
        final DistributedResultRequest request;
        final DiscoveryNode node;

        public Downstream(DiscoveryNode node,
                          UUID jobId,
                          int targetExecutionNodeId,
                          int bucketIdx,
                          Streamer<?>[] streamers) {
            this.node = node;
            this.request = new DistributedResultRequest(jobId, targetExecutionNodeId, bucketIdx, streamers);
        }
    }
}
