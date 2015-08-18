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

import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class DistributingDownstream extends ResultProviderBase {

    public static final String PAGES_BUFFER_SIZE = "node.downstream.pages_buffer_size";
    public static final int DEFAULT_PAGES_BUFFER_SIZE = 1;

    private final TransportDistributedResultAction transportDistributedResultAction;
    private final AtomicInteger finishedDownstreams = new AtomicInteger(0);
    private final AtomicInteger currentPageProcessed = new AtomicInteger(0);
    private final AtomicBoolean requestsPending = new AtomicBoolean(false);
    private final AtomicBoolean lastPageSent = new AtomicBoolean(false);

    protected final Collection<Row> currentPage;
    protected final BlockingQueue<Row> rowQueue;
    protected final Downstream[] downstreams;
    protected final int pageSize;
    private volatile boolean killed = false;

    public DistributingDownstream(UUID jobId,
                                  int targetExecutionNodeId,
                                  byte inputId,
                                  int bucketIdx,
                                  Collection<String> downstreamNodeIds,
                                  TransportDistributedResultAction transportDistributedResultAction,
                                  Streamer<?>[] streamers,
                                  Settings settings,
                                  int pageSize) {
        this.transportDistributedResultAction = transportDistributedResultAction;

        downstreams = new Downstream[downstreamNodeIds.size()];

        int idx = 0;
        for (String downstreamNodeId : downstreamNodeIds) {
            downstreams[idx] = new Downstream(downstreamNodeId, jobId, targetExecutionNodeId,
                    inputId, bucketIdx, streamers);
            idx++;
        }

        this.pageSize = pageSize;
        currentPage = new ArrayList<>(pageSize);
        int pagesBufferSize = settings.getAsInt(PAGES_BUFFER_SIZE, DEFAULT_PAGES_BUFFER_SIZE);
        rowQueue = new ArrayBlockingQueue<>(pageSize * pagesBufferSize);
    }

    @Override
    public boolean setNextRow(Row row) {
        if (allDownstreamsFinished()) {
            return false;
        }

        try {
            rowQueue.put(new RowN(row.materialize()));
            if (allDownstreamsFinished()) {
                // in case the Q just got unblocked from a response with needMore=false
                return false;
            }
            sendRequestsIfNeeded();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            fail(e);
            return false;
        }
        return true;
    }

    private void sendRequestsIfNeeded() {
        synchronized (rowQueue) {
            if (!requestsPending.get() && (fullPageInQueue() || remainingUpstreams.get() == 0)) {
                if (!requestsPending.compareAndSet(false, true)) {
                    return;
                }
                if (remainingUpstreams.get() == 0) {
                    lastPageSent.set(true);
                }
                drainPageFromQueue();
                sendRequests();
            }
        }
    }

    private void drainPageFromQueue() {
        currentPage.clear();
        rowQueue.drainTo(currentPage, pageSize);
    }

    private boolean fullPageInQueue() {
        return rowQueue.size() >= pageSize;
    }

    protected boolean isLast() {
        return remainingUpstreams.get() == 0 && rowQueue.size() <= pageSize;
    }

    private void onAllUpstreamsFinished() {
        sendRequestsIfNeeded();
    }

    private void forwardFailures(Throwable throwable) {
        for (Downstream downstream : downstreams) {
            downstream.sendRequest(throwable);
        }
    }

    protected boolean allDownstreamsFinished() {
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
            logger().debug("{} killed", getClass().getSimpleName());
            killed = true;
        } else {
            forwardFailures(t);
        }
        return t;
    }

    private void onDownstreamResponse(boolean needMore) {
        if (!needMore) {
            if (finishedDownstreams.incrementAndGet() == downstreams.length) {
                drainPageFromQueue();
            };
        }
        synchronized (requestsPending) {
            if (currentPageProcessed.incrementAndGet() == downstreams.length) {
                currentPageProcessed.set(0);
                requestsPending.set(false);
            }
        }

        if (needMore && !lastPageSent.get() && !killed) {
            sendRequestsIfNeeded();
        }
    }

    protected abstract void sendRequests();

    protected abstract ESLogger logger();

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume() {
        throw new UnsupportedOperationException();
    }

    protected class Downstream implements ActionListener<DistributedResultResponse> {

        final AtomicBoolean wantMore = new AtomicBoolean(true);
        final String node;

        final UUID jobId;
        final int targetExecutionPhaseId;
        private final byte inputId;
        final int bucketIdx;
        final Streamer<?>[] streamers;

        public Downstream(String node,
                          UUID jobId,
                          int targetExecutionPhaseId,
                          byte inputId,
                          int bucketIdx,
                          Streamer<?>[] streamers) {
            this.node = node;
            this.jobId = jobId;
            this.targetExecutionPhaseId = targetExecutionPhaseId;
            this.inputId = inputId;
            this.bucketIdx = bucketIdx;
            this.streamers = streamers;
        }

        public void sendRequest(Throwable t) {
            DistributedResultRequest request = new DistributedResultRequest(
                    jobId, targetExecutionPhaseId, inputId, bucketIdx, streamers, t);
            sendRequest(request);
        }

        public void sendRequest(Bucket bucket, boolean isLast) {
            DistributedResultRequest request = new DistributedResultRequest(jobId, targetExecutionPhaseId,
                    inputId, bucketIdx, streamers, bucket != null ? bucket : Bucket.EMPTY, isLast);
            sendRequest(request);
        }

        private void sendRequest(final DistributedResultRequest request) {
            if (logger().isTraceEnabled()) {
                logger().trace("[{}] sending distributing result request to {} {} input {}, isLast? {}, size {} ...",
                        jobId.toString(),
                        node, inputId, request.isLast(), request.rows().size());
            }
            try {
                transportDistributedResultAction.pushResult(
                        node,
                        request,
                        this
                );
            } catch (IllegalArgumentException e) {
                logger().error(e.getMessage(), e);
                wantMore.set(false);
            }
        }

        @Override
        public void onResponse(DistributedResultResponse response) {
            if (logger().isTraceEnabled()) {
                logger().trace("[{}] successfully sent distributing result request to {} {} input {}, needMore? {}",
                         jobId,
                         targetExecutionPhaseId,
                         node,
                         inputId,
                         response.needMore());
            }

            wantMore.set(response.needMore());

            onDownstreamResponse(response.needMore());
        }

        @Override
        public void onFailure(Throwable exp) {
            logger().error("[{}] Exception sending distributing collect results to {}", exp, jobId, node);
            wantMore.set(false);
            onDownstreamResponse(false);
        }
    }
}
