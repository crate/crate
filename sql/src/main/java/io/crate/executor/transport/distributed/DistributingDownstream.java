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

import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.transport.merge.TransportDistributedResultAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class DistributingDownstream extends ResultProviderBase {

    private static final ESLogger logger = Loggers.getLogger(DistributingDownstream.class);

    private final UUID jobId;
    private final MultiBucketBuilder bucketBuilder;
    private final TransportService transportService;
    private final DistributedResultRequest[] requests;
    private List<DiscoveryNode> downstreams;


    public DistributingDownstream(UUID jobId,
                                  List<DiscoveryNode> downstreams,
                                  TransportService transportService,
                                  Streamer<?>[] streamers) {
        this.downstreams = downstreams;
        this.bucketBuilder = new MultiBucketBuilder(streamers, downstreams.size());
        this.jobId = jobId;
        this.transportService = transportService;
        this.requests = new DistributedResultRequest[downstreams.size()];
        for (int i = 0, length = downstreams.size(); i < length; i++) {
            this.requests[i] = new DistributedResultRequest(jobId, streamers);
        }
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        try {
            bucketBuilder.setNextRow(row);
        } catch (IOException e) {
            fail(e);
            return false;
        }
        return true;
    }

    protected void onAllUpstreamsFinished() {
        int i = 0;
        for (Bucket rows : bucketBuilder.build()) {
            DistributedResultRequest request = this.requests[i];
            request.rows(rows);
            final DiscoveryNode node = downstreams.get(i);
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] sending distributing collect request to {} ...",
                        jobId.toString(),
                        node.id());
            }
            sendRequest(request, node);
            i++;
        }
    }

    private void forwardFailures() {
        int idx = 0;
        for (DistributedResultRequest request : requests) {
            request.failure(true);
            sendRequest(request, downstreams.get(idx));
            idx++;
        }
    }

    private void sendRequest(final DistributedResultRequest request, final DiscoveryNode node) {
        transportService.submitRequest(
                node,
                TransportDistributedResultAction.DISTRIBUTED_RESULT_ACTION,
                request,
                new BaseTransportResponseHandler<DistributedResultResponse>() {
                    @Override
                    public DistributedResultResponse newInstance() {
                        return new DistributedResultResponse();
                    }

                    @Override
                    public void handleResponse(DistributedResultResponse response) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}] successfully sent distributing collect request to {}",
                                    jobId.toString(),
                                    node.id());
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        Throwable cause = exp.getCause();
                        if (cause instanceof EsRejectedExecutionException) {
                            sendFailure(request.contextId(), node);
                        } else {
                            logger.error("[{}] Exception sending distributing collect request to {}",
                                    exp, jobId, node.id());
                            fail(cause);
                        }
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                }
        );
    }

    private void sendFailure(UUID contextId, final DiscoveryNode node) {
        transportService.submitRequest(
                node,
                TransportDistributedResultAction.failAction,
                new DistributedFailureRequest(contextId),
                new BaseTransportResponseHandler<DistributedResultResponse>() {
                    @Override
                    public DistributedResultResponse newInstance() {
                        return new DistributedResultResponse();
                    }

                    @Override
                    public void handleResponse(DistributedResultResponse response) {
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("[{}] Exception sending distributing collect failure to {}",
                                exp, jobId, node.id());
                        fail(exp.getCause());
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
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
        forwardFailures();
        return t;
    }
}
