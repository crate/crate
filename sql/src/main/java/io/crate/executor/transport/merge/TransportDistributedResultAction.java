/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.merge;

import io.crate.Streamer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.DistributedResultRequestHandler;
import io.crate.executor.transport.ResponseForwarder;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.PageDownstreamFactory;
import io.crate.planner.node.StreamerVisitor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;


public class TransportDistributedResultAction {

    public final static String DISTRIBUTED_RESULT_ACTION = "crate/sql/node/merge/add_rows";

    private final TransportService transportService;
    private final ClusterService clusterService;

    @Inject
    public TransportDistributedResultAction(final ClusterService clusterService,
                                            JobContextService jobContextService,
                                            TransportService transportService) {
        this.transportService = transportService;
        this.clusterService = clusterService;

        transportService.registerHandler(DISTRIBUTED_RESULT_ACTION, new DistributedResultRequestHandler(jobContextService));
    }

    public void pushResult(String node, DistributedResultRequest request, ActionListener<DistributedResultResponse> listener) {
        new AsyncMergeRowsAction(node, request, listener).start();
    }

    protected String executorName() {
        return ThreadPool.Names.SEARCH;
    }

    private class AsyncMergeRowsAction {

        private final DiscoveryNode node;
        private final DistributedResultRequest request;
        private final ActionListener<DistributedResultResponse> listener;

        public AsyncMergeRowsAction(String node, DistributedResultRequest request, ActionListener<DistributedResultResponse> listener) {
            this.node = clusterService.state().nodes().get(node);
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            transportService.sendRequest(
                    node,
                    DISTRIBUTED_RESULT_ACTION,
                    request,
                    new BaseTransportResponseHandler<DistributedResultResponse>() {
                        @Override
                        public DistributedResultResponse newInstance() {
                            return new DistributedResultResponse();
                        }

                        @Override
                        public void handleResponse(DistributedResultResponse response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            listener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return executorName();
                        }
                    }
            );
        }
    }
}
