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
import io.crate.executor.transport.distributed.DistributedRequestContextManager;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
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

    private static final ESLogger logger = Loggers.getLogger(TransportDistributedResultAction.class);
    private final DistributedRequestContextManager contextManager;

    public final static String DISTRIBUTED_RESULT_ACTION = "crate/sql/node/merge/add_rows";
    public final static String failAction = "crate/sql/node/merge/fail";
    private final static String startMergeAction = "crate/sql/node/merge/start";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final StreamerVisitor planNodeStreamerVisitor;
    private final ThreadPool threadPool;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public TransportDistributedResultAction(final ClusterService clusterService,
                                            TransportService transportService,
                                            final Functions functions,
                                            final ThreadPool threadPool,
                                            final PageDownstreamFactory pageDownstreamFactory,
                                            StatsTables statsTables,
                                            CrateCircuitBreakerService breakerService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);

        planNodeStreamerVisitor = new StreamerVisitor(functions);
        this.contextManager = new DistributedRequestContextManager(pageDownstreamFactory, functions, statsTables, circuitBreaker);

        transportService.registerHandler(startMergeAction, new StartMergeHandler());
        transportService.registerHandler(failAction, new FailureHandler(contextManager));
        transportService.registerHandler(DISTRIBUTED_RESULT_ACTION, new DistributedResultRequestHandler(contextManager));
    }

    public void startMerge(String node, NodeMergeRequest request, ActionListener<NodeMergeResponse> listener) {
        logger.trace("startMerge: {}", node);
        new AsyncMergeStartAction(node, request, listener).start();
    }

    public void mergeRows(String node, DistributedResultRequest request, ActionListener<DistributedResultResponse> listener) {
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

    private class AsyncMergeStartAction {

        private final DiscoveryNode node;
        private final NodeMergeRequest request;
        private final ActionListener<NodeMergeResponse> listener;
        private final Streamer<?>[] streamers;
        private final String nodeId;

        public AsyncMergeStartAction(String node, NodeMergeRequest request, ActionListener<NodeMergeResponse> listener) {
            this.node = clusterService.state().nodes().get(node);
            this.nodeId = this.node.id();
            this.request = request;
            this.listener = listener;
            StreamerVisitor.Context streamerContext = planNodeStreamerVisitor.processPlanNode(
                    request.mergeNode(),
                    new RamAccountingContext("dummy", circuitBreaker));
            this.streamers = streamerContext.outputStreamers();
        }

        public void start() {
            if (nodeId.equals("_local") || nodeId.equals(clusterService.state().nodes().localNode().getId())) {
                logger.trace("AsyncMergeStartAction.start local node: {} {}", this, nodeId);
                threadPool.executor(executorName()).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            contextManager.createContext(request.mergeNode(), new ActionListener<NodeMergeResponse>() {
                                @Override
                                public void onResponse(NodeMergeResponse nodeMergeResponse) {
                                    logger.trace("createContext.onRespnose", nodeId);
                                    listener.onResponse(nodeMergeResponse);
                                }

                                @Override
                                public void onFailure(Throwable e) {
                                    logger.trace("createContext.onFailure", nodeId);
                                    listener.onFailure(e);
                                }
                            });
                        } catch (Throwable e) {
                            logger.error("createContext.catched local exception node: {}", e, nodeId);
                            listener.onFailure(e);
                        }
                    }
                });
            } else {
                logger.trace("AsyncMergeStartAction.start remote node: {} {}", this, nodeId);
                transportService.sendRequest(
                        node,
                        startMergeAction,
                        request,
                        new BaseTransportResponseHandler<NodeMergeResponse>() {
                            @Override
                            public NodeMergeResponse newInstance() {
                                return new NodeMergeResponse(streamers);
                            }

                            @Override
                            public void handleResponse(NodeMergeResponse response) {
                                logger.trace("NodeMergeResponse.handleResponse: {}", nodeId);
                                listener.onResponse(response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                logger.error("NodeMergeResponse.handleException {}", exp);
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

    private class StartMergeHandler extends BaseTransportRequestHandler<NodeMergeRequest> {

        @Override
        public NodeMergeRequest newInstance() {
            return new NodeMergeRequest();
        }

        @Override
        public void messageReceived(final NodeMergeRequest request, final TransportChannel channel) throws Exception {
            ActionListener<NodeMergeResponse> listener = ResponseForwarder.forwardTo(channel);
            contextManager.createContext(request.mergeNode(), listener);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }
    }
}
