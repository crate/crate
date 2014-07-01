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
import io.crate.executor.transport.DistributedResultRequestHandler;
import io.crate.executor.transport.distributed.DistributedRequestContextManager;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.DownstreamOperation;
import io.crate.operation.DownstreamOperationFactory;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.merge.MergeOperation;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;


public class TransportMergeNodeAction {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final DistributedRequestContextManager contextManager;

    public final static String mergeRowsAction = "crate/sql/node/merge/add_rows";
    private final static String startMergeAction = "crate/sql/node/merge/start";
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final PlanNodeStreamerVisitor planNodeStreamerVisitor;
    private final ThreadPool threadPool;

    @Inject
    public TransportMergeNodeAction(final ClusterService clusterService,
                                    final Settings settings,
                                    final TransportShardBulkAction transportShardBulkAction,
                                    final TransportCreateIndexAction transportCreateIndexAction,
                                    TransportService transportService,
                                    ReferenceResolver referenceResolver,
                                    Functions functions,
                                    ThreadPool threadPool,
                                    StatsTables statsTables) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        final ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver, functions, RowGranularity.DOC
        );

        planNodeStreamerVisitor = new PlanNodeStreamerVisitor(functions);
        this.contextManager = new DistributedRequestContextManager(new DownstreamOperationFactory<MergeNode>() {
            @Override
            public DownstreamOperation create(MergeNode node) {
                return new MergeOperation(
                        clusterService,
                        settings,
                        transportShardBulkAction,
                        transportCreateIndexAction,
                        implementationSymbolVisitor,
                        node
                );
            }
        }, functions, statsTables);

        transportService.registerHandler(startMergeAction, new StartMergeHandler());
        transportService.registerHandler(mergeRowsAction, new DistributedResultRequestHandler(contextManager));
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
                    mergeRowsAction,
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
            this.streamers = planNodeStreamerVisitor.process(request.mergeNode()).outputStreamers();
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
                        } catch (IOException e) {
                            logger.error("createContext.catched local exception node: {}", nodeId, e);
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
            contextManager.createContext(request.mergeNode(), new ActionListener<NodeMergeResponse>() {
                @Override
                public void onResponse(NodeMergeResponse nodeMergeResponse) {
                    try {
                        channel.sendResponse(nodeMergeResponse);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException e1) {
                        logger.error(e.getMessage(), e);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return executorName();
        }
    }
}
