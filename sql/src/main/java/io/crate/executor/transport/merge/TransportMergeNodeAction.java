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

import io.crate.executor.transport.distributed.DistributedRequestContextManager;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.operations.DownstreamOperationFactory;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.operations.merge.DownstreamOperation;
import io.crate.operator.operations.merge.MergeOperation;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.MergeNode;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import org.cratedb.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;


public class TransportMergeNodeAction {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final DistributedRequestContextManager contextManager;

    private final static String mergeRowsAction = "crate/sql/node/merge/add_rows";
    private final static String startMergeAction = "crate/sql/node/merge/start";
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final PlanNodeStreamerVisitor planNodeStreamerVisitor;

    @Inject
    public TransportMergeNodeAction(TransportService transportService, ClusterService clusterService,
                                    ReferenceResolver referenceResolver,
                                    Functions functions) {
        this.transportService = transportService;
        this.clusterService = clusterService;

        final ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver, functions, RowGranularity.DOC
        );

        planNodeStreamerVisitor = new PlanNodeStreamerVisitor(functions);
        this.contextManager = new DistributedRequestContextManager(new DownstreamOperationFactory<MergeNode>() {
            @Override
            public DownstreamOperation create(MergeNode node) {
                return new MergeOperation(implementationSymbolVisitor, node);
            }
        }, functions);

        transportService.registerHandler(startMergeAction, new StartMergeHandler());

        // TODO: extract the MergeRowsHandler and make it a generic DistributedRequestHandler
        transportService.registerHandler(mergeRowsAction, new MergeRowsHandler());
    }

    public void startMerge(String node, NodeMergeRequest request, ActionListener<NodeMergeResponse> listener) {
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
            // TODO: optimize if local

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
        private final DataType.Streamer<?>[] streamers;

        public AsyncMergeStartAction(String node, NodeMergeRequest request, ActionListener<NodeMergeResponse> listener) {
            this.node = clusterService.state().nodes().get(node);
            this.request = request;
            this.listener = listener;
            this.streamers = planNodeStreamerVisitor.process(request.mergeNode()).outputStreamers();
        }

        public void start() {
            // TODO: optimize if local

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
                    logger.error(e.getMessage(), e);
                }
            });
        }

        @Override
        public String executor() {
            return executorName();
        }
    }

    // TODO: extract handler and make contextManager injectable so that the handler can be re-used
    private class MergeRowsHandler extends BaseTransportRequestHandler<DistributedResultRequest> {
        @Override
        public DistributedResultRequest newInstance() {
            return new DistributedResultRequest(contextManager);
        }

        @Override
        public void messageReceived(DistributedResultRequest request, TransportChannel channel) throws Exception {
            try {
                contextManager.addToContext(request);
                channel.sendResponse(new DistributedResultResponse());
            } catch (Exception ex) {
                channel.sendResponse(ex);
            }
        }

        @Override
        public String executor() {
            return executorName();
        }
    }
}
