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

package io.crate.executor.transport;

import com.google.common.base.Preconditions;
import io.crate.exceptions.Exceptions;
import io.crate.operation.collect.CollectContextService;
import io.crate.operation.collect.StatsTables;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.UUID;

/**
 * Transport handler for closing a context(lucene) for a complete job.
 * This is currently ONLY used by the {@link io.crate.operation.projectors.FetchProjector}
 * and should NOT be re-used somewhere else.
 * We will refactor this architecture later on, so a fetch projection will only close related
 * operation contexts and NOT a whole job context.
 * (This requires a refactoring of the {@link CollectContextService} architecture)
 */
@Singleton
public class TransportCloseContextNodeAction {

    private final String transportAction = "crate/sql/node/context/close";
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final StatsTables statsTables;
    private final CollectContextService collectContextService;
    private final String executorName = ThreadPool.Names.SEARCH;
    private final ThreadPool threadPool;

    @Inject
    public TransportCloseContextNodeAction(TransportService transportService,
                                    ThreadPool threadPool,
                                    ClusterService clusterService,
                                    StatsTables statsTables,
                                    CollectContextService collectContextService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.statsTables = statsTables;
        this.collectContextService = collectContextService;
        this.threadPool = threadPool;

        transportService.registerHandler(transportAction, new TransportHandler());
    }

    public void execute(
            String targetNode,
            NodeCloseContextRequest request,
            ActionListener<NodeCloseContextResponse> listener) {
        new AsyncAction(targetNode, request, listener).start();
    }

    private void nodeOperation(final NodeCloseContextRequest request,
                               final ActionListener<NodeCloseContextResponse> response) {
        final UUID operationId = UUID.randomUUID();
        statsTables.operationStarted(operationId, request.jobId(), "closeContext");

        try {
            collectContextService.closeContext(request.jobId());
            statsTables.operationFinished(operationId, null, 0);
            response.onResponse(new NodeCloseContextResponse());
        } catch (Exception e) {
            statsTables.operationFinished(operationId, Exceptions.messageOf(e), 0);
            response.onFailure(e);
        }
    }

    private class AsyncAction {

        private final NodeCloseContextRequest request;
        private final ActionListener<NodeCloseContextResponse> listener;
        private final DiscoveryNode node;
        private final String nodeId;
        private final ClusterState clusterState;

        private AsyncAction(String nodeId, NodeCloseContextRequest request, ActionListener<NodeCloseContextResponse> listener) {
            Preconditions.checkNotNull(nodeId, "nodeId is null");
            clusterState = clusterService.state();
            node = clusterState.nodes().get(nodeId);
            Preconditions.checkNotNull(node, "DiscoveryNode for id '%s' not found in cluster state", nodeId);

            this.nodeId = nodeId;
            this.request = request;
            this.listener = listener;
        }

        private void start() {
            if (nodeId.equals("_local") || nodeId.equals(clusterState.nodes().localNodeId())) {
                threadPool.executor(executorName).execute(new Runnable() {
                    @Override
                    public void run() {
                        nodeOperation(request, listener);
                    }
                });
            } else {
                transportService.sendRequest(
                        node,
                        transportAction,
                        request,
                        new DefaultTransportResponseHandler<NodeCloseContextResponse>(listener, executorName) {
                            @Override
                            public NodeCloseContextResponse newInstance() {
                                return new NodeCloseContextResponse();
                            }
                        }
                );
            }
        }

    }

    private class TransportHandler extends BaseTransportRequestHandler<NodeCloseContextRequest> {

        @Override
        public NodeCloseContextRequest newInstance() {
            return new NodeCloseContextRequest();
        }

        @Override
        public void messageReceived(final NodeCloseContextRequest request, final TransportChannel channel) throws Exception {
            ActionListener<NodeCloseContextResponse> actionListener = ResponseForwarder.forwardTo(channel);
            nodeOperation(request, actionListener);
        }

        @Override
        public String executor() {
            return executorName;
        }
    }

}
