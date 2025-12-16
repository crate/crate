/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeShouldNotConnectException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

public abstract class TransportNodesAction<NodesRequest extends BaseNodesRequest<NodesRequest>,
                                           NodesResponse extends BaseNodesResponse,
                                           NodeRequest extends TransportRequest,
                                           NodeResponse extends BaseNodeResponse>
    extends HandledTransportAction<NodesRequest, NodesResponse> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final Class<NodeResponse> nodeResponseClass;

    final String transportNodeAction;

    protected TransportNodesAction(String actionName,
                                   ThreadPool threadPool,
                                   ClusterService clusterService,
                                   TransportService transportService,
                                   Writeable.Reader<NodesRequest> nodesRequestReader,
                                   Writeable.Reader<NodeRequest> nodeRequestReader,
                                   String nodeExecutor,
                                   Class<NodeResponse> nodeResponseClass) {
        super(actionName, transportService, nodesRequestReader);
        this.threadPool = threadPool;
        this.clusterService = Objects.requireNonNull(clusterService);
        this.transportService = Objects.requireNonNull(transportService);
        this.nodeResponseClass = Objects.requireNonNull(nodeResponseClass);

        this.transportNodeAction = actionName + "[n]";

        transportService.registerRequestHandler(
            transportNodeAction, nodeExecutor, nodeRequestReader, new NodeTransportHandler());
    }

    @Override
    protected void doExecute(NodesRequest request, ActionListener<NodesResponse> listener) {
        new AsyncAction(request, listener).start();
    }

    /**
     * Map the responses into {@code nodeResponseClass} responses and {@link FailedNodeException}s.
     *
     * @param request The associated request.
     * @param nodesResponses All node-level responses
     * @return Never {@code null}.
     * @throws NullPointerException if {@code nodesResponses} is {@code null}
     * @see #newResponse(BaseNodesRequest, List, List)
     */
    protected NodesResponse newResponse(NodesRequest request, AtomicReferenceArray nodesResponses) {
        final List<NodeResponse> responses = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();

        for (int i = 0; i < nodesResponses.length(); ++i) {
            Object response = nodesResponses.get(i);

            if (response instanceof FailedNodeException) {
                failures.add((FailedNodeException)response);
            } else {
                responses.add(nodeResponseClass.cast(response));
            }
        }

        return newResponse(request, responses, failures);
    }

    /**
     * Create a new {@link NodesResponse} (multi-node response).
     *
     * @param request The associated request.
     * @param responses All successful node-level responses.
     * @param failures All node-level failures.
     * @return Never {@code null}.
     * @throws NullPointerException if any parameter is {@code null}.
     */
    protected abstract NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures);

    protected abstract NodeRequest newNodeRequest(NodesRequest request);

    protected abstract NodeResponse read(StreamInput in) throws IOException;

    protected abstract NodeResponse nodeOperation(NodeRequest request);

    class AsyncAction {

        private final NodesRequest request;
        private final ActionListener<NodesResponse> listener;
        private final AtomicReferenceArray<Object> responses;
        private final AtomicInteger counter = new AtomicInteger();

        AsyncAction(NodesRequest request, ActionListener<NodesResponse> listener) {
            this.request = request;
            this.listener = listener;
            this.responses = new AtomicReferenceArray<>(request.concreteNodes().length);
        }

        void start() {
            final DiscoveryNode[] nodes = request.concreteNodes();
            if (nodes.length == 0) {
                // nothing to notify
                threadPool.generic().execute(() -> listener.onResponse(newResponse(request, responses)));
                return;
            }
            TransportRequestOptions requestOptions = new TransportRequestOptions(request.timeout());
            for (int i = 0; i < nodes.length; i++) {
                final int idx = i;
                final DiscoveryNode node = nodes[i];
                final String nodeId = node.getId();
                try {
                    TransportRequest nodeRequest = newNodeRequest(request);
                    transportService.sendRequest(
                        node,
                        transportNodeAction,
                        nodeRequest,
                        requestOptions,
                        new TransportResponseHandler<NodeResponse>() {

                            @Override
                            public NodeResponse read(StreamInput in) throws IOException {
                                return TransportNodesAction.this.read(in);
                            }

                            @Override
                            public void handleResponse(NodeResponse response) {
                                onOperation(idx, response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                onFailure(idx, node.getId(), exp);
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }
                        }
                    );
                } catch (Exception e) {
                    onFailure(idx, nodeId, e);
                }
            }
        }

        private void onOperation(int idx, NodeResponse nodeResponse) {
            responses.set(idx, nodeResponse);
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void onFailure(int idx, String nodeId, Throwable t) {
            if (logger.isDebugEnabled() && !(t instanceof NodeShouldNotConnectException)) {
                logger.debug(new ParameterizedMessage("failed to execute on node [{}]", nodeId), t);
            }
            responses.set(idx, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t));
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void finishHim() {
            NodesResponse finalResponse;
            try {
                finalResponse = newResponse(request, responses);
            } catch (Exception e) {
                logger.debug("failed to combine responses from nodes", e);
                listener.onFailure(e);
                return;
            }
            listener.onResponse(finalResponse);
        }
    }

    class NodeTransportHandler implements TransportRequestHandler<NodeRequest> {

        @Override
        public void messageReceived(NodeRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(nodeOperation(request));
        }
    }

}
