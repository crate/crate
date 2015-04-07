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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Streamer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.exceptions.Exceptions;
import io.crate.metadata.Functions;
import io.crate.operation.collect.CollectContextService;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.fetch.NodeFetchOperation;
import io.crate.planner.symbol.Reference;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

@Singleton
public class TransportFetchNodeAction {

    private final String transportAction = "crate/sql/node/fetch";
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final StatsTables statsTables;
    private final CircuitBreaker circuitBreaker;
    private final CollectContextService collectContextService;
    private final String executorName = ThreadPool.Names.SEARCH;
    private final ThreadPool threadPool;
    private final Functions functions;

    @Inject
    public TransportFetchNodeAction(TransportService transportService,
                                    ThreadPool threadPool,
                                    ClusterService clusterService,
                                    StatsTables statsTables,
                                    Functions functions,
                                    CircuitBreakerService breakerService,
                                    CollectContextService collectContextService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.statsTables = statsTables;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.collectContextService = collectContextService;
        this.threadPool = threadPool;
        this.functions = functions;

        transportService.registerHandler(transportAction, new TransportHandler());
    }

    public void execute(
            String targetNode,
            NodeFetchRequest request,
            ActionListener<NodeFetchResponse> listener) {
        new AsyncAction(targetNode, request, listener).start();
    }

    private void nodeOperation(final NodeFetchRequest request,
                               final ActionListener<NodeFetchResponse> fetchResponse) {
        final UUID operationId = UUID.randomUUID();
        statsTables.operationStarted(operationId, request.jobId(), "fetch");
        String ramAccountingContextId = String.format("%s: %s", request.jobId(), operationId);
        final RamAccountingContext ramAccountingContext = new RamAccountingContext(ramAccountingContextId, circuitBreaker);

        NodeFetchOperation fetchOperation = new NodeFetchOperation(
                request.jobId(),
                request.jobSearchContextDocIds(),
                request.toFetchReferences(),
                request.closeContext(),
                collectContextService,
                threadPool,
                functions,
                ramAccountingContext);

        ListenableFuture<Bucket> result;
        try {
            result = fetchOperation.fetch();
        } catch (Throwable t) {
            fetchResponse.onFailure(t);
            statsTables.operationFinished(operationId, Exceptions.messageOf(t),
                    ramAccountingContext.totalBytes());
            ramAccountingContext.close();
            return;

        }

        Futures.addCallback(result, new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(@Nullable Bucket result) {
                assert result != null;
                NodeFetchResponse response = new NodeFetchResponse(outputStreamers(request.toFetchReferences()));
                response.rows(result);

                fetchResponse.onResponse(response);
                statsTables.operationFinished(operationId, null, ramAccountingContext.totalBytes());
                ramAccountingContext.close();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                fetchResponse.onFailure(t);
                statsTables.operationFinished(operationId, Exceptions.messageOf(t),
                        ramAccountingContext.totalBytes());
                ramAccountingContext.close();
            }
        });

    }

    private static Streamer<?>[] outputStreamers(List<Reference> toFetchReferences) {
        Streamer<?>[] streamers = new Streamer<?>[toFetchReferences.size()];
        for (int i = 0; i < toFetchReferences.size(); i++) {
            streamers[i] = toFetchReferences.get(i).valueType().streamer();
        }
        return streamers;
    }


    private class AsyncAction {

        private final NodeFetchRequest request;
        private final ActionListener<NodeFetchResponse> listener;
        private final Streamer<?>[] streamers;
        private final DiscoveryNode node;
        private final String nodeId;
        private final ClusterState clusterState;

        private AsyncAction(String nodeId, NodeFetchRequest request, ActionListener<NodeFetchResponse> listener) {
            Preconditions.checkNotNull(nodeId, "nodeId is null");
            clusterState = clusterService.state();
            node = clusterState.nodes().get(nodeId);
            Preconditions.checkNotNull(node, "DiscoveryNode for id '%s' not found in cluster state", nodeId);

            this.nodeId = nodeId;
            this.request = request;
            this.listener = listener;
            this.streamers = outputStreamers(request.toFetchReferences());
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
                        new DefaultTransportResponseHandler<NodeFetchResponse>(listener, executorName) {
                            @Override
                            public NodeFetchResponse newInstance() {
                                return new NodeFetchResponse(streamers);
                            }
                        }
                );
            }
        }

    }

    private class TransportHandler extends BaseTransportRequestHandler<NodeFetchRequest> {

        @Override
        public NodeFetchRequest newInstance() {
            return new NodeFetchRequest();
        }

        @Override
        public void messageReceived(final NodeFetchRequest request, final TransportChannel channel) throws Exception {
            ActionListener<NodeFetchResponse> actionListener = ResponseForwarder.forwardTo(channel);
            nodeOperation(request, actionListener);
        }

        @Override
        public String executor() {
            return executorName;
        }
    }

}
