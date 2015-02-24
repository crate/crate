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
import io.crate.operation.collect.DistributingCollectOperation;
import io.crate.operation.collect.LocalCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;

public class TransportCollectNodeAction {

    private static final ESLogger logger = Loggers.getLogger(TransportCollectNodeAction.class);

    private final String transportAction = "crate/sql/node/collect";
    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final LocalCollectOperation localDataCollector;
    private final PlanNodeStreamerVisitor planNodeStreamerVisitor;
    private final String executor = ThreadPool.Names.SEARCH;
    private final DistributingCollectOperation distributingCollectOperation;
    private final StatsTables statsTables;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public TransportCollectNodeAction(ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      LocalCollectOperation localDataCollector,
                                      DistributingCollectOperation distributingCollectOperation,
                                      PlanNodeStreamerVisitor planNodeStreamerVisitor,
                                      StatsTables statsTables,
                                      CrateCircuitBreakerService breakerService) {
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.localDataCollector = localDataCollector;
        this.distributingCollectOperation = distributingCollectOperation;
        this.planNodeStreamerVisitor = planNodeStreamerVisitor;
        this.statsTables = statsTables;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);

        transportService.registerHandler(transportAction, new TransportHandler());
    }

    public void execute(
            String targetNode,
            NodeCollectRequest request,
            ActionListener<NodeCollectResponse> listener) {
        new AsyncAction(targetNode, request, listener).start();
    }

    private void nodeOperation(final NodeCollectRequest request,
                               final ActionListener<NodeCollectResponse> collectResponse) {
        final CollectNode node = request.collectNode();
        final ListenableFuture<Bucket> collectResult;

        final UUID operationId;
        if (node.jobId().isPresent()) {
            operationId = UUID.randomUUID();
            statsTables.operationStarted(operationId, node.jobId().get(), node.id());
        } else {
            collectResponse.onFailure(new IllegalArgumentException("no jobId given for CollectOperation"));
            return;
        }
        String ramAccountingContextId = String.format("%s: %s", node.id(), operationId);
        final RamAccountingContext ramAccountingContext = new RamAccountingContext(ramAccountingContextId, circuitBreaker);

        try {
            if (node.hasDownstreams()) {
                collectResult = distributingCollectOperation.collect(node, ramAccountingContext);
            } else {
                collectResult = localDataCollector.collect(node, ramAccountingContext);
            }
        } catch (Throwable e){
            logger.error("Error when creating result futures", e);
            collectResponse.onFailure(e);
            statsTables.operationFinished(operationId, Exceptions.messageOf(e),
                    ramAccountingContext.totalBytes());
            ramAccountingContext.close();
            return;
        }

        Futures.addCallback(collectResult, new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(@Nullable Bucket result) {
                assert result != null;
                NodeCollectResponse response = new NodeCollectResponse(
                        planNodeStreamerVisitor.process(node, ramAccountingContext).outputStreamers());
                response.rows(result);

                collectResponse.onResponse(response);
                statsTables.operationFinished(operationId, null, ramAccountingContext.totalBytes());
                ramAccountingContext.close();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                collectResponse.onFailure(t);
                statsTables.operationFinished(operationId, Exceptions.messageOf(t),
                        ramAccountingContext.totalBytes());
                ramAccountingContext.close();
            }
        });
    }

    private class AsyncAction {

        private final NodeCollectRequest request;
        private final ActionListener<NodeCollectResponse> listener;
        private final Streamer<?>[] streamers;
        private final DiscoveryNode node;
        private final String nodeId;
        private final ClusterState clusterState;

        private AsyncAction(String nodeId, NodeCollectRequest request, ActionListener<NodeCollectResponse> listener) {
            Preconditions.checkNotNull(nodeId, "nodeId is null");
            clusterState = clusterService.state();
            node = clusterState.nodes().get(nodeId);
            Preconditions.checkNotNull(node, "DiscoveryNode for id '%s' not found in cluster state", nodeId);

            this.nodeId = nodeId;
            this.request = request;
            this.listener = listener;
            PlanNodeStreamerVisitor.Context streamerContext = planNodeStreamerVisitor.process(
                    request.collectNode(),
                    new RamAccountingContext("dummy", circuitBreaker));
            this.streamers = streamerContext.outputStreamers();
        }

        private void start() {
            if (nodeId.equals("_local") || nodeId.equals(clusterState.nodes().localNodeId())) {
                threadPool.executor(executor).execute(new Runnable() {
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
                        new DefaultTransportResponseHandler<NodeCollectResponse>(listener, executor) {
                            @Override
                            public NodeCollectResponse newInstance() {
                                return new NodeCollectResponse(streamers);
                            }
                        }
                );
            }
        }

    }

    private class TransportHandler extends BaseTransportRequestHandler<NodeCollectRequest> {

        @Override
        public NodeCollectRequest newInstance() {
            return new NodeCollectRequest();
        }

        @Override
        public void messageReceived(final NodeCollectRequest request, final TransportChannel channel) throws Exception {
            ActionListener<NodeCollectResponse> actionListener = ResponseForwarder.forwardTo(channel);
            nodeOperation(request, actionListener);
        }

        @Override
        public String executor() {
            return executor;
        }
    }
}
