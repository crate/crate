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
import io.crate.operation.collect.DistributingCollectOperation;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import io.crate.Streamer;
import io.crate.exceptions.CrateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import javax.annotation.Nullable;
import java.io.IOException;

public class TransportCollectNodeAction {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final String transportAction = "crate/sql/node/collect";
    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final MapSideDataCollectOperation localDataCollector;
    private final PlanNodeStreamerVisitor planNodeStreamerVisitor;
    private final String executor = ThreadPool.Names.SEARCH;
    private final DistributingCollectOperation distributingCollectOperation;

    @Inject
    public TransportCollectNodeAction(ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      MapSideDataCollectOperation localDataCollector,
                                      DistributingCollectOperation distributingCollectOperation,
                                      PlanNodeStreamerVisitor planNodeStreamerVisitor) {
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.localDataCollector = localDataCollector;
        this.distributingCollectOperation = distributingCollectOperation;
        this.planNodeStreamerVisitor = planNodeStreamerVisitor;

        transportService.registerHandler(transportAction, new TransportHandler());
    }

    public void execute(
            String targetNode,
            NodeCollectRequest request,
            ActionListener<NodeCollectResponse> listener) {
        new AsyncAction(targetNode, request, listener).start();
    }

    protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    private ListenableActionFuture<NodeCollectResponse> nodeOperation(final NodeCollectRequest request) throws CrateException {
        final CollectNode node = request.collectNode();
        final ListenableFuture<Object[][]> collectResult;
        final PlainListenableActionFuture<NodeCollectResponse> collectResponse = new PlainListenableActionFuture<>(false, threadPool);

        try {
            if (node.hasDownstreams()) {
                collectResult = distributingCollectOperation.collect(node);
            } else {
                collectResult = localDataCollector.collect(node);
            }
        } catch (Exception e){
            logger.error("Error when creating result futures", e);
            collectResponse.onFailure(e);
            return collectResponse;
        }

        Futures.addCallback(collectResult, new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] result) {
                assert result != null;
                PlanNodeStreamerVisitor.Context streamerContext = planNodeStreamerVisitor.process(node);
                NodeCollectResponse response = new NodeCollectResponse(streamerContext.outputStreamers());
                response.rows(result);
                collectResponse.onResponse(response);
            }

            @Override
            public void onFailure(Throwable t) {
                collectResponse.onFailure(t);
            }
        });
        return collectResponse;
    }

    private class AsyncAction {

        private final NodeCollectRequest request;
        private final ActionListener<NodeCollectResponse> listener;
        private final Streamer<?>[] streamers;
        private final DiscoveryNode node;
        private final String nodeId;
        private final ClusterState clusterState;

        private AsyncAction(String nodeId, NodeCollectRequest request, ActionListener<NodeCollectResponse> listener) {
            Preconditions.checkNotNull(nodeId);
            clusterState = clusterService.state();
            node = clusterState.nodes().get(nodeId);
            Preconditions.checkNotNull(node);

            this.nodeId = nodeId;
            this.request = request;
            this.listener = listener;
            PlanNodeStreamerVisitor.Context streamerContext = planNodeStreamerVisitor.process(request.collectNode());
            this.streamers = streamerContext.outputStreamers();
        }

        private void start() {
            if (nodeId.equals("_local") || nodeId.equals(clusterState.nodes().localNodeId())) {
                threadPool.executor(executor).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ListenableActionFuture<NodeCollectResponse> collectResponseFuture = nodeOperation(request);
                            collectResponseFuture.addListener(new ActionListener<NodeCollectResponse>() {
                                @Override
                                public void onResponse(NodeCollectResponse nodeCollectResponse) {
                                    listener.onResponse(nodeCollectResponse);
                                }

                                @Override
                                public void onFailure(Throwable e) {
                                    listener.onFailure(e);
                                }
                            });
                        } catch (Throwable e) {
                            listener.onFailure(e);
                        }
                    }
                });
            } else {
                transportService.sendRequest(
                        node,
                        transportAction,
                        request,
                        new BaseTransportResponseHandler<NodeCollectResponse>() {

                            @Override
                            public NodeCollectResponse newInstance() {
                                return new NodeCollectResponse(streamers);
                            }

                            @Override
                            public void handleResponse(NodeCollectResponse response) {
                                listener.onResponse(response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                listener.onFailure(exp);
                            }

                            @Override
                            public String executor() {
                                return executor;
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
            try {
                nodeOperation(request).addListener(new ActionListener<NodeCollectResponse>() {
                    @Override
                    public void onResponse(NodeCollectResponse response) {
                        try {
                            channel.sendResponse(response);
                        } catch (IOException e) {
                            logger.error("Error sending collect response", e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(e);
                        } catch (IOException e1) {
                            logger.error("Error sending collect failure", e1);
                        }
                    }
                });
            } catch (CrateException e) {
                channel.sendResponse(e);
            }
        }

        @Override
        public String executor() {
            return executor;
        }
    }
}
