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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.RejectedExecutionException;


@Singleton
public class Transports {

    private static final ESLogger LOGGER = Loggers.getLogger(Transports.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ThreadPool threadPool;

    @Inject
    public Transports(ClusterService clusterService, TransportService transportService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
    }

    public <TRequest extends TransportRequest, TResponse extends TransportResponse> void executeLocalOrWithTransport(
            final NodeAction<TRequest, TResponse> nodeAction,
            String node,
            final TRequest request,
            final ActionListener<TResponse> listener,
            TransportResponseHandler<TResponse> transportResponseHandler) {
        DiscoveryNode discoveryNode = clusterService.state().nodes().get(node);
        if (discoveryNode == null) {
            throw new IllegalArgumentException(String.format("node \"%s\" not found in cluster state!", node));
        }
        executeLocalOrWithTransport(nodeAction, discoveryNode, request, listener, transportResponseHandler);
    }

    public <TRequest extends TransportRequest, TResponse extends TransportResponse> void executeLocalOrWithTransport(
            final NodeAction<TRequest, TResponse> nodeAction,
            DiscoveryNode node,
            final TRequest request,
            final ActionListener<TResponse> listener,
            TransportResponseHandler<TResponse> transportResponseHandler) {

        ClusterState clusterState = clusterService.state();
        if (node.id().equals("_local") || node.equals(clusterState.nodes().localNode())) {
            try {
                threadPool.executor(nodeAction.executorName()).execute(new Runnable() {
                    @Override
                    public void run() {
                        nodeAction.nodeOperation(request, listener);
                    }
                });
            } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Couldn't execute {} locally on node {}", e, nodeAction.getClass().getSimpleName(), node);
                }
                listener.onFailure(e);
            }
        } else {
            transportService.sendRequest(
                    node, nodeAction.actionName(), request, transportResponseHandler);
        }
    }
}
