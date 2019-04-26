/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.jobs.kill;

import io.crate.execution.jobs.TasksService;
import io.crate.execution.support.MultiActionListener;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

abstract class TransportKillNodeAction<Request extends TransportRequest> implements NodeAction<Request, KillResponse>, Callable<Request> {

    protected final TasksService tasksService;
    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final String name;

    TransportKillNodeAction(String name,
                            TasksService tasksService,
                            ClusterService clusterService,
                            TransportService transportService,
                            Supplier<Request> requestSupplier) {
        this.tasksService = tasksService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.name = name;
        transportService.registerRequestHandler(
            name,
            requestSupplier,
            ThreadPool.Names.GENERIC,
            new NodeActionRequestHandler<>(this));
    }

    protected abstract CompletableFuture<Integer> doKill(Request request);

    @Override
    public CompletableFuture<KillResponse> nodeOperation(Request request) {
        return doKill(request).thenApply(KillResponse::new);
    }

    /**
     * Broadcasts the given kill request to all nodes in the cluster
     */
    public void broadcast(Request request, ActionListener<Long> listener) {
        broadcast(request, listener, Collections.emptyList());
    }

    public void broadcast(Request request, ActionListener<Long> listener, Collection<String> excludedNodeIds) {
        Stream<DiscoveryNode> nodes = StreamSupport.stream(clusterService.state().nodes().spliterator(), false);
        Collection<DiscoveryNode> filteredNodes = nodes.filter(node -> !excludedNodeIds.contains(node.getId())).collect(Collectors.toList());

        MultiActionListener<KillResponse, ?, Long> multiListener =
            new MultiActionListener<>(filteredNodes.size(), Collectors.summingLong(KillResponse::numKilled), listener);

        TransportResponseHandler<KillResponse> responseHandler =
            new ActionListenerResponseHandler<>(multiListener, () -> new KillResponse(0));
        for (DiscoveryNode node : filteredNodes) {
            transportService.sendRequest(node, name, request, responseHandler);
        }
    }
}
