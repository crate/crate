/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.jobs.kill;

import static io.crate.execution.jobs.kill.TransportKillNodeAction.broadcast;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.jobs.TasksService;
import io.crate.execution.support.NodeActionRequestHandler;

@Singleton
public class TransportKillAllNodeAction extends TransportAction<KillAllRequest, KillResponse> {

    protected final TasksService tasksService;
    protected final ClusterService clusterService;
    protected final TransportService transportService;

    @Inject
    public TransportKillAllNodeAction(TasksService tasksService,
                                      ClusterService clusterService,
                                      TransportService transportService) {
        super(KillAllNodeAction.NAME);
        this.tasksService = tasksService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        transportService.registerRequestHandler(
            KillAllNodeAction.NAME,
            ThreadPool.Names.GENERIC,
            KillAllRequest::new,
            new NodeActionRequestHandler<>(this::nodeOperation));
    }

    @Override
    public void doExecute(KillAllRequest request, ActionListener<KillResponse> listener) {
        broadcast(clusterService, transportService, request, KillAllNodeAction.NAME, listener, List.of());
    }

    public CompletableFuture<KillResponse> nodeOperation(KillAllRequest request) {
        return tasksService.killAll(request.userName()).thenApply(KillResponse::new);
    }
}
