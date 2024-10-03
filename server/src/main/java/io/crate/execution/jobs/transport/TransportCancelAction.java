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

package io.crate.execution.jobs.transport;

import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import io.crate.session.Sessions;
import io.crate.execution.support.MultiActionListener;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;

/**
 * Action that broadcasts a cancel to all *other* nodes.
 **/
public class TransportCancelAction extends TransportAction<CancelRequest, AcknowledgedResponse> {

    public static final String NAME = "internal:crate:pg/cancel";
    public static final ActionType<AcknowledgedResponse> ACTION = new ActionType<>(NAME);
    private final Sessions sessions;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public TransportCancelAction(Sessions sessions,
                                 ClusterService clusterService,
                                 TransportService transportService) {
        super(NAME);
        this.sessions = sessions;
        this.clusterService = clusterService;
        this.transportService = transportService;
        NodeAction<CancelRequest, TransportResponse> nodeAction = this::nodeOperation;
        transportService.registerRequestHandler(
            NAME,
            ThreadPool.Names.SAME, // operation is cheap
            true,
            false,
            CancelRequest::new,
            new NodeActionRequestHandler<>(nodeAction)
        );
    }

    protected void doExecute(CancelRequest request, ActionListener<AcknowledgedResponse> finalListener) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        var multiActionListener = new MultiActionListener<>(
            nodes.getSize() - 1, // localNode is excluded
            () -> null,
            (x, y) -> {},
            state -> new AcknowledgedResponse(true),
            finalListener
        );
        for (var node : nodes) {
            if (node.equals(clusterService.localNode())) {
                continue;
            }
            transportService.sendRequest(
                node,
                NAME,
                request,
                new ActionListenerResponseHandler<>(multiActionListener, AcknowledgedResponse::new)
            );
        }
    }

    CompletableFuture<TransportResponse> nodeOperation(CancelRequest request) {
        sessions.cancelLocally(request.keyData());
        return CompletableFuture.completedFuture(TransportResponse.Empty.INSTANCE);
    }
}
