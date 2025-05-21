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

package io.crate.execution.engine.collect.stats;

import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.Transports;
import io.crate.expression.reference.sys.node.NodeStatsContext;
import io.crate.expression.reference.sys.node.NodeStatsContextFieldResolver;

@Singleton
public class TransportNodeStatsAction extends TransportAction<NodeStatsRequest, NodeStatsResponse> {

    private final NodeStatsContextFieldResolver nodeContextFieldsResolver;
    private final Transports transports;

    @Inject
    public TransportNodeStatsAction(TransportService transportService,
                                    NodeStatsContextFieldResolver nodeContextFieldsResolver,
                                    Transports transports) {
        super(NodeStatsAction.NAME);
        this.nodeContextFieldsResolver = nodeContextFieldsResolver;
        this.transports = transports;
        transportService.registerRequestHandler(
            NodeStatsAction.NAME,
            ThreadPool.Names.MANAGEMENT,
            NodeStatsRequest.StatsRequest::new,
            new NodeActionRequestHandler<>(this::nodeOperation)
        );
    }

    @Override
    public void doExecute(NodeStatsRequest request, ActionListener<NodeStatsResponse> listener) {
        TransportRequestOptions options = new TransportRequestOptions(request.timeout());
        transports.sendRequest(
            NodeStatsAction.NAME,
            request.nodeId(),
            request.innerRequest(),
            listener,
            new ActionListenerResponseHandler<>(NodeStatsAction.NAME, listener, NodeStatsResponse::new),
            options
        );
    }

    public CompletableFuture<NodeStatsResponse> nodeOperation(NodeStatsRequest.StatsRequest request) {
        try {
            NodeStatsContext context = nodeContextFieldsResolver.forTopColumnIdents(request.columnIdents());
            return CompletableFuture.completedFuture(new NodeStatsResponse(context));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }
}
