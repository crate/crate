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

package io.crate.execution.support;

import io.crate.action.FutureActionListener;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


@Singleton
public class Transports {

    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public Transports(ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    /**
     * Convenience to turn
     *
     *      `action.execute(request, listener) -> void`
     *
     *  into
     *
     *      `execute(action, request, convertResp) -> resultFuture`
     */
    public static <Req extends TransportRequest, Resp extends TransportResponse, Result> CompletableFuture<Result> execute(
        TransportAction<Req, Resp> transportAction,
        Req request,
        Function<? super Resp, Result> convertResponse) {

        FutureActionListener<Resp, Result> listener = new FutureActionListener<>(convertResponse);
        transportAction.execute(request, listener);
        return listener;
    }

    public <TRequest extends TransportRequest, TResponse extends TransportResponse> void sendRequest(
        String action,
        String node,
        TRequest request,
        ActionListener<TResponse> listener,
        TransportResponseHandler<TResponse> handler,
        TransportRequestOptions options) {

        DiscoveryNode discoveryNode = clusterService.state().nodes().get(node);
        if (discoveryNode == null) {
            listener.onFailure(new NodeNotConnectedException(null,
                String.format(Locale.ENGLISH, "node \"%s\" not found in cluster state!", node)));
            return;
        }
        transportService.sendRequest(discoveryNode, action, request, options, handler);
    }

    public <TRequest extends TransportRequest, TResponse extends TransportResponse> void sendRequest(
        String action,
        String node,
        TRequest request,
        ActionListener<TResponse> listener,
        TransportResponseHandler<TResponse> handler) {
        sendRequest(action, node, request, listener, handler, TransportRequestOptions.EMPTY);
    }
}
