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

package io.crate.client;

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.TransportService;

public class InternalCrateClient {

    private final ImmutableMap<Action, TransportActionNodeProxy> actions;
    private final TransportClientNodesService nodesService;

    @Inject
    public InternalCrateClient(Settings settings,
                               TransportService transportService,
                               TransportClientNodesService nodesService) {

        this.nodesService = nodesService;

        MapBuilder<Action, TransportActionNodeProxy> actionsBuilder = new MapBuilder<>();
        actionsBuilder.put(SQLAction.INSTANCE,
                           new TransportActionNodeProxy<>(settings, SQLAction.INSTANCE, transportService))
                      .put(SQLBulkAction.INSTANCE,
                           new TransportActionNodeProxy<>(settings, SQLBulkAction.INSTANCE, transportService));
        this.actions = actionsBuilder.immutableMap();
    }

    public ActionFuture<SQLResponse> sql(final SQLRequest request) {
        return execute(SQLAction.INSTANCE, request);
    }

    public ActionFuture<SQLBulkResponse> bulkSql(final SQLBulkRequest bulkRequest) {
        return execute(SQLBulkAction.INSTANCE, bulkRequest);
    }

    protected <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>, Client extends ElasticsearchClient> ActionFuture<Response> execute(final Action<Request,
            Response, RequestBuilder, Client> action, final Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    public void sql(final SQLRequest request, final ActionListener<SQLResponse> listener) {
        execute(SQLAction.INSTANCE, request, listener);
    }

    public void bulkSql(final SQLBulkRequest bulkRequest, final ActionListener<SQLBulkResponse> listener) {
        execute(SQLBulkAction.INSTANCE, bulkRequest, listener);
    }

    protected <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response,
                    RequestBuilder, Client>, Client extends ElasticsearchClient> void execute(final Action<Request,
            Response, RequestBuilder, Client> action, final Request request, final ActionListener<Response> listener) {
        final TransportActionNodeProxy<Request, Response> proxy = actions.get(action);
        nodesService.execute(
            new TransportClientNodesService.NodeListenerCallback<Response>() {
                @Override
                public void doWithNode(DiscoveryNode node, ActionListener<Response> listener) throws
                        ElasticsearchException {
                    proxy.execute(node, request, listener);
                }
            }, listener);
    }

    public void addTransportAddress(TransportAddress transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
    }

    public void close() {
        nodesService.close();
    }
}
