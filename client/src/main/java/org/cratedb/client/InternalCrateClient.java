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

package org.cratedb.client;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.*;
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
            TransportClientNodesService nodesService
                              ) {

        this.nodesService = nodesService;

        // Currently we only support the sql action, so this gets registered directly
        MapBuilder<Action, TransportActionNodeProxy> actionsBuilder = new MapBuilder<Action,
                TransportActionNodeProxy>();
        actionsBuilder.put((Action) SQLAction.INSTANCE,
                new TransportActionNodeProxy(settings, SQLAction.INSTANCE, transportService));
        this.actions = actionsBuilder.immutableMap();
    }

    public ActionFuture<SQLResponse> sql(final SQLRequest request) {
        return execute(SQLAction.INSTANCE, request);
    }


    protected <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response,
                    RequestBuilder>> ActionFuture<Response> execute(final Action<Request,
            Response, RequestBuilder> action, final Request request) {
        final TransportActionNodeProxy<Request, Response> proxy = actions.get(action);
        return nodesService.execute(
                new TransportClientNodesService.NodeCallback<ActionFuture<Response>>() {
                    @Override
                    public ActionFuture<Response> doWithNode(DiscoveryNode node) throws
                            ElasticSearchException {
                        return proxy.execute(node, request);
                    }
                });
    }

    public void addTransportAddress(TransportAddress transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
    }
}
