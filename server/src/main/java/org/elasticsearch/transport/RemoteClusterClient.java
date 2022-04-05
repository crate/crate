/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public final class RemoteClusterClient extends AbstractClient {

    private final TransportService service;
    private final RemoteClusterConnection remoteClusterConnection;

    public RemoteClusterClient(Settings settings,
                               ThreadPool threadPool,
                               TransportService service,
                               RemoteClusterConnection remoteClusterConnection) {
        super(settings, threadPool);
        this.service = service;
        this.remoteClusterConnection = remoteClusterConnection;
    }

    @Override
    protected <Request extends TransportRequest, Response extends TransportResponse> void doExecute(ActionType<Response> action,
                                                                                                    Request request,
                                                                                                    ActionListener<Response> listener) {
        ActionListener<Void> onConnection = ActionListener.wrap(
            v -> {
                var connection = request instanceof RemoteClusterAwareRequest remoteClusterAware
                    ? remoteClusterConnection.getConnection(remoteClusterAware.getPreferredTargetNode())
                    : remoteClusterConnection.getConnection();
                service.sendRequest(
                    connection,
                    action.name(),
                    request,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(listener, action.getResponseReader())
                );
            },
            listener::onFailure
        );
        remoteClusterConnection.ensureConnected(onConnection);
    }

    @Override
    public void close() {
        // do nothing
    }
}
