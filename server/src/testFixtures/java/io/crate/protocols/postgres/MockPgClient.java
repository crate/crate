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

package io.crate.protocols.postgres;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.test.transport.StubbableTransport.OpenConnectionBehavior;
import org.elasticsearch.test.transport.StubbableTransport.SendRequestBehavior;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;

import io.crate.action.FutureActionListener;

public class MockPgClient extends PgClient {

    private final StubbableTransport stubTransport;

    public MockPgClient(PgClient pgClient) {
        super(
            pgClient.name,
            pgClient.settings(),
            pgClient.transportService,
            pgClient.nettyBootstrap,
            pgClient.transport,
            pgClient.sslContextProvider,
            pgClient.pageCacheRecycler,
            pgClient.connectionInfo);
        // MockPgClient must only be used if the TransportService is a MockTransportService
        this.stubTransport = (StubbableTransport) ((MockTransportService ) pgClient.transportService).transport();
    }

    @Override
    public CompletableFuture<Connection> ensureConnected() {
        OpenConnectionBehavior openConnectionBehavior = stubTransport.connectBehaviors.get(host.getAddress());
        if (openConnectionBehavior == null) {
            return super.ensureConnected().thenApply(WrappedConnection::new);
        }
        var connectionListener = new FutureActionListener<Connection>();
        openConnectionBehavior.openConnection(transport, host, profile, connectionListener);
        return connectionListener.thenApply(WrappedConnection::new);
    }

    class WrappedConnection implements Connection {

        private final Connection connection;

        public WrappedConnection(Connection connection) {
            this.connection = connection;
        }

        public DiscoveryNode getNode() {
            return connection.getNode();
        }

        public void sendRequest(long requestId,
                                String action,
                                TransportRequest request,
                                TransportRequestOptions options) throws IOException, TransportException {
            TransportAddress address = connection.getNode().getAddress();
            SendRequestBehavior behavior = stubTransport.sendBehaviors.getOrDefault(
                address,
                stubTransport.defaultSendRequest
            );
            if (behavior == null) {
                connection.sendRequest(requestId, action, request, options);
            } else {
                behavior.sendRequest(connection, requestId, action, request, options);
            }
        }

        public void addCloseListener(ActionListener<Void> listener) {
            connection.addCloseListener(listener);
        }

        public boolean isClosed() {
            return connection.isClosed();
        }

        public Version getVersion() {
            return connection.getVersion();
        }

        public Object getCacheKey() {
            return connection.getCacheKey();
        }

        public void close() {
            connection.close();
        }
    }
}
