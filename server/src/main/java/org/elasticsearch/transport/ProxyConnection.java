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

package org.elasticsearch.transport;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.transport.Transport.Connection;

public final class ProxyConnection implements Transport.Connection {

    private final Connection connection;
    private final DiscoveryNode targetNode;

    public ProxyConnection(Transport.Connection connection, DiscoveryNode targetNode) {
        this.connection = connection;
        this.targetNode = targetNode;
    }

    @Override
    public DiscoveryNode getNode() {
        return targetNode;
    }

    @Override
    public void sendRequest(long requestId,
                            String action,
                            TransportRequest request,
                            TransportRequestOptions options) throws IOException, TransportException {
        connection.sendRequest(
            requestId,
            TransportActionProxy.getProxyAction(action),
            TransportActionProxy.wrapRequest(targetNode, request),
            options
        );
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        connection.addCloseListener(listener);
    }

    @Override
    public boolean isClosed() {
        return connection.isClosed();
    }

    @Override
    public void close() {
        assert false : "Proxy connections must not be closed";
    }

    @Override
    public Version getVersion() {
        return connection.getVersion();
    }

    @Override
    public Object getCacheKey() {
        return connection.getCacheKey();
    }
}
