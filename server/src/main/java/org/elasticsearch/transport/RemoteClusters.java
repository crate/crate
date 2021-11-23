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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.action.FutureActionListener;
import io.crate.common.io.IOUtils;
import io.crate.replication.logical.metadata.ConnectionInfo;


public class RemoteClusters implements Closeable {

    private final Settings settings;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final Map<String, RemoteClusterClient> remoteClusters = new HashMap<>();

    public RemoteClusters(Settings settings,
                          ThreadPool threadPool,
                          TransportService transportService) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.transportService = transportService;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(remoteClusters.values());
        remoteClusters.clear();
    }

    /**
     * Get a client that is already connected.
     * Use {@link #connect(String, ConnectionInfo)} to create or get a client.
     *
     * @throws NoSuchRemoteClusterException if there is no connected client for the subscription
     **/
    public synchronized Client getClient(String subscriptionName) {
        RemoteClusterClient remoteClusterClient = remoteClusters.get(subscriptionName);
        if (remoteClusterClient == null) {
            throw new NoSuchRemoteClusterException(subscriptionName);
        }
        return remoteClusterClient;
    }

    public synchronized CompletableFuture<Client> connect(String name,
                                                          ConnectionInfo connectionInfo) {
        RemoteClusterClient remoteClient = this.remoteClusters.get(name);
        // The ConnectionManager uses node settings in addition to connection settings
        var connectionSettings = Settings.builder()
            .put(settings)
            .put(connectionInfo.settings())
            .build();

        if (remoteClient == null) {
            // this is a new cluster we have to add a new representation
            var remoteConnection = new RemoteClusterConnection(settings, connectionSettings, name, transportService);
            var newRemoteClient = new RemoteClusterClient(settings, threadPool, transportService, remoteConnection);
            remoteClusters.put(name, newRemoteClient);
            FutureActionListener<Void, Client> listener = new FutureActionListener<>(ignored -> newRemoteClient);
            remoteConnection.ensureConnected(listener);
            return listener;
        } else {
            return CompletableFuture.completedFuture(remoteClient);
        }
    }

    public synchronized void remove(String subscriptionName) {
        RemoteClusterClient remoteClusterClient = remoteClusters.remove(subscriptionName);
        if (remoteClusterClient != null) {
            remoteClusterClient.close();
        }
    }
}
