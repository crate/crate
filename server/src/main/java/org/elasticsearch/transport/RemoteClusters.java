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

import io.crate.common.io.IOUtils;
import io.crate.protocols.postgres.PgClientFactory;
import io.crate.replication.logical.metadata.ConnectionInfo;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


public class RemoteClusters implements Closeable {

    private final Settings settings;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final Map<String, RemoteCluster> remoteClusters = new HashMap<>();
    private final PgClientFactory pgClientFactory;
    private boolean closed;

    public RemoteClusters(Settings settings,
                          ThreadPool threadPool,
                          PgClientFactory pgClientFactory,
                          TransportService transportService) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.pgClientFactory = pgClientFactory;
        this.transportService = transportService;
    }

    @Override
    public synchronized void close() throws IOException {
        IOUtils.close(remoteClusters.values());
        remoteClusters.clear();
        closed = true;
    }

    /**
     * Get a client that is already connected.
     * Use {@link #connect(String, ConnectionInfo)} to create or get a client.
     *
     * @throws NoSuchRemoteClusterException if there is no connected client for the subscription
     **/
    public synchronized Client getClient(String subscriptionName) {
        if (closed) {
            throw new AlreadyClosedException("RemoteClusters is closed");
        }
        RemoteCluster remoteCluster = remoteClusters.get(subscriptionName);
        if (remoteCluster == null) {
            throw new NoSuchRemoteClusterException(subscriptionName);
        }
        return remoteCluster.client();
    }

    public synchronized CompletableFuture<Client> connect(String name,
                                                          ConnectionInfo connectionInfo) {
        if (closed) {
            return CompletableFuture.failedFuture(new AlreadyClosedException("RemoteClusters is closed"));
        }
        RemoteCluster remoteCluster = this.remoteClusters.get(name);
        if (remoteCluster == null) {
            remoteCluster = new RemoteCluster(
                name,
                settings,
                connectionInfo,
                pgClientFactory,
                threadPool,
                transportService
            );
            remoteClusters.put(name, remoteCluster);
        }
        return remoteCluster.connectAndGetClient();
    }

    public synchronized void remove(String subscriptionName) {
        RemoteCluster remoteCluster = remoteClusters.remove(subscriptionName);
        if (remoteCluster != null) {
            IOUtils.closeWhileHandlingException(remoteCluster);
        }
    }
}
