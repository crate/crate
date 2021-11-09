
package org.elasticsearch.transport;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.io.IOUtils;


public class RemoteClusters implements Closeable {

    private final Settings settings;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ConcurrentMap<String, RemoteCluster> remoteClusters = new ConcurrentHashMap<>();

    //
    // TODO: Instead of grouping the remote clusters by subscription we could de-dup them
    // and group by unique connection information to reduce the amount of connections in case
    // there are many subscriptions against a single remote cluster
    //
    // TODO: Remove remote connections if subscription is removed
    //

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

    public RemoteClusterClient getClient(String subscriptionName) {
        RemoteCluster remoteCluster = remoteClusters.get(subscriptionName);
        if (remoteCluster == null) {
            throw new IllegalArgumentException("No remote cluster with name [" + subscriptionName + "]");
        }
        return new RemoteClusterClient(settings, threadPool, transportService, remoteCluster);
    }

    public CompletableFuture<RemoteClusterClient> connectAndGetClient(String subscriptionName,
                                                                      Settings settings) {
        RemoteCluster remoteCluster = remoteClusters.get(subscriptionName);
        if (remoteCluster == null) {
            remoteCluster = new RemoteCluster(settings);
            remoteClusters.put(subscriptionName, remoteCluster);
        }
        return remoteCluster.getConnection(null)
            .thenApply(ignoredConnection -> getClient(subscriptionName));
    }
}
