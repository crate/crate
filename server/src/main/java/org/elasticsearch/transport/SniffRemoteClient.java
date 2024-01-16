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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportService.HandshakeResponse;
import org.jetbrains.annotations.Nullable;

import io.crate.action.FutureActionListener;
import io.crate.common.collections.Lists;
import io.crate.common.exceptions.Exceptions;
import io.crate.replication.logical.metadata.ConnectionInfo;

public final class SniffRemoteClient extends AbstractClient {

    private final TransportService transportService;
    private final ConnectionProfile profile;
    private final List<Supplier<DiscoveryNode>> seedNodes;
    private final String clusterAlias;
    private final SetOnce<ClusterName> remoteClusterName = new SetOnce<>();
    private final Predicate<ClusterName> allNodesShareClusterName;

    private CompletableFuture<DiscoveredNodes> discoveredNodes = null;

    public SniffRemoteClient(Settings nodeSettings,
                             ThreadPool threadPool,
                             ConnectionInfo connectionInfo,
                             String clusterAlias,
                             TransportService transportService) {
        super(nodeSettings, threadPool);
        this.clusterAlias = clusterAlias;
        this.transportService = transportService;
        this.seedNodes = Lists.map(
            connectionInfo.hosts(),
            seedNode -> () -> resolveSeedNode(clusterAlias, seedNode)
        );
        this.profile = new ConnectionProfile.Builder()
            .setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .setPingInterval(TransportSettings.PING_SCHEDULE.get(settings))
            .setCompressionEnabled(TransportSettings.TRANSPORT_COMPRESS.get(settings))
            .addConnections(1, TransportRequestOptions.Type.BULK)
            .addConnections(0, TransportRequestOptions.Type.PING)
            .addConnections(1, TransportRequestOptions.Type.STATE)
            .addConnections(1, TransportRequestOptions.Type.RECOVERY)
            .addConnections(1, TransportRequestOptions.Type.REG)
            .build();
        this.allNodesShareClusterName = new Predicate<ClusterName>() {

            @Override
            public boolean test(ClusterName c) {
                ClusterName clusterName = remoteClusterName.get();
                return clusterName == null || c.equals(clusterName);
            }
        };
    }

    @Override
    public void close() {
        synchronized (this) {
            if (discoveredNodes == null) {
                return;
            }
            if (discoveredNodes.isCompletedExceptionally()) {
                discoveredNodes = null;
                return;
            }
            discoveredNodes.thenAccept(nodes -> {
                synchronized (nodes) {
                    var it = nodes.connections.entrySet().iterator();
                    while (it.hasNext()) {
                        DiscoveryNode node = it.next().getKey();
                        transportService.disconnectFromNode(node);
                        it.remove();
                    }
                }
            });
            discoveredNodes = null;
        }
    }

    static class DiscoveredNodes {

        private final HashMap<DiscoveryNode, CompletableFuture<Connection>> connections;
        private final DiscoveryNodes discoveryNodes;

        DiscoveredNodes(Connection connection, DiscoveryNodes discoveryNodes) {
            this.connections = new HashMap<>();
            this.connections.put(connection.getNode(), CompletableFuture.completedFuture(connection));
            this.discoveryNodes = discoveryNodes;
        }

        public boolean hasDiscovered(DiscoveryNode preferredNode) {
            return discoveryNodes.nodeExists(preferredNode);
        }
    }

    private CompletionStage<Connection> getAnyConnection(DiscoveredNodes discoveredNodes) {
        synchronized (discoveredNodes) {
            var it = discoveredNodes.connections.entrySet().iterator();
            List<Connection> closedConnections = new ArrayList<>();
            while (it.hasNext()) {
                var entry = it.next();
                var connection = entry.getValue();
                if (connection.isCompletedExceptionally()) {
                    it.remove();
                    continue;
                } else if (connection.isDone() && connection.join().isClosed()) {
                    closedConnections.add(connection.join());
                    it.remove();
                } else {
                    return connection;
                }
            }
            if (!closedConnections.isEmpty()) {
                Connection conn = closedConnections.iterator().next();
                DiscoveryNode node = conn.getNode();
                var newConn = connectWithHandshake(node);
                discoveredNodes.connections.put(node, newConn);
                return newConn;
            }
        }
        synchronized (this) {
            this.discoveredNodes = null;
            return ensureConnected(null);
        }
    }

    private CompletionStage<Connection> getConnection(DiscoveredNodes nodes, DiscoveryNode preferredNode) {
        synchronized (nodes) {
            CompletableFuture<Connection> conn = nodes.connections.get(preferredNode);
            if (conn == null || conn.isCompletedExceptionally() || (conn.isDone() && conn.join().isClosed())) {
                if (nodes.hasDiscovered(preferredNode)) {
                    CompletableFuture<Connection> newConn = connectWithHandshake(preferredNode);
                    nodes.connections.put(preferredNode, newConn);
                    return newConn;
                } else {
                    return getAnyConnection(nodes).thenApply(c -> new ProxyConnection(c, preferredNode));
                }
            } else {
                return conn;
            }
        }
    }

    public CompletableFuture<Connection> ensureConnected(@Nullable DiscoveryNode preferredNode) {
        synchronized (this) {
            if (discoveredNodes == null) {
                discoveredNodes = discoverNodes();
            } else if (discoveredNodes.isCompletedExceptionally()) {
                discoveredNodes = discoverNodes();
            }
            return discoveredNodes.thenCompose(nodes -> {
                if (preferredNode == null) {
                    return getAnyConnection(nodes);
                } else {
                    return getConnection(nodes, preferredNode);
                }
            });
        }
    }

    private CompletableFuture<DiscoveredNodes> discoverNodes() {
        return tryConnectToAnySeedNode(seedNodes.iterator())
            .thenCompose(this::discoverNodes);
    }

    private CompletableFuture<DiscoveredNodes> discoverNodes(Connection connection) {
        var request = new ClusterStateRequest();
        request.clear();
        request.nodes(true);
        request.local(true);

        CompletableFuture<DiscoveredNodes> result = new CompletableFuture<>();
        transportService.sendRequest(
            connection,
            ClusterStateAction.NAME,
            request,
            TransportRequestOptions.EMPTY,
            new TransportResponseHandler<ClusterStateResponse>() {

                @Override
                public ClusterStateResponse read(StreamInput in) throws IOException {
                    return new ClusterStateResponse(in);
                }

                @Override
                public void handleResponse(ClusterStateResponse response) {
                    DiscoveryNodes nodes = response.getState().nodes();
                    result.complete(new DiscoveredNodes(connection, nodes));
                }

                @Override
                public void handleException(TransportException exp) {
                    connection.close();
                    result.completeExceptionally(exp.unwrapCause());
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
        return result;
    }

    private CompletableFuture<Connection> tryConnectToAnySeedNode(Iterator<Supplier<DiscoveryNode>> seedNodes) {
        if (seedNodes.hasNext()) {
            try {
                DiscoveryNode seedNode = seedNodes.next().get();
                return connectWithHandshake(seedNode)
                    .exceptionallyCompose(err -> {
                        if (seedNodes.hasNext()) {
                            return tryConnectToAnySeedNode(seedNodes);
                        } else {
                            throw Exceptions.toRuntimeException(err);
                        }
                    });
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(t);
            }
        }
        return CompletableFuture.failedFuture(new NoSuchRemoteClusterException(clusterAlias));
    }

    private CompletableFuture<Connection> connectWithHandshake(DiscoveryNode node) {
        FutureActionListener<Connection> openedConnection = new FutureActionListener<>();
        transportService.openConnection(node, profile, openedConnection);

        CompletableFuture<HandshakeResponse> receivedHandshakeResponse = openedConnection.thenCompose(connection -> {
            FutureActionListener<HandshakeResponse> handshakeResponse = new FutureActionListener<>();
            transportService.handshake(
                connection,
                profile.getHandshakeTimeout().millis(),
                allNodesShareClusterName,
                handshakeResponse
            );
            return handshakeResponse;
        });

        return receivedHandshakeResponse.thenCompose(handshakeResponse -> {
            DiscoveryNode handshakeNode = handshakeResponse.getDiscoveryNode();
            if (!nodeIsCompatible(handshakeNode)) {
                throw new IllegalStateException(
                    "Remote publisher node [" + handshakeNode + "] is not compatible with subscriber node (" + transportService.localNode + "). "
                    + "The publisher node must have a compatible version and needs to be a data node");
            }
            remoteClusterName.trySet(handshakeResponse.getClusterName());
            FutureActionListener<Void> connectedListener = new FutureActionListener<>();
            transportService.connectToNode(handshakeNode, connectedListener);
            return connectedListener.thenApply(ignored -> openedConnection.join());
        });
    }

    private static boolean nodeIsCompatible(DiscoveryNode node) {
        return Version.CURRENT.isCompatible(node.getVersion())
            && (node.isMasterEligibleNode() == false || node.isDataNode());
    }

    @Override
    public <Req extends TransportRequest, Resp extends TransportResponse> CompletableFuture<Resp> execute(ActionType<Resp> action, Req request) {
        DiscoveryNode targetNode = request instanceof RemoteClusterAwareRequest remoteClusterAware
            ? remoteClusterAware.getPreferredTargetNode()
            : null;
        return ensureConnected(targetNode).thenCompose(conn -> {
            FutureActionListener<Resp> future = new FutureActionListener<>();
            var connection = targetNode == null ? conn : new ProxyConnection(conn, targetNode);
            transportService.sendRequest(
                connection,
                action.name(),
                request,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(future, action.getResponseReader())
            );
            return future;
        });
    }

    private static DiscoveryNode resolveSeedNode(String clusterAlias, String address) {
        TransportAddress transportAddress = new TransportAddress(RemoteConnectionParser.parseConfiguredAddress(address));
        return new DiscoveryNode(
            "sniff_to=" + clusterAlias + "#" + transportAddress.toString(),
            transportAddress,
            Version.CURRENT.minimumCompatibilityVersion()
        );
    }
}
