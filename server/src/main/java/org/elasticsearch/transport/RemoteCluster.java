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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportService.HandshakeResponse;

import io.crate.action.FutureActionListener;
import io.crate.common.collections.Lists2;
import io.crate.exceptions.Exceptions;
import io.crate.netty.EventLoopGroups;
import io.crate.protocols.postgres.PgClient;
import io.crate.protocols.postgres.PgClientFactory;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.types.DataTypes;

public final class RemoteCluster implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(RemoteCluster.class.getName());

    public enum ConnectionStrategy {
        SNIFF,
        PG_TUNNEL;
    }

    public static final Setting<ConnectionStrategy> REMOTE_CONNECTION_MODE = new Setting<>(
        "mode",
        ConnectionStrategy.SNIFF.name(),
        value -> ConnectionStrategy.valueOf(value.toUpperCase(Locale.ROOT)),
        DataTypes.STRING,
        Setting.Property.Dynamic
    );

    /**
     * A list of initial seed nodes to discover eligible nodes from the remote cluster
     */
    public static final Setting<List<String>> REMOTE_CLUSTER_SEEDS = Setting.listSetting(
        "seeds",
        null,
        s -> {
            // validate seed address
            RemoteConnectionParser.parsePort(s);
            return s;
        },
        s -> List.of(),
        value -> {},
        DataTypes.STRING_ARRAY,
        Setting.Property.Dynamic
    );

    public static Set<String> SETTING_NAMES = Set.of(
        REMOTE_CONNECTION_MODE.getKey(),
        REMOTE_CLUSTER_SEEDS.getKey()
    );

    private final ConcurrentMap<DiscoveryNode, CompletableFuture<Transport.Connection>> connections = new ConcurrentHashMap<>();
    private final String clusterName;
    private final ConnectionStrategy connectionStrategy;
    private final List<DiscoveryNode> seedNodes;
    private final TransportService transportService;
    private final ConnectionProfile connectionProfile;
    private final ConnectionInfo connectionInfo;
    private final PgClientFactory pgClientFactory;

    private final SetOnce<ClusterName> remoteClusterName = new SetOnce<>();


    public RemoteCluster(String clusterName,
                         ConnectionInfo connectionInfo,
                         PgClientFactory pgClientFactory,
                         TransportService transportService) {
        this.pgClientFactory = pgClientFactory;
        Settings settings = connectionInfo.settings();
        this.clusterName = clusterName;
        this.connectionProfile = ConnectionProfile.buildDefaultConnectionProfile(settings);
        this.connectionInfo = connectionInfo;
        this.transportService = transportService;
        this.connectionStrategy = REMOTE_CONNECTION_MODE.get(settings);
        this.seedNodes = Lists2.map(REMOTE_CLUSTER_SEEDS.get(settings), this::resolveSeedNode);
    }

    public CompletableFuture<Connection> getConnection(@Nullable DiscoveryNode preferredTargetNode) {
        // TODO: avoid initiating duplicate connections
        // Need to approach concurrency differently
        if (preferredTargetNode != null) {
            return connections.computeIfAbsent(preferredTargetNode, this::connect);
        }
        return connections.values().stream().findFirst()
            .orElseGet(() -> connect(null));
    }

    private CompletableFuture<Connection> connect(@Nullable DiscoveryNode preferredNode) {
        return switch (connectionStrategy) {
            case SNIFF -> connectSniff(preferredNode);
            case PG_TUNNEL -> connectPgTunnel(preferredNode);
        };
    }

    @Override
    public void close() throws IOException {
        // TODO: close connections
    }

    private CompletableFuture<Connection> connectPgTunnel(@Nullable DiscoveryNode preferredNode) {
        List<String> hosts = connectionInfo.hosts();
        if (hosts.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("No hosts configured for remote cluster [" + clusterName + "]"));
        }
        PgClient client = pgClientFactory.createClient(hosts.get(0));
        return client.connect();
    }

    private CompletableFuture<Connection> connectSniff(@Nullable DiscoveryNode preferredNode) {
        if (preferredNode == null) {
            Iterator<DiscoveryNode> iterator = seedNodes.iterator();
            if (iterator.hasNext()) {
                preferredNode = iterator.next();
            } else {
                return CompletableFuture.failedFuture(new IllegalStateException("no seed nodes configured"));
            }
        }
        FutureActionListener<Connection, Connection> openConnFuture = FutureActionListener.newInstance();
        transportService.transport.openConnection(preferredNode, connectionProfile, openConnFuture);
        return openConnFuture
            .thenCompose(this::handshake)
            .thenCompose(this::finalizeConnection);
    }

    private CompletableFuture<HandshakeAndConnection> handshake(Connection conn) {
        FutureActionListener<HandshakeResponse, HandshakeResponse> handshakeFuture = FutureActionListener.newInstance();
        transportService.handshake(
            conn,
            connectionProfile.getHandshakeTimeout().millis(),
            getRemoteClusterNamePredicate(),
            handshakeFuture
        );
        return handshakeFuture.handle((response, err) -> {
            if (err != null) {
                conn.close();
                LOGGER.warn("Error during handshake with node [{}]", response, err);
                throw Exceptions.toRuntimeException(err);
            }
            remoteClusterName.trySet(response.getClusterName());
            return new HandshakeAndConnection(response, conn);
        });
    }

    private CompletableFuture<Connection> finalizeConnection(HandshakeAndConnection handshakeAndConnection) {
        FutureActionListener<Void, Connection> future = new FutureActionListener<>(ignored -> handshakeAndConnection.connection);
        DiscoveryNode discoveryNode = handshakeAndConnection.handshakeResponse.getDiscoveryNode();
        transportService.connectToNode(discoveryNode, future);
        return future.handle((res, err) -> {
            if (err != null) {
                handshakeAndConnection.connection.close();
                LOGGER.warn("Error during connection to node [{}]", discoveryNode, err);
                throw Exceptions.toRuntimeException(err);
            }
            return res;
        });
    }

    private DiscoveryNode resolveSeedNode(String seedNode) {
        TransportAddress transportAddress = new TransportAddress(RemoteConnectionParser.parseConfiguredAddress(seedNode));
        return new DiscoveryNode(
            clusterName + "#" + transportAddress.toString(),
            transportAddress,
            Version.CURRENT.minimumCompatibilityVersion()
        );
    }

    private Predicate<ClusterName> getRemoteClusterNamePredicate() {
        return new Predicate<ClusterName>() {
            @Override
            public boolean test(ClusterName c) {
                return remoteClusterName.get() == null || c.equals(remoteClusterName.get());
            }

            @Override
            public String toString() {
                return remoteClusterName.get() == null ? "any cluster name"
                    : "expected remote cluster name [" + remoteClusterName.get().value() + "]";
            }
        };
    }


    record HandshakeAndConnection(HandshakeResponse handshakeResponse, Connection connection) {
    }
}

