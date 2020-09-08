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

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.RunOnce;

import io.crate.common.io.IOUtils;

/**
 * This class manages node connections. The connection is opened by the underlying transport. Once the
 * connection is opened, this class manages the connection. This includes keep-alive pings and closing
 * the connection when the connection manager is closed.
 */
public class ConnectionManager implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(ConnectionManager.class);

    private final ConcurrentMap<DiscoveryNode, Transport.Connection> connectedNodes = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<DiscoveryNode, ListenableFuture<Void>> pendingConnections = ConcurrentCollections.newConcurrentMap();
    private final AbstractRefCounted connectingRefCounter = new AbstractRefCounted("connection manager") {
        @Override
        protected void closeInternal() {
            closeLatch.countDown();
        }
    };
    private final Transport transport;
    private final ConnectionProfile defaultProfile;
    private final Lifecycle lifecycle = new Lifecycle();
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private final DelegatingNodeConnectionListener connectionListener = new DelegatingNodeConnectionListener();

    public ConnectionManager(Settings settings, Transport transport) {
        this(ConnectionProfile.buildDefaultConnectionProfile(settings), transport);
    }

    public ConnectionManager(ConnectionProfile connectionProfile, Transport transport) {
        this.transport = transport;
        this.defaultProfile = connectionProfile;
        this.lifecycle.moveToStarted();
    }

    public void addListener(TransportConnectionListener listener) {
        this.connectionListener.listeners.addIfAbsent(listener);
    }

    public void removeListener(TransportConnectionListener listener) {
        this.connectionListener.listeners.remove(listener);
    }

    public void openConnection(DiscoveryNode node,
                               ConnectionProfile connectionProfile,
                               ActionListener<Transport.Connection> listener) {
        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        internalOpenConnection(node, resolvedProfile, listener);
    }

    @FunctionalInterface
    public interface ConnectionValidator {
        void validate(Transport.Connection connection, ConnectionProfile profile, ActionListener<Void> listener);
    }

    /**
     * Connects to a node with the given connection profile. If the node is already connected this method has no effect.
     * Once a successful is established, it can be validated before being exposed.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     */
    public void connectToNode(DiscoveryNode node,
                              ConnectionProfile connectionProfile,
                              ConnectionValidator connectionValidator,
                              ActionListener<Void> listener) throws ConnectTransportException {
        if (node == null) {
            listener.onFailure(new ConnectTransportException(null, "can't connect to a null node"));
            return;
        }

        if (connectingRefCounter.tryIncRef() == false) {
            listener.onFailure(new IllegalStateException("connection manager is closed"));
            return;
        }

        if (connectedNodes.containsKey(node)) {
            connectingRefCounter.decRef();
            listener.onResponse(null);
            return;
        }
        final ListenableFuture<Void> currentListener = new ListenableFuture<>();
        final ListenableFuture<Void> existingListener = pendingConnections.putIfAbsent(node, currentListener);
        if (existingListener != null) {
            try {
                // wait on previous entry to complete connection attempt
                existingListener.addListener(listener, EsExecutors.directExecutor());
            } finally {
                connectingRefCounter.decRef();
            }
            return;
        }
        currentListener.addListener(listener, EsExecutors.directExecutor());
        final RunOnce releaseOnce = new RunOnce(connectingRefCounter::decRef);
        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        internalOpenConnection(
            node,
            resolvedProfile,
            ActionListener.wrap(
                conn -> {
                    connectionValidator.validate(
                        conn,
                        resolvedProfile,
                        ActionListener.wrap(
                            ignored -> {
                                assert Transports.assertNotTransportThread("connection validator success");
                                try {
                                    if (connectedNodes.putIfAbsent(node, conn) != null) {
                                        LOGGER.debug("existing connection to node [{}], closing new redundant connection", node);
                                        IOUtils.closeWhileHandlingException(conn);
                                    } else {
                                        LOGGER.debug("connected to node [{}]", node);
                                        try {
                                            connectionListener.onNodeConnected(node);
                                        } finally {
                                            final Transport.Connection finalConnection = conn;
                                            conn.addCloseListener(ActionListener.wrap(() -> {
                                                LOGGER.trace("unregistering {} after connection close and marking as disconnected", node);
                                                connectedNodes.remove(node, finalConnection);
                                                connectionListener.onNodeDisconnected(node);
                                            }));
                                        }
                                    }
                                } finally {
                                    ListenableFuture<Void> future = pendingConnections.remove(node);
                                    assert future == currentListener : "Listener in pending map is different than the expected listener";
                                    releaseOnce.run();
                                    future.onResponse(null);
                                }
                            },
                            e -> {
                                assert Transports.assertNotTransportThread("connection validator failure");
                                IOUtils.closeWhileHandlingException(conn);
                                failConnectionListeners(node, releaseOnce, e, currentListener);
                            }
                        )
                    );
                },
                e -> {
                    assert Transports.assertNotTransportThread("internalOpenConnection failure");
                    failConnectionListeners(node, releaseOnce, e, currentListener);
                }
            )
        );
    }

    /**
     * Returns a connection for the given node if the node is connected.
     * Connections returned from this method must not be closed. The lifecycle of this connection is
     * maintained by this connection manager
     *
     * @throws NodeNotConnectedException if the node is not connected
     * @see #connectToNode(DiscoveryNode, ConnectionProfile, ConnectionValidator, ActionListener)
     */
    public Transport.Connection getConnection(DiscoveryNode node) {
        Transport.Connection connection = connectedNodes.get(node);
        if (connection == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return connection;
    }

    /**
     * Returns {@code true} if the node is connected.
     */
    public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    /**
     * Disconnected from the given node, if not connected, will do nothing.
     */
    public void disconnectFromNode(DiscoveryNode node) {
        Transport.Connection nodeChannels = connectedNodes.remove(node);
        if (nodeChannels != null) {
            // if we found it and removed it we close
            nodeChannels.close();
        }
    }

    /**
     * Returns the number of nodes this manager is connected to.
     */
    public int size() {
        return connectedNodes.size();
    }

    @Override
    public void close() {
        assert Transports.assertNotTransportThread("Closing ConnectionManager");
        if (closing.compareAndSet(false, true)) {
            connectingRefCounter.decRef();
            try {
                closeLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            Iterator<Map.Entry<DiscoveryNode, Transport.Connection>> iterator = connectedNodes.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<DiscoveryNode, Transport.Connection> next = iterator.next();
                try {
                    IOUtils.closeWhileHandlingException(next.getValue());
                } finally {
                    iterator.remove();
                }
            }
        }
    }

    private void internalOpenConnection(DiscoveryNode node,
                                        ConnectionProfile connectionProfile,
                                        ActionListener<Transport.Connection> listener) {
        transport.openConnection(node, connectionProfile, ActionListener.map(listener, connection -> {
            assert Transports.assertNotTransportThread("internalOpenConnection success");
            try {
                connectionListener.onConnectionOpened(connection);
            } finally {
                connection.addCloseListener(ActionListener.wrap(() -> connectionListener.onConnectionClosed(connection)));
            }
            if (connection.isClosed()) {
                throw new ConnectTransportException(node, "a channel closed while connecting");
            }
            return connection;
        }));
    }

    private static final class DelegatingNodeConnectionListener implements TransportConnectionListener {

        private final CopyOnWriteArrayList<TransportConnectionListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onNodeDisconnected(DiscoveryNode key) {
            for (TransportConnectionListener listener : listeners) {
                listener.onNodeDisconnected(key);
            }
        }

        @Override
        public void onNodeConnected(DiscoveryNode node) {
            for (TransportConnectionListener listener : listeners) {
                listener.onNodeConnected(node);
            }
        }

        @Override
        public void onConnectionOpened(Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onConnectionOpened(connection);
            }
        }

        @Override
        public void onConnectionClosed(Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onConnectionClosed(connection);
            }
        }
    }

    private void failConnectionListeners(DiscoveryNode node,
                                         RunOnce releaseOnce,
                                         Exception e,
                                         ListenableFuture<Void> expectedListener) {
        ListenableFuture<Void> future = pendingConnections.remove(node);
        releaseOnce.run();
        if (future != null) {
            assert future == expectedListener : "Listener in pending map is different than the expected listener";
            future.onFailure(e);
        }
    }

    ConnectionProfile getConnectionProfile() {
        return defaultProfile;
    }
}
