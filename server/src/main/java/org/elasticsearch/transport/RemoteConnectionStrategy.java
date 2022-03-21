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

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

import java.io.Closeable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.unit.TimeValue;

public abstract class RemoteConnectionStrategy implements TransportConnectionListener, Closeable {

    /**
     * The name of a node attribute to select nodes that should be connected to in the remote cluster.
     * For instance a node can be configured with {@code node.attr.gateway: true} in order to be eligible as a gateway node between
     * clusters. In that case {@code search.remote.node.attr: gateway} can be used to filter out other nodes in the remote cluster.
     * The value of the setting is expected to be a boolean, {@code true} for nodes that can become gateways, {@code false} otherwise.
     */
    public static final Setting<String> REMOTE_NODE_ATTRIBUTE =
        Setting.simpleString("cluster.remote.node.attr", Setting.Property.NodeScope);

    public static final Setting<Boolean> REMOTE_CONNECTION_COMPRESS = boolSetting(
        "transport.compress",
        TransportSettings.TRANSPORT_COMPRESS.getDefault(Settings.EMPTY),
        Setting.Property.Dynamic);

    public static final Setting<TimeValue> REMOTE_CONNECTION_PING_SCHEDULE = timeSetting(
        "transport.ping_schedule",
        TransportSettings.PING_SCHEDULE.getDefault(Settings.EMPTY),
        Setting.Property.Dynamic);

    protected final Logger logger = LogManager.getLogger(getClass());

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object mutex = new Object();
    private ActionListener<Void> listener;

    protected final TransportService transportService;
    protected final RemoteConnectionManager connectionManager;
    protected final String clusterAlias;

    RemoteConnectionStrategy(String clusterAlias,
                             TransportService transportService,
                             RemoteConnectionManager connectionManager) {
        this.clusterAlias = clusterAlias;
        this.transportService = transportService;
        this.connectionManager = connectionManager;
        connectionManager.addListener(this);
    }

    static ConnectionProfile buildConnectionProfile(Settings nodeSettings,
                                                    Settings connectionSettings) {
        boolean compress = REMOTE_CONNECTION_COMPRESS.exists(connectionSettings)
            ? REMOTE_CONNECTION_COMPRESS.get(connectionSettings)
            : TransportSettings.TRANSPORT_COMPRESS.get(nodeSettings);
        TimeValue pingInterval = REMOTE_CONNECTION_PING_SCHEDULE.exists(connectionSettings)
            ? REMOTE_CONNECTION_PING_SCHEDULE.get(connectionSettings)
            : TransportSettings.PING_SCHEDULE.get(nodeSettings);
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder()
            .setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(nodeSettings))
            .setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(nodeSettings))
            .setCompressionEnabled(compress)
            .setPingInterval(pingInterval)
            .addConnections(0, TransportRequestOptions.Type.BULK, TransportRequestOptions.Type.STATE,
                            TransportRequestOptions.Type.RECOVERY, TransportRequestOptions.Type.PING)
            .addConnections(SniffConnectionStrategy.CHANNELS_PER_CONNECTION, TransportRequestOptions.Type.REG);
        return builder.build();
    }

    /**
     * Triggers a connect round unless there is one running already. If there is a connect round running, the listener will either
     * be queued or rejected and failed.
     */
    void connect(ActionListener<Void> listener) {
        boolean runConnect;
        boolean closed;
        synchronized (mutex) {
            closed = this.closed.get();
            runConnect = this.listener == null;
            if (closed == false) {
                this.listener = listener;
            }
        }
        if (closed) {
            listener.onFailure(new AlreadyClosedException("connect handler is already closed"));
            return;
        }
        if (runConnect) {
            Executor executor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    var listener = getAndClearListeners();
                    if (listener != null) {
                        listener.onFailure(e);
                    }
                }

                @Override
                protected void doRun() {
                    connectImpl(new ActionListener<>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            var listener = getAndClearListeners();
                            if (listener != null) {
                                listener.onResponse(aVoid);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            var listener = getAndClearListeners();
                            if (listener != null) {
                                listener.onFailure(e);
                            }
                        }
                    });
                }
            });
        }
    }

    @Override
    public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
        if (shouldOpenMoreConnections()) {
            // try to reconnect and fill up the slot of the disconnected node
            connect(ActionListener.wrap(
                ignore -> logger.trace("[{}] successfully connected after disconnect of {}", clusterAlias, node),
                e -> logger.debug(() -> new ParameterizedMessage("[{}] failed to connect after disconnect of {}", clusterAlias, node), e)));
        }
    }

    @Override
    public void close() {
        ActionListener<Void> toNotify = null;
        synchronized (mutex) {
            if (closed.compareAndSet(false, true)) {
                connectionManager.removeListener(this);
                toNotify = listener;
                listener = null;
            }
        }
        if (toNotify != null) {
            try {
                toNotify.onFailure(new AlreadyClosedException("connect handler is already closed"));
            } catch (Exception e) {
                throw new ElasticsearchException(e);
            }
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    // for testing only
    boolean assertNoRunningConnections() {
        synchronized (mutex) {
            assert listener == null : "Expecting connection listener to be NULL";
        }
        return true;
    }

    protected abstract boolean shouldOpenMoreConnections();

    protected abstract void connectImpl(ActionListener<Void> listener);

    @Nullable
    private ActionListener<Void> getAndClearListeners() {
        ActionListener<Void> result = null;
        synchronized (mutex) {
            if (listener != null) {
                result = listener;
                listener = null;
            }
        }
        return result;
    }
}
