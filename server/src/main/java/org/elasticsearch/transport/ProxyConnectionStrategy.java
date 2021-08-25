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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.intSetting;

public class ProxyConnectionStrategy extends RemoteConnectionStrategy {

    /**
     * The remote address for the proxy. The connections will be opened to the configured address.
     */
    public static final Setting<String> PROXY_ADDRESS = Setting.simpleString(
        "proxy_address",
        new StrategyValidator<>("proxy_address", ConnectionStrategy.PROXY, s -> {
            if (Strings.hasLength(s)) {
                parsePort(s);
            }
        }), Setting.Property.Dynamic);

    /**
     * The maximum number of socket connections that will be established to a remote cluster. The default is 18.
     */
    public static final Setting<Integer> REMOTE_SOCKET_CONNECTIONS = intSetting(
        "proxy_socket_connections",
        18,
        1,
        new StrategyValidator<>("proxy_socket_connections", ConnectionStrategy.PROXY),
        Setting.Property.Dynamic);

    static final int CHANNELS_PER_CONNECTION = 1;

    private static final int MAX_CONNECT_ATTEMPTS_PER_RUN = 3;

    private final int maxNumConnections;
    private final String configuredAddress;
    private final Supplier<TransportAddress> address;
    private final AtomicReference<ClusterName> remoteClusterName = new AtomicReference<>();
    private final ConnectionManager.ConnectionValidator clusterNameValidator;

    ProxyConnectionStrategy(String clusterAlias,
                            TransportService transportService,
                            RemoteConnectionManager connectionManager,
                            Settings connectionSettings) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            REMOTE_SOCKET_CONNECTIONS.get(connectionSettings),
            PROXY_ADDRESS.get(connectionSettings)
        );
    }

    ProxyConnectionStrategy(String clusterAlias,
                            TransportService transportService,
                            RemoteConnectionManager connectionManager,
                            int maxNumConnections,
                            String configuredAddress) {
        this(clusterAlias, transportService, connectionManager, maxNumConnections, configuredAddress,
             () -> resolveAddress(configuredAddress));
    }

    ProxyConnectionStrategy(String clusterAlias,
                            TransportService transportService,
                            RemoteConnectionManager connectionManager,
                            int maxNumConnections,
                            String configuredAddress,
                            Supplier<TransportAddress> address) {
        super(clusterAlias, transportService, connectionManager);
        this.maxNumConnections = maxNumConnections;
        this.configuredAddress = configuredAddress;
        assert Strings.isEmpty(configuredAddress) == false : "Cannot use proxy connection strategy with no configured addresses";
        this.address = address;
        this.clusterNameValidator = (newConnection, actualProfile, listener) ->
            transportService.handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true,
                                       ActionListener.map(listener, resp -> {
                                           ClusterName remote = resp.getClusterName();
                                           if (remoteClusterName.compareAndSet(null, remote)) {
                                               return null;
                                           } else {
                                               if (remoteClusterName.get().equals(remote) == false) {
                                                   DiscoveryNode node = newConnection.getNode();
                                                   throw new ConnectTransportException(node, "handshake failed. unexpected remote cluster name " + remote);
                                               }
                                               return null;
                                           }
                                       }));
    }

    static Stream<Setting<?>> enablementSettings() {
        return Stream.of(ProxyConnectionStrategy.PROXY_ADDRESS);
    }

    @Override
    protected boolean shouldOpenMoreConnections() {
        return connectionManager.size() < maxNumConnections;
    }

    @Override
    protected boolean strategyMustBeRebuilt(Settings newSettings) {
        String address = PROXY_ADDRESS.get(newSettings);
        int numOfSockets = REMOTE_SOCKET_CONNECTIONS.get(newSettings);
        return numOfSockets != maxNumConnections || configuredAddress.equals(address) == false;
    }

    @Override
    protected ConnectionStrategy strategyType() {
        return ConnectionStrategy.PROXY;
    }

    @Override
    protected void connectImpl(ActionListener<Void> listener) {
        performProxyConnectionProcess(listener);
    }

    private void performProxyConnectionProcess(ActionListener<Void> listener) {
        openConnections(listener, 1);
    }

    private void openConnections(ActionListener<Void> finished, int attemptNumber) {
        if (attemptNumber <= MAX_CONNECT_ATTEMPTS_PER_RUN) {
            TransportAddress resolved = address.get();

            int remaining = maxNumConnections - connectionManager.size();
            ActionListener<Void> compositeListener = new ActionListener<Void>() {

                private final AtomicInteger successfulConnections = new AtomicInteger(0);
                private final CountDown countDown = new CountDown(remaining);

                @Override
                public void onResponse(Void v) {
                    successfulConnections.incrementAndGet();
                    if (countDown.countDown()) {
                        if (shouldOpenMoreConnections()) {
                            openConnections(finished, attemptNumber + 1);
                        } else {
                            finished.onResponse(v);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (countDown.countDown()) {
                        openConnections(finished, attemptNumber + 1);
                    }
                }
            };


            for (int i = 0; i < remaining; ++i) {
                String id = clusterAlias + "#" + resolved;
                Map<String, String> attributes = Collections.emptyMap();
                DiscoveryNode node = new DiscoveryNode(id, resolved, attributes, DiscoveryNodeRole.BUILT_IN_ROLES,
                                                       Version.CURRENT.minimumCompatibilityVersion());

                connectionManager.connectToNode(node, null, clusterNameValidator, new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void v) {
                        compositeListener.onResponse(v);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(new ParameterizedMessage("failed to open remote connection [remote cluster: {}, address: {}]",
                                                              clusterAlias, resolved), e);
                        compositeListener.onFailure(e);
                    }
                });
            }
        } else {
            int openConnections = connectionManager.size();
            if (openConnections == 0) {
                finished.onFailure(new IllegalStateException("Unable to open any proxy connections to remote cluster [" + clusterAlias
                                                             + "]"));
            } else {
                logger.debug("unable to open maximum number of connections [remote cluster: {}, opened: {}, maximum: {}]", clusterAlias,
                             openConnections, maxNumConnections);
                finished.onResponse(null);
            }
        }
    }

    private static TransportAddress resolveAddress(String address) {
        return new TransportAddress(parseConfiguredAddress(address));
    }
}
