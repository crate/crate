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

import io.crate.common.Booleans;
import io.crate.common.io.IOUtils;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.intSetting;

public class SniffConnectionStrategy extends RemoteConnectionStrategy {

    /**
     * A list of initial seed nodes to discover eligible nodes from the remote cluster
     */
    public static final Setting<List<String>> REMOTE_CLUSTER_SEEDS = Setting.listSetting(
        "seeds",
        null,
        s -> {
            // validate seed address
            parsePort(s);
            return s;
        },
        s -> List.of(),
        new StrategyValidator<>("seeds", ConnectionStrategy.SNIFF),
        DataTypes.STRING_ARRAY,
        Setting.Property.Dynamic);

    /**
     * A proxy address for the remote cluster. By default this is not set, meaning that Elasticsearch will connect directly to the nodes in
     * the remote cluster using their publish addresses. If this setting is set to an IP address or hostname then Elasticsearch will connect
     * to the nodes in the remote cluster using this address instead. Use of this setting is not recommended and it is deliberately
     * undocumented as it does not work well with all proxies.
     */
    public static final Setting<String> REMOTE_CLUSTERS_PROXY = Setting.simpleString(
        "proxy",
        new StrategyValidator<>("proxy", ConnectionStrategy.SNIFF, s -> {
            if (Strings.hasLength(s)) {
                parsePort(s);
            }
        }),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope);

    /**
     * The maximum number of node connections that will be established to a remote cluster. For instance if there is only a single
     * seed node, other nodes will be discovered up to the given number of nodes in this setting. The default is 3.
     */
    public static final Setting<Integer> REMOTE_NODE_CONNECTIONS = intSetting(
        "node_connections",
        3,
        1,
        new StrategyValidator<>("node_connections", ConnectionStrategy.SNIFF),
        Setting.Property.Dynamic);


    static final int CHANNELS_PER_CONNECTION = 6;

    private static final Predicate<DiscoveryNode> DEFAULT_NODE_PREDICATE = (node) ->
        Version.CURRENT.isCompatible(node.getVersion())
        && (node.isMasterNode() == false || node.isDataNode());


    private final List<String> configuredSeedNodes;
    private final List<Supplier<DiscoveryNode>> seedNodes;
    private final int maxNumRemoteConnections;
    private final Predicate<DiscoveryNode> nodePredicate;
    private final SetOnce<ClusterName> remoteClusterName = new SetOnce<>();
    private final String proxyAddress;

    SniffConnectionStrategy(String clusterAlias,
                            TransportService transportService,
                            RemoteConnectionManager connectionManager,
                            Settings nodeSettings,
                            Settings connectionSettings) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            REMOTE_CLUSTERS_PROXY.get(connectionSettings),
            REMOTE_NODE_CONNECTIONS.get(connectionSettings),
            getNodePredicate(nodeSettings),
            REMOTE_CLUSTER_SEEDS.get(connectionSettings)
        );
    }

    SniffConnectionStrategy(String clusterAlias,
                            TransportService transportService,
                            RemoteConnectionManager connectionManager,
                            String proxyAddress,
                            int maxNumRemoteConnections,
                            Predicate<DiscoveryNode> nodePredicate,
                            List<String> configuredSeedNodes) {
        this(clusterAlias,
             transportService,
             connectionManager,
             proxyAddress,
             maxNumRemoteConnections,
             nodePredicate,
             configuredSeedNodes,
             configuredSeedNodes.stream().map(
                 seedAddress ->
                     (Supplier<DiscoveryNode>) () -> resolveSeedNode(clusterAlias, seedAddress, proxyAddress)).collect(
                 Collectors.toList()));
    }

    SniffConnectionStrategy(String clusterAlias,
                            TransportService transportService,
                            RemoteConnectionManager connectionManager,
                            String proxyAddress,
                            int maxNumRemoteConnections,
                            Predicate<DiscoveryNode> nodePredicate,
                            List<String> configuredSeedNodes,
                            List<Supplier<DiscoveryNode>> seedNodes) {
        super(clusterAlias, transportService, connectionManager);
        this.proxyAddress = proxyAddress;
        this.maxNumRemoteConnections = maxNumRemoteConnections;
        this.nodePredicate = nodePredicate;
        this.configuredSeedNodes = configuredSeedNodes;
        this.seedNodes = seedNodes;
    }

    static Stream<Setting<?>> enablementSettings() {
        return Stream.of(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS);
    }

    @Override
    protected boolean shouldOpenMoreConnections() {
        return connectionManager.size() < maxNumRemoteConnections;
    }

    @Override
    protected boolean strategyMustBeRebuilt(Settings newSettings) {
        String proxy = REMOTE_CLUSTERS_PROXY.get(newSettings);
        List<String> addresses = REMOTE_CLUSTER_SEEDS.get(newSettings);
        int nodeConnections = REMOTE_NODE_CONNECTIONS.get(newSettings);
        return nodeConnections != maxNumRemoteConnections || seedsChanged(configuredSeedNodes, addresses)
               || proxyChanged(proxyAddress, proxy);
    }

    @Override
    protected ConnectionStrategy strategyType() {
        return ConnectionStrategy.SNIFF;
    }

    @Override
    protected void connectImpl(ActionListener<Void> listener) {
        collectRemoteNodes(seedNodes.iterator(), listener);
    }

    private void collectRemoteNodes(Iterator<Supplier<DiscoveryNode>> seedNodes, ActionListener<Void> listener) {
        if (Thread.currentThread().isInterrupted()) {
            listener.onFailure(new InterruptedException("remote connect thread got interrupted"));
            return;
        }

        if (seedNodes.hasNext()) {
            final Consumer<Exception> onFailure = e -> {
                if (e instanceof ConnectTransportException ||
                    e instanceof IOException ||
                    e instanceof IllegalStateException) {
                    // ISE if we fail the handshake with an version incompatible node
                    if (seedNodes.hasNext()) {
                        logger.debug(() -> new ParameterizedMessage(
                                         "fetching nodes from external cluster [{}] failed moving to next seed node", clusterAlias),
                                     e);
                        collectRemoteNodes(seedNodes, listener);
                        return;
                    }
                }
                logger.warn(new ParameterizedMessage("fetching nodes from external cluster [{}] failed", clusterAlias),
                            e);
                listener.onFailure(e);
            };

            final DiscoveryNode seedNode = seedNodes.next().get();
            logger.trace("[{}] opening transient connection to seed node: [{}]", clusterAlias, seedNode);
            final StepListener<Transport.Connection> openConnectionStep = new StepListener<>();
            try {
                connectionManager.openConnection(seedNode, null, openConnectionStep);
            } catch (Exception e) {
                onFailure.accept(e);
            }

            final StepListener<TransportService.HandshakeResponse> handshakeStep = new StepListener<>();
            openConnectionStep.whenComplete(connection -> {
                ConnectionProfile connectionProfile = connectionManager.getConnectionProfile();
                transportService.handshake(connection, connectionProfile.getHandshakeTimeout().millis(),
                                           getRemoteClusterNamePredicate(), handshakeStep);
            }, onFailure);

            final StepListener<Void> fullConnectionStep = new StepListener<>();
            handshakeStep.whenComplete(handshakeResponse -> {
                final DiscoveryNode handshakeNode = handshakeResponse.getDiscoveryNode();

                if (nodePredicate.test(handshakeNode) && shouldOpenMoreConnections()) {
                    logger.trace("[{}] opening managed connection to seed node: [{}] proxy address: [{}]",
                                 clusterAlias,
                                 handshakeNode,
                                 proxyAddress);
                    final DiscoveryNode handshakeNodeWithProxy = maybeAddProxyAddress(proxyAddress, handshakeNode);
                    connectionManager.connectToNode(handshakeNodeWithProxy,
                                                    null,
                                                    transportService.connectionValidator(handshakeNodeWithProxy),
                                                    fullConnectionStep);
                } else {
                    fullConnectionStep.onResponse(null);
                }
            }, e -> {
                final Transport.Connection connection = openConnectionStep.result();
                final DiscoveryNode node = connection.getNode();
                logger.debug(() -> new ParameterizedMessage("[{}] failed to handshake with seed node: [{}]",
                                                            clusterAlias,
                                                            node), e);
                IOUtils.closeWhileHandlingException(connection);
                onFailure.accept(e);
            });

            fullConnectionStep.whenComplete(aVoid -> {
                if (remoteClusterName.get() == null) {
                    TransportService.HandshakeResponse handshakeResponse = handshakeStep.result();
                    assert handshakeResponse.getClusterName().value() != null;
                    remoteClusterName.set(handshakeResponse.getClusterName());
                }
                final Transport.Connection connection = openConnectionStep.result();

                ClusterStateRequest request = new ClusterStateRequest();
                request.clear();
                request.nodes(true);
                // here we pass on the connection since we can only close it once the sendRequest returns otherwise
                // due to the async nature (it will return before it's actually sent) this can cause the request to fail
                // due to an already closed connection.
                TransportService.TimeoutResponseHandler<ClusterStateResponse> responseHandler = new TransportService
                    .TimeoutResponseHandler<>(new SniffClusterStateResponseHandler(connection, listener, seedNodes));
                transportService.sendRequest(connection,
                                             ClusterStateAction.NAME,
                                             request,
                                             TransportRequestOptions.EMPTY,
                                             responseHandler);
            }, e -> {
                final Transport.Connection connection = openConnectionStep.result();
                final DiscoveryNode node = connection.getNode();
                logger.debug(() -> new ParameterizedMessage(
                    "[{}] failed to open managed connection to seed node: [{}]", clusterAlias, node), e);
                IOUtils.closeWhileHandlingException(openConnectionStep.result());
                onFailure.accept(e);
            });
        } else {
            listener.onFailure(new NoSeedNodeLeftException(clusterAlias));
        }
    }

    /* This class handles the _state response from the remote cluster when sniffing nodes to connect to */
    private class SniffClusterStateResponseHandler implements TransportResponseHandler<ClusterStateResponse> {

        private final Transport.Connection connection;
        private final ActionListener<Void> listener;
        private final Iterator<Supplier<DiscoveryNode>> seedNodes;

        SniffClusterStateResponseHandler(Transport.Connection connection, ActionListener<Void> listener,
                                         Iterator<Supplier<DiscoveryNode>> seedNodes) {
            this.connection = connection;
            this.listener = listener;
            this.seedNodes = seedNodes;
        }

        @Override
        public ClusterStateResponse read(StreamInput in) throws IOException {
            return new ClusterStateResponse(in);
        }

        @Override
        public void handleResponse(ClusterStateResponse response) {
            handleNodes(response.getState().nodes().getNodes().valuesIt());
        }

        private void handleNodes(Iterator<DiscoveryNode> nodesIter) {
            while (nodesIter.hasNext()) {
                final DiscoveryNode node = nodesIter.next();
                if (nodePredicate.test(node) && shouldOpenMoreConnections()) {
                    logger.trace("[{}] opening managed connection to node: [{}] proxy address: [{}]",
                                 clusterAlias,
                                 node,
                                 proxyAddress);
                    final DiscoveryNode nodeWithProxy = maybeAddProxyAddress(proxyAddress, node);
                    connectionManager.connectToNode(
                        nodeWithProxy,
                        null,
                        transportService.connectionValidator(node),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(Void aVoid) {
                                handleNodes(nodesIter);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (e instanceof ConnectTransportException ||
                                    e instanceof IllegalStateException) {
                                    // ISE if we fail the handshake with an version incompatible node
                                    // fair enough we can't connect just move on
                                    logger.debug(() -> new ParameterizedMessage(
                                        "[{}] failed to open managed connection to node [{}]",
                                        clusterAlias,
                                        node), e);
                                    handleNodes(nodesIter);
                                } else {
                                    logger.warn(new ParameterizedMessage(
                                        "[{}] failed to open managed connection to node [{}]",
                                        clusterAlias,
                                        node), e);
                                    IOUtils.closeWhileHandlingException(connection);
                                    collectRemoteNodes(seedNodes, listener);
                                }
                            }
                        });
                    return;
                }
            }
            // We have to close this connection before we notify listeners - this is mainly needed for test correctness
            // since if we do it afterwards we might fail assertions that check if all high level connections are closed.
            // from a code correctness perspective we could also close it afterwards.
            IOUtils.closeWhileHandlingException(connection);
            int openConnections = connectionManager.size();
            if (openConnections == 0) {
                listener.onFailure(new IllegalStateException(
                    "Unable to open any connections to remote cluster [" + clusterAlias + "]"));
            } else {
                listener.onResponse(null);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            logger.warn(new ParameterizedMessage("fetching nodes from external cluster {} failed", clusterAlias), exp);
            try {
                IOUtils.closeWhileHandlingException(connection);
            } finally {
                // once the connection is closed lets try the next node
                collectRemoteNodes(seedNodes, listener);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.MANAGEMENT;
        }
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

    private static DiscoveryNode resolveSeedNode(String clusterAlias, String address, String proxyAddress) {
        if (proxyAddress == null || proxyAddress.isEmpty()) {
            TransportAddress transportAddress = new TransportAddress(parseConfiguredAddress(address));
            return new DiscoveryNode(clusterAlias + "#" + transportAddress.toString(), transportAddress,
                                     Version.CURRENT.minimumCompatibilityVersion());
        } else {
            TransportAddress transportAddress = new TransportAddress(parseConfiguredAddress(proxyAddress));
            String hostName = RemoteConnectionStrategy.parseHost(proxyAddress);
            return new DiscoveryNode("",
                                     clusterAlias + "#" + address,
                                     UUIDs.randomBase64UUID(),
                                     hostName,
                                     address,
                                     transportAddress,
                                     Collections.singletonMap("server_name", hostName),
                                     DiscoveryNodeRole.BUILT_IN_ROLES,
                                     Version.CURRENT.minimumCompatibilityVersion());
        }
    }

    // Default visibility for tests
    static Predicate<DiscoveryNode> getNodePredicate(Settings settings) {
        if (RemoteConnectionStrategy.REMOTE_NODE_ATTRIBUTE.exists(settings)) {
            // nodes can be tagged with node.attr.remote_gateway: true to allow a node to be a gateway node for cross cluster search
            String attribute = RemoteConnectionStrategy.REMOTE_NODE_ATTRIBUTE.get(settings);
            return DEFAULT_NODE_PREDICATE.and((node) -> Booleans.parseBoolean(node.getAttributes().getOrDefault(
                attribute,
                "false")));
        }
        return DEFAULT_NODE_PREDICATE;
    }

    private static DiscoveryNode maybeAddProxyAddress(String proxyAddress, DiscoveryNode node) {
        if (proxyAddress == null || proxyAddress.isEmpty()) {
            return node;
        } else {
            // resolve proxy address lazy here
            InetSocketAddress proxyInetAddress = parseConfiguredAddress(proxyAddress);
            return new DiscoveryNode(node.getName(),
                                     node.getId(),
                                     node.getEphemeralId(),
                                     node.getHostName(),
                                     node
                                         .getHostAddress(),
                                     new TransportAddress(proxyInetAddress),
                                     node.getAttributes(),
                                     node.getRoles(),
                                     node.getVersion());
        }
    }

    private boolean seedsChanged(final List<String> oldSeedNodes, final List<String> newSeedNodes) {
        if (oldSeedNodes.size() != newSeedNodes.size()) {
            return true;
        }
        Set<String> oldSeeds = new HashSet<>(oldSeedNodes);
        Set<String> newSeeds = new HashSet<>(newSeedNodes);
        return oldSeeds.equals(newSeeds) == false;
    }

    private boolean proxyChanged(String oldProxy, String newProxy) {
        if (oldProxy == null || oldProxy.isEmpty()) {
            return (newProxy == null || newProxy.isEmpty()) == false;
        }

        return Objects.equals(oldProxy, newProxy) == false;
    }
}
