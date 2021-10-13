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

import io.crate.common.io.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

/**
 * Represents a connection to a single remote cluster. In contrast to a local cluster a remote cluster is not joined such that the
 * current node is part of the cluster and it won't receive cluster state updates from the remote cluster. Remote clusters are also not
 * fully connected with the current node. From a connection perspective a local cluster forms a bi-directional star network while in the
 * remote case we only connect to a subset of the nodes in the cluster in an uni-directional fashion.
 * <p>
 * This class also handles the discovery of nodes from the remote cluster. The initial list of seed nodes is only used to discover all nodes
 * in the remote cluster and connects to all eligible nodes.
 * <p>
 * In the case of a disconnection, this class will issue a re-connect task to establish at most
 * {@link SniffConnectionStrategy#REMOTE_NODE_CONNECTIONS} until either all eligible nodes are exhausted or the maximum number of
 * connections per cluster has been reached.
 */
public final class RemoteClusterConnection implements Closeable {


    private final TransportService transportService;
    private final RemoteConnectionManager remoteConnectionManager;
    private final RemoteConnectionStrategy connectionStrategy;

    /**
     * Creates a new {@link RemoteClusterConnection}
     *
     * @param nodeSettings     the nodes settings object
     * @param clusterAlias     the configured alias of the cluster to connect to
     * @param transportService the local nodes transport service
     */
    RemoteClusterConnection(Settings nodeSettings,
                            Settings connectionSettings,
                            String clusterAlias,
                            TransportService transportService) {
        this.transportService = transportService;
        ConnectionProfile profile = RemoteConnectionStrategy.buildConnectionProfile(nodeSettings, connectionSettings);
        this.remoteConnectionManager = new RemoteConnectionManager(clusterAlias,
                                                                   createConnectionManager(profile, transportService));
        this.connectionStrategy = RemoteConnectionStrategy.buildStrategy(
            clusterAlias,
            transportService,
            remoteConnectionManager,
            nodeSettings,
            connectionSettings
        );
        // we register the transport service here as a listener to make sure we notify handlers on disconnect etc.
        this.remoteConnectionManager.addListener(transportService);
    }

    /**
     * Ensures that this cluster is connected. If the cluster is connected this operation
     * will invoke the listener immediately.
     */
    void ensureConnected(ActionListener<Void> listener) {
        if (remoteConnectionManager.size() == 0) {
            connectionStrategy.connect(listener);
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Collects all nodes on the connected cluster and returns / passes a nodeID to {@link DiscoveryNode} lookup function
     * that returns <code>null</code> if the node ID is not found.
     * <p>
     * The requests to get cluster state on the connected cluster are made in the system context because logically
     * they are equivalent to checking a single detail in the local cluster state and should not require that the
     * user who made the request that is using this method in its implementation is authorized to view the entire
     * cluster state.
     */
    void collectNodes(ActionListener<Function<String, DiscoveryNode>> listener) {
        Runnable runnable = () -> {
            final ClusterStateRequest request = new ClusterStateRequest();
            request.clear();
            request.nodes(true);
            request.local(true); // run this on the node that gets the request it's as good as any other
            Transport.Connection connection = remoteConnectionManager.getAnyRemoteConnection();
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
                        listener.onResponse(nodes::get);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                }
            );
        };
        try {
            // just in case if we are not connected for some reason we try to connect and if we fail we have to notify the listener
            // this will cause some back pressure on the search end and eventually will cause rejections but that's fine
            // we can't proceed with a search on a cluster level.
            // in the future we might want to just skip the remote nodes in such a case but that can already be implemented on the
            // caller end since they provide the listener.
            ensureConnected(ActionListener.wrap((x) -> runnable.run(), listener::onFailure));
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    /**
     * Returns a connection to the remote cluster, preferably a direct connection to the provided {@link DiscoveryNode}.
     * If such node is not connected, the returned connection will be a proxy connection that redirects to it.
     */
    Transport.Connection getConnection(DiscoveryNode remoteClusterNode) {
        return remoteConnectionManager.getConnection(remoteClusterNode);
    }

    Transport.Connection getConnection() {
        return remoteConnectionManager.getAnyRemoteConnection();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(connectionStrategy, remoteConnectionManager);
    }

    public boolean isClosed() {
        return connectionStrategy.isClosed();
    }

    // for testing only
    boolean assertNoRunningConnections() {
        return connectionStrategy.assertNoRunningConnections();
    }

    boolean isNodeConnected(final DiscoveryNode node) {
        return remoteConnectionManager.nodeConnected(node);
    }

    int getNumNodesConnected() {
        return remoteConnectionManager.size();
    }

    private static ConnectionManager createConnectionManager(ConnectionProfile connectionProfile,
                                                             TransportService transportService) {
        return new ClusterConnectionManager(connectionProfile, transportService.transport);
    }

    ConnectionManager getConnectionManager() {
        return remoteConnectionManager;
    }

    boolean shouldRebuildConnection(Settings newSettings) {
        return connectionStrategy.shouldRebuildConnection(newSettings);
    }
}
