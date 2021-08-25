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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Base class for all services and components that need up-to-date information about the registered remote clusters
 */
public abstract class RemoteClusterAware {

    public static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';
    public static final String LOCAL_CLUSTER_GROUP_KEY = "";

    protected final Settings settings;

    /**
     * Creates a new {@link RemoteClusterAware} instance
     *
     * @param settings the nodes level settings
     */
    protected RemoteClusterAware(Settings settings) {
        this.settings = settings;
    }

    /**
     * Returns remote clusters that are enabled in these settings
     */
    protected static Set<String> getEnabledRemoteClusters(final Settings settings) {
        return RemoteConnectionStrategy.getRemoteClusters(settings);
    }


    void validateAndUpdateRemoteCluster(String clusterAlias, Settings settings) {
        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
            throw new IllegalArgumentException("remote clusters must not have the empty string as its key");
        }
        updateRemoteCluster(clusterAlias, settings);
    }

    /**
     * Subclasses must implement this to receive information about updated cluster aliases.
     */
    protected abstract void updateRemoteCluster(String clusterAlias, Settings settings);

    /**
     * Returns a connection to the given node on the given remote cluster
     *
     * @throws IllegalArgumentException if the remote cluster is unknown
     */
    public abstract Transport.Connection getConnection(DiscoveryNode node, String cluster);

    public abstract Transport.Connection getConnection(String cluster);

    /**
     * Ensures that the given cluster alias is connected. If the cluster is connected this operation
     * will invoke the listener immediately.
     */
    public abstract void ensureConnected(String clusterAlias, ActionListener<Void> listener);

    /**
     * Returns a client to the remote cluster if the given cluster alias exists.
     *
     * @param threadPool   the {@link ThreadPool} for the client
     * @param clusterAlias the cluster alias the remote cluster is registered under
     * @throws IllegalArgumentException if the given clusterAlias doesn't exist
     */
    public abstract Client getRemoteClusterClient(ThreadPool threadPool, String clusterAlias);

    /**
     * Registers this instance to listen to updates on the cluster settings.
     */
    public void listenForUpdates(ClusterSettings clusterSettings) {
        List<Setting<?>> remoteClusterSettings = Arrays.asList(
            RemoteConnectionStrategy.REMOTE_CONNECTION_MODE,
            SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS,
            SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS,
            ProxyConnectionStrategy.PROXY_ADDRESS,
            ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS,
            ProxyConnectionStrategy.SERVER_NAME);
        //clusterSettings.addAffixGroupUpdateConsumer(remoteClusterSettings, this::validateAndUpdateRemoteCluster);
    }

    public static String buildRemoteIndexName(String clusterAlias, String indexName) {
        return clusterAlias == null || LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)
            ? indexName : clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + indexName;
    }
}
