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

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link GatewayMetaState} constructor accepts a lot of arguments.
 * It's not always easy / convenient to construct these dependencies.
 * This class constructor takes far fewer dependencies and constructs usable {@link GatewayMetaState} with 2 restrictions:
 * no metadata upgrade will be performed and no cluster state updaters will be run. This is sufficient for most of the tests.
 * Metadata upgrade is tested in {@link  GatewayMetaStateTests} and different {@link ClusterStateUpdaters} in
 * {@link ClusterStateUpdatersTests}.
 */
public class MockGatewayMetaState extends GatewayMetaState {
    private final DiscoveryNode localNode;

    public MockGatewayMetaState(DiscoveryNode localNode) {
        this.localNode = localNode;
    }

    @Override
    void upgradeMetadata(Settings settings, MetaStateService metaStateService, MetadataIndexUpgradeService metadataIndexUpgradeService,
                         MetadataUpgrader metadataUpgrader) {
        // Metadata upgrade is tested in GatewayMetaStateTests, we override this method to NOP to make mocking easier
    }

    @Override
    ClusterState prepareInitialClusterState(TransportService transportService, ClusterService clusterService, ClusterState clusterState) {
        // Just set localNode here, not to mess with ClusterService and IndicesService mocking
        return ClusterStateUpdaters.setLocalNode(clusterState, localNode);
    }

    public void start(Settings settings, NodeEnvironment nodeEnvironment, NamedXContentRegistry xContentRegistry) {
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(mock(ThreadPool.class));
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings())
            .thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        start(settings, transportService, clusterService, new MetaStateService(nodeEnvironment, xContentRegistry),
            null, null);
    }
}
