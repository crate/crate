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
package org.elasticsearch.cluster.health;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;

public class ClusterHealthAllocationTests extends ESAllocationTestCase {

    public void testClusterHealth() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        if (randomBoolean()) {
            clusterState = addNode(clusterState, "node_m", true);
        }
        assertThat(getClusterHealthStatus(clusterState)).isEqualTo(ClusterHealthStatus.GREEN);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT).put(AutoExpandReplicas.SETTING.getKey(), false))
                .numberOfShards(2)
                .numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(routingTable).build();
        MockAllocationService allocation = createAllocationService();
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertThat(clusterState.nodes().getDataNodes().size()).isEqualTo(0);
        assertThat(getClusterHealthStatus(clusterState)).isEqualTo(ClusterHealthStatus.RED);

        clusterState = addNode(clusterState, "node_d1", false);
        assertThat(clusterState.nodes().getDataNodes().size()).isEqualTo(1);
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertThat(getClusterHealthStatus(clusterState)).isEqualTo(ClusterHealthStatus.YELLOW);

        clusterState = addNode(clusterState, "node_d2", false);
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertThat(getClusterHealthStatus(clusterState)).isEqualTo(ClusterHealthStatus.GREEN);

        clusterState = removeNode(clusterState, "node_d1", allocation);
        assertThat(getClusterHealthStatus(clusterState)).isEqualTo(ClusterHealthStatus.YELLOW);

        clusterState = removeNode(clusterState, "node_d2", allocation);
        assertThat(getClusterHealthStatus(clusterState)).isEqualTo(ClusterHealthStatus.RED);

        routingTable = RoutingTable.builder(routingTable).remove("test").build();
        metadata = Metadata.builder(clusterState.metadata()).remove("test").build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadata).build();
        assertThat(clusterState.nodes().getDataNodes().size()).isEqualTo(0);
        assertThat(getClusterHealthStatus(clusterState)).isEqualTo(ClusterHealthStatus.GREEN);
    }

    private ClusterState addNode(ClusterState clusterState, String nodeName, boolean isMaster) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        nodeBuilder.add(newNode(nodeName, Collections.singleton(isMaster ? DiscoveryNodeRole.MASTER_ROLE : DiscoveryNodeRole.DATA_ROLE)));
        return ClusterState.builder(clusterState).nodes(nodeBuilder).build();
    }

    private ClusterState removeNode(ClusterState clusterState, String nodeName, AllocationService allocationService) {
        return allocationService.disassociateDeadNodes(ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(nodeName)).build(), true, "reroute");
    }

    private ClusterHealthStatus getClusterHealthStatus(ClusterState clusterState) {
        return new ClusterStateHealth(clusterState).getStatus();
    }

}
