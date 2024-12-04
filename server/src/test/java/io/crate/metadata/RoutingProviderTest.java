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

package io.crate.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class RoutingProviderTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_random_master_or_data_node_that_is_not_local_with_negative_seed() {
        RoutingProvider routingProvider = new RoutingProvider(-3, List.of());
        Map<String, String> attr = Map.of();
        var local = new DiscoveryNode("client", buildNewFakeTransportAddress(), attr, Set.of(), Version.CURRENT);
        var node1 = new DiscoveryNode("master", buildNewFakeTransportAddress(), attr, Set.of(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        var node2 = new DiscoveryNode("data", buildNewFakeTransportAddress(), attr, Set.of(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
        DiscoveryNodes nodes = new DiscoveryNodes.Builder()
            .add(node1)
            .add(node2)
            .add(local)
            .localNodeId(local.getId())
            .build();

        Routing forRandomMasterOrDataNode = routingProvider.forRandomMasterOrDataNode(new RelationName("doc", "dummy"), nodes);
        assertThat(forRandomMasterOrDataNode.nodes()).hasSize(1).containsAnyOf(
            node1.getId(),
            node2.getId()
        );
    }
}
