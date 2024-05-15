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
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;

public class RoutingTest extends ESTestCase {

    @Test
    public void testStreamingWithLocations() throws Exception {
        Map<String, Map<String, IntIndexedContainer>> locations = new TreeMap<>();
        Map<String, IntIndexedContainer> indexMap = new TreeMap<>();
        indexMap.put("index-0", IntArrayList.from(1, 2));
        locations.put("node-0", indexMap);

        BytesStreamOutput out = new BytesStreamOutput();
        Routing routing1 = new Routing(locations);
        routing1.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        Routing routing2 = new Routing(in);

        assertThat(routing1.locations()).isEqualTo(routing2.locations());
    }

    @Test
    public void testStreamingWithoutLocations() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        Routing routing1 = new Routing(Map.of());
        routing1.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        Routing routing2 = new Routing(in);
        assertThat(routing1.locations()).isEqualTo(routing2.locations());
    }


    @Test
    public void testRoutingForRandomMasterOrDataNode() throws IOException {
        Map<String, String> attr = Map.of();
        Set<DiscoveryNodeRole> master_and_data = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        DiscoveryNode local = new DiscoveryNode("client_node_1", buildNewFakeTransportAddress(), attr, Set.of(), null);
        DiscoveryNodes nodes = new DiscoveryNodes.Builder()
            .add(new DiscoveryNode("data_master_node_1", buildNewFakeTransportAddress(), attr, master_and_data, null))
            .add(new DiscoveryNode("data_master_node_2", buildNewFakeTransportAddress(), attr, master_and_data, null))
            .add(local)
            .add(new DiscoveryNode("client_node_2", buildNewFakeTransportAddress(), attr, Set.of(), null))
            .add(new DiscoveryNode("client_node_3", buildNewFakeTransportAddress(), attr, Set.of(), null))
            .localNodeId(local.getId())
            .build();

        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList());
        Routing routing = routingProvider.forRandomMasterOrDataNode(new RelationName("doc", "table"), nodes);
        assertThat(routing.locations().keySet(), anyOf(contains("data_master_node_1"), contains("data_master_node_2")));

        Routing routing2 = routingProvider.forRandomMasterOrDataNode(new RelationName("doc", "table"), nodes);
        assertThat(routing.locations()).as("routingProvider is seeded and must return deterministic routing").isEqualTo(routing2.locations());
    }

    @Test
    public void testRoutingForRandomMasterOrDataNodePrefersLocal() throws Exception {
        Set<DiscoveryNodeRole> data = Set.of(DiscoveryNodeRole.DATA_ROLE);
        Map<String, String> attr = Map.of();
        DiscoveryNode local = new DiscoveryNode("local_data", buildNewFakeTransportAddress(), attr, data, null);
        DiscoveryNodes nodes = new DiscoveryNodes.Builder()
            .add(local)
            .localNodeId(local.getId())
            .add(new DiscoveryNode("data_1", buildNewFakeTransportAddress(), attr, data, null))
            .add(new DiscoveryNode("data_2", buildNewFakeTransportAddress(), attr, data, null))
            .add(new DiscoveryNode("data_3", buildNewFakeTransportAddress(), attr, data, null))
            .add(new DiscoveryNode("data_4", buildNewFakeTransportAddress(), attr, data, null))
            .build();

        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList());
        Routing routing = routingProvider.forRandomMasterOrDataNode(new RelationName("doc", "table"), nodes);
        assertThat(routing.locations().keySet(), contains("local_data"));
    }
}
