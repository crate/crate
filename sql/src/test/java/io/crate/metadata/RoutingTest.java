/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class RoutingTest extends CrateUnitTest {

    @Test
    public void testStreamingWithLocations() throws Exception {
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        Map<String, List<Integer>> indexMap = new TreeMap<>();
        indexMap.put("index-0", Arrays.asList(1, 2));
        locations.put("node-0", indexMap);

        BytesStreamOutput out = new BytesStreamOutput();
        Routing routing1 = new Routing(locations);
        routing1.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        Routing routing2 = new Routing(in);

        assertThat(routing1.locations(), is(routing2.locations()));
    }

    @Test
    public void testStreamingWithoutLocations() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        Routing routing1 = new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of());
        routing1.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        Routing routing2 = new Routing(in);
        assertThat(routing1.locations(), is(routing2.locations()));
    }

    @Test
    public void testMixedShardAllocationsAreMerged() {
        Map<String, Map<String, List<Integer>>> exitingLocations = new TreeMap<>();
        Map<String, List<Integer>> indexMapExisting = new TreeMap<>();
        indexMapExisting.put("index-0", Arrays.asList(1, 2));
        indexMapExisting.put("index-1", Arrays.asList(1, 2));
        exitingLocations.put("node-0", indexMapExisting);
        indexMapExisting = new TreeMap<>();
        indexMapExisting.put("index-0", Arrays.asList(3, 4));
        indexMapExisting.put("index-1", Arrays.asList(3, 4));
        exitingLocations.put("node-1", indexMapExisting);

        Routing routing = new Routing(exitingLocations);
        routing.mergeLocations(exitingLocations);
        // Same locations must be returned
        assertThat(exitingLocations, is(routing.locations()));

        Map<String, Map<String, List<Integer>>> newLocations = new TreeMap<>();
        Map<String, List<Integer>> indexMapNew = new TreeMap<>();
        indexMapNew.put("index-0", Arrays.asList(3, 4, 5));
        indexMapNew.put("index-1", Arrays.asList(3, 4, 5));
        newLocations.put("node-0", indexMapNew);
        indexMapNew = new TreeMap<>();
        indexMapNew.put("index-0", Arrays.asList(1, 2, 6));
        indexMapNew.put("index-1", Arrays.asList(1, 2, 6));
        newLocations.put("node-1", indexMapNew);

        routing = new Routing(newLocations);
        routing.mergeLocations(exitingLocations);

        // Merged locations must be returned
        assertThat(routing.locations().keySet(), containsInAnyOrder("node-0", "node-1"));
        assertThat(routing.locations().get("node-0").keySet(), containsInAnyOrder("index-0", "index-1"));
        assertThat(routing.locations().get("node-1").keySet(), containsInAnyOrder("index-0", "index-1"));
        assertThat(routing.locations().get("node-0").get("index-0"), containsInAnyOrder(1, 2, 5));
        assertThat(routing.locations().get("node-0").get("index-1"), containsInAnyOrder(1, 2, 5));
        assertThat(routing.locations().get("node-1").get("index-0"), containsInAnyOrder(3, 4, 6));
        assertThat(routing.locations().get("node-1").get("index-1"), containsInAnyOrder(3, 4, 6));
    }

    @Test
    public void testRoutingForRandomMasterOrDataNode() throws IOException {
        Map<String, String> attr = ImmutableMap.of();
        Set<DiscoveryNode.Role> master_and_data = ImmutableSet.of(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA);
        DiscoveryNode local = new DiscoveryNode("client_node_1", LocalTransportAddress.buildUnique(), attr, ImmutableSet.of(), null);
        DiscoveryNodes nodes = new DiscoveryNodes.Builder()
            .add(new DiscoveryNode("data_master_node_1", LocalTransportAddress.buildUnique(), attr, master_and_data, null))
            .add(new DiscoveryNode("data_master_node_2", LocalTransportAddress.buildUnique(), attr, master_and_data, null))
            .add(local)
            .add(new DiscoveryNode("client_node_2", LocalTransportAddress.buildUnique(), attr, ImmutableSet.of(), null))
            .add(new DiscoveryNode("client_node_3", LocalTransportAddress.buildUnique(), attr, ImmutableSet.of(), null))
            .localNodeId(local.getId())
            .build();
        ClusterState state = new ClusterState.Builder(new ClusterName("my_cluster")).nodes(nodes).build();

        Routing routing = Routing.forRandomMasterOrDataNode(new TableIdent("doc", "table"), state);
        assertThat(routing.locations().keySet(), anyOf(contains("data_master_node_1"), contains("data_master_node_2")));
    }

    @Test
    public void testRoutingForRandomMasterOrDataNodePrefersLocal() throws Exception {
        Set<DiscoveryNode.Role> data = ImmutableSet.of(DiscoveryNode.Role.DATA);
        Map<String, String> attr = ImmutableMap.of();
        DiscoveryNode local = new DiscoveryNode("local_data", LocalTransportAddress.buildUnique(), attr, data, null);
        DiscoveryNodes nodes = new DiscoveryNodes.Builder()
            .add(local)
            .localNodeId(local.getId())
            .add(new DiscoveryNode("data_1", LocalTransportAddress.buildUnique(), attr, data, null))
            .add(new DiscoveryNode("data_2", LocalTransportAddress.buildUnique(), attr, data, null))
            .add(new DiscoveryNode("data_3", LocalTransportAddress.buildUnique(), attr, data, null))
            .add(new DiscoveryNode("data_4", LocalTransportAddress.buildUnique(), attr, data, null))
            .build();
        ClusterState state = new ClusterState.Builder(new ClusterName("my_cluster")).nodes(nodes).build();

        Routing routing = Routing.forRandomMasterOrDataNode(new TableIdent("doc", "table"), state);
        assertThat(routing.locations().keySet(), contains("local_data"));
    }
}
