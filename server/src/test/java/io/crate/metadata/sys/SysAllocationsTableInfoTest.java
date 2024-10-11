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

package io.crate.metadata.sys;


import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.elasticsearch.test.ClusterServiceUtils.setState;

import java.util.Collections;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class SysAllocationsTableInfoTest extends CrateDummyClusterServiceUnitTest {

    @Before
    public void add_non_data_node() {
        var discoveryNode = new DiscoveryNode(
            "data_only_node",
            "d1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DATA_ROLE),
            Version.CURRENT
        );
        var nodes = DiscoveryNodes.builder(clusterService.state().nodes())
            .add(discoveryNode)
            .localNodeId("d1")
            .build();
        var builder = ClusterState.builder(clusterService.state()).nodes(nodes);
        setState(clusterService, builder);
    }

    @Test
    public void test_table_is_routed_to_master_node() {
        var allocationsTable = SysAllocationsTableInfo.INSTANCE;
        var routing = allocationsTable.getRouting(clusterService.state(), null, null, null, null);
        assertThat(routing.nodes()).contains(NODE_ID);
    }
}
