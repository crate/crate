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

package io.crate.analyze;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Locale;
import java.util.Optional;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;

public class NumberOfShardsTest extends ESTestCase {

    private static final QualifiedNameReference QNAME_REF = new QualifiedNameReference(new QualifiedName("id"));
    private static ClusterService clusterService = mock(ClusterService.class);
    private static DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
    private static ClusterState clusterState = mock(ClusterState.class);
    private static NumberOfShards numberOfShards;

    @BeforeClass
    public static void beforeClass() {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
    }

    @Before
    public void setupNumberOfShards() {
        when(discoveryNodes.getDataNodes()).thenReturn(createDataNodes(3));
        numberOfShards = new NumberOfShards(clusterService);
    }

    private static ImmutableOpenMap<String, DiscoveryNode> createDataNodes(int numNodes) {
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodesBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < numNodes; i++) {
            dataNodesBuilder.put(String.format(Locale.ENGLISH, "node%s", i), mock(DiscoveryNode.class));
        }
        return dataNodesBuilder.build();
    }

    @Test
    public void testDefaultNumberOfShards() {
        assertThat(numberOfShards.defaultNumberOfShards()).isEqualTo(6);
    }

    @Test
    public void testDefaultNumberOfShardsLessThanMinimumShardsNumber() {
        when(discoveryNodes.getDataNodes()).thenReturn(createDataNodes(1));
        numberOfShards = new NumberOfShards(clusterService);
        assertThat(numberOfShards.defaultNumberOfShards()).isEqualTo(4);
    }

    @Test
    public void testGetNumberOfShards() {
        ClusteredBy<Object> clusteredBy = new ClusteredBy<>(Optional.of(QNAME_REF), Optional.of(7L));
        assertThat(numberOfShards.fromClusteredByClause(clusteredBy)).isEqualTo(7);
    }

    @Test
    public void testGetNumberOfShardsLessThanOne() {
        assertThatThrownBy(
            () -> numberOfShards.fromClusteredByClause(new ClusteredBy<>(Optional.of(QNAME_REF), Optional.of(0L))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("num_shards in CLUSTERED clause must be greater than 0");
    }
}
