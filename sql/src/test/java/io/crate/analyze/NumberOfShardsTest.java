/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.data.Row;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Locale;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NumberOfShardsTest extends CrateUnitTest {

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
    public void beforeTest() {
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
        assertThat(numberOfShards.defaultNumberOfShards(), is(6));
    }

    @Test
    public void testDefaultNumberOfShardsLessThanMinimumShardsNumber() {
        when(discoveryNodes.getDataNodes()).thenReturn(createDataNodes(1));
        numberOfShards = new NumberOfShards(clusterService);
        assertThat(numberOfShards.defaultNumberOfShards(), is(4));
    }

    @Test
    public void testGetNumberOfShards() {
        ClusteredBy clusteredBy = new ClusteredBy(Optional.of(QNAME_REF), Optional.of(LongLiteral.fromObject(7)));
        assertThat(numberOfShards.fromClusteredByClause(clusteredBy, Row.EMPTY), is(7));
    }

    @Test
    public void testGetNumberOfShardsLessThanOne() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("num_shards in CLUSTERED clause must be greater than 0");
        numberOfShards.fromClusteredByClause(
            new ClusteredBy(Optional.of(QNAME_REF), Optional.of(LongLiteral.fromObject(0))), Row.EMPTY);
    }

}
