/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.collectors.NodeStatsCollector;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.elasticsearch.test.ESAllocationTestCase.newNode;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeStatsCollectSourceTest extends CrateUnitTest {

    private ClusterService clusterService;

    @Before
    public void prepare() throws Exception {

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("parted").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index("parted")).build();
        ClusterState state = ClusterState
            .builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
            .metaData(metaData)
            .routingTable(routingTable)
            .build();

        state = ClusterState.builder(state).nodes(
            DiscoveryNodes.builder()
                .put(newNode("nodeOne"))
                .put(newNode("nodeTwo"))
                .put(newNode("nodeThree"))
                .localNodeId("nodeOne")).build();
        clusterService = new NoopClusterService(state);

    }

    @Test
    public void testFilterNodes() throws Exception {
        String id = "nodeThree";

        // build where clause with id = ?
        SysNodesTableInfo tableInfo = mock(SysNodesTableInfo.class);
        when(tableInfo.getReference(new ColumnIdent("id"))).thenReturn(
            new Reference(new ReferenceIdent(new TableIdent("sys", "nodes"), "id"), RowGranularity.DOC, DataTypes.STRING));

        TableRelation tableRelation = new TableRelation(tableInfo);
        Map<QualifiedName, AnalyzedRelation> tableSources = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
            new QualifiedName("sys.nodes"), tableRelation);
        SqlExpressions sqlExpressions = new SqlExpressions(tableSources, tableRelation);
        String where = String.format("id = '%s'", id);
        WhereClause whereClause = new WhereClause(sqlExpressions.normalize(sqlExpressions.asSymbol(where)));

        RoutedCollectPhase collectPhase = mock(RoutedCollectPhase.class);
        when(collectPhase.whereClause()).thenReturn(whereClause);

        NodeStatsCollectSource nodeStatsCollectSource = new NodeStatsCollectSource(
            mock(TransportActionProvider.class),
            clusterService,
            getFunctions()
        );

        Collection<CrateCollector> collectors = nodeStatsCollectSource.getCollectors(collectPhase, null, null);
        assertThat(collectors.size(), is(1));

        // check that the collector has only the requested node
        NodeStatsCollector collector = (NodeStatsCollector)collectors.iterator().next();
        final Field collectorNodes = NodeStatsCollector.class.getDeclaredField("nodes");
        collectorNodes.setAccessible(true);
        List<DiscoveryNode> discoveryNodes = (List<DiscoveryNode>)collectorNodes.get(collector);
        assertThat(discoveryNodes.size(), is(1));
        assertThat(discoveryNodes.get(0).getId(), is(id));
    }
}
