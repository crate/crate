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

package io.crate.execution.engine.collect.sources;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static io.crate.testing.DiscoveryNodes.newNode;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeStatsCollectSourceTest extends CrateUnitTest {

    private Collection<DiscoveryNode> discoveryNodes;

    @Before
    public void prepare() throws Exception {
        discoveryNodes = new ArrayList<>();
        discoveryNodes.add(newNode("Ford Perfect", "node-1"));
        discoveryNodes.add(newNode("Trillian", "node-2"));
        discoveryNodes.add(newNode("Arthur", "node-3"));
    }

    private List<DiscoveryNode> filterNodes(String where) throws NoSuchFieldException, IllegalAccessException {
        // build where clause with id = ?
        SysNodesTableInfo tableInfo = mock(SysNodesTableInfo.class);
        when(tableInfo.ident()).thenReturn(new RelationName("sys", "nodes"));
        when(tableInfo.getReadReference(new ColumnIdent("id"))).thenReturn(
            new Reference(new ReferenceIdent(new RelationName("sys", "nodes"), "id"),
                          RowGranularity.DOC,
                          DataTypes.STRING,
                          null,
                          null));
        when(tableInfo.getReadReference(SysNodesTableInfo.Columns.NAME)).thenReturn(
            new Reference(
                new ReferenceIdent(SysNodesTableInfo.IDENT, SysNodesTableInfo.Columns.NAME),
                RowGranularity.DOC,
                DataTypes.STRING,
                null,
                null
            )
        );
        when(tableInfo.getReadReference(SysNodesTableInfo.Columns.HOSTNAME)).thenReturn(
            new Reference(
                new ReferenceIdent(SysNodesTableInfo.IDENT, SysNodesTableInfo.Columns.HOSTNAME),
                RowGranularity.DOC,
                DataTypes.STRING,
                null,
                null
            )
        );


        TableRelation tableRelation = new TableRelation(tableInfo);
        Map<RelationName, AnalyzedRelation> tableSources = Map.of(tableInfo.ident(), tableRelation);
        SqlExpressions sqlExpressions = new SqlExpressions(tableSources, tableRelation);
        Symbol query = sqlExpressions.normalize(sqlExpressions.asSymbol(where));

        List<DiscoveryNode> nodes = new ArrayList<>(NodeStatsCollectSource.filterNodes(
            discoveryNodes,
            query,
            getFunctions()));
        nodes.sort(Comparator.comparing(DiscoveryNode::getId));
        return nodes;
    }

    @Test
    public void testFilterNodesById() throws Exception {
        List<DiscoveryNode> discoveryNodes = filterNodes("id = 'node-3'");
        assertThat(discoveryNodes.size(), is(1));
        assertThat(discoveryNodes.get(0).getId(), is("node-3"));

        // Filter for two nodes by id
        discoveryNodes = filterNodes("id = 'node-3' or id ='node-2' or id='unknown'");
        assertThat(discoveryNodes.size(), is(2));
        assertThat(discoveryNodes.get(0).getId(), is("node-2"));
        assertThat(discoveryNodes.get(1).getId(), is("node-3"));
    }

    @Test
    public void testFilterNodesByName() throws Exception {
        List<DiscoveryNode> discoveryNodes = filterNodes("name = 'Arthur'");
        assertThat(discoveryNodes.size(), is(1));
        assertThat(discoveryNodes.get(0).getId(), is("node-3"));

        // Filter for two nodes by id
        discoveryNodes = filterNodes("name = 'Arthur' or name ='Trillian' or name='unknown'");
        assertThat(discoveryNodes.size(), is(2));
        assertThat(discoveryNodes.get(0).getId(), is("node-2"));
        assertThat(discoveryNodes.get(1).getId(), is("node-3"));
    }

    @Test
    public void testMixedFilter() throws Exception {
        List<DiscoveryNode> discoveryNodes = filterNodes("name = 'Arthur' or id ='node-2' or name='unknown'");
        assertThat(discoveryNodes.size(), is(2));
        assertThat(discoveryNodes.get(0).getId(), is("node-2"));
        assertThat(discoveryNodes.get(1).getId(), is("node-3"));


        discoveryNodes = filterNodes("name = 'Arthur' or id ='node-2' or hostname='unknown'");
        assertThat(discoveryNodes.size(), is(3));

        discoveryNodes = filterNodes("name = 'Arthur' or id ='node-2' and hostname='unknown'");
        assertThat(discoveryNodes.size(), is(2));
        assertThat(discoveryNodes.get(0).getId(), is("node-2"));
        assertThat(discoveryNodes.get(1).getId(), is("node-3"));

        discoveryNodes = filterNodes("name = 'Arthur' and id = 'node-2'");
        assertThat(discoveryNodes.size(), is(0));
    }

    @Test
    public void testNoLocalInfoInWhereClause() throws Exception {
        List<DiscoveryNode> discoveryNodes = filterNodes("hostname = 'localhost'");
        assertThat(discoveryNodes.size(), is(3));
    }

}
