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

package io.crate.execution.engine.collect.sources;

import static io.crate.testing.DiscoveryNodes.newNode;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.testing.SqlExpressions;

public class NodeStatsCollectSourceTest extends ESTestCase {

    private DiscoveryNodes discoveryNodes;

    @Before
    public void prepare() throws Exception {
        discoveryNodes = DiscoveryNodes.builder()
            .masterNodeId("node-2")
            .add(newNode("Ford Perfect", "node-1"))
            .add(newNode("Trillian", "node-2"))
            .add(newNode("Arthur", "node-3"))
            .build();
    }

    private DiscoveryNodes filterNodes(String where) {
        // build where clause with id = ?
        TableInfo tableInfo = SysNodesTableInfo.INSTANCE;
        TableRelation tableRelation = new TableRelation(tableInfo);
        Map<RelationName, AnalyzedRelation> tableSources = Map.of(tableInfo.ident(), tableRelation);
        SqlExpressions sqlExpressions = new SqlExpressions(tableSources, tableRelation);
        Symbol query = sqlExpressions.normalize(sqlExpressions.asSymbol(where));

        return NodeStatsCollectSource.filterNodes(
            discoveryNodes,
            query,
            sqlExpressions.nodeCtx);
    }

    @Test
    public void testFilterNodesById() throws Exception {
        DiscoveryNodes discoveryNodes = filterNodes("id = 'node-3'");
        assertThat(discoveryNodes).satisfiesExactly(
            d -> assertThat(d.getId()).isEqualTo("node-3")
        );

        // Filter for two nodes by id
        discoveryNodes = filterNodes("id = 'node-3' or id ='node-2' or id='unknown'");
        assertThat(discoveryNodes).satisfiesExactlyInAnyOrder(
            d -> assertThat(d.getId()).isEqualTo("node-2"),
            d -> assertThat(d.getId()).isEqualTo("node-3")
        );
    }

    @Test
    public void testFilterNodesByName() throws Exception {
        DiscoveryNodes discoveryNodes = filterNodes("name = 'Arthur'");
        assertThat(discoveryNodes).satisfiesExactly(
            d -> assertThat(d.getId()).isEqualTo("node-3")
        );

        // Filter for two nodes by id
        discoveryNodes = filterNodes("name = 'Arthur' or name ='Trillian' or name='unknown'");
        assertThat(discoveryNodes).satisfiesExactlyInAnyOrder(
            d -> assertThat(d.getId()).isEqualTo("node-2"),
            d -> assertThat(d.getId()).isEqualTo("node-3")
        );
    }

    @Test
    public void testMixedFilter() throws Exception {
        DiscoveryNodes discoveryNodes = filterNodes("name = 'Arthur' or id ='node-2' or name='unknown'");
        assertThat(discoveryNodes).satisfiesExactlyInAnyOrder(
            d -> assertThat(d.getId()).isEqualTo("node-2"),
            d -> assertThat(d.getId()).isEqualTo("node-3")
        );


        discoveryNodes = filterNodes("name = 'Arthur' or id ='node-2' or hostname='unknown'");
        assertThat(discoveryNodes).hasSize(3);

        discoveryNodes = filterNodes("name = 'Arthur' or id ='node-2' and hostname='unknown'");
        assertThat(discoveryNodes).satisfiesExactlyInAnyOrder(
            d -> assertThat(d.getId()).isEqualTo("node-2"),
            d -> assertThat(d.getId()).isEqualTo("node-3")
        );

        discoveryNodes = filterNodes("name = 'Arthur' and id = 'node-2'");
        assertThat(discoveryNodes).hasSize(0);
    }

    @Test
    public void testNoLocalInfoInWhereClause() throws Exception {
        DiscoveryNodes discoveryNodes = filterNodes("hostname = 'localhost'");
        assertThat(discoveryNodes).hasSize(3);
    }
}
