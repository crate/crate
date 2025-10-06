/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.upgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.test.IntegTestCase.resolveIndex;

import java.util.Collection;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;
import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.expression.symbol.InputColumn;
import io.crate.metadata.RelationName;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.operators.Fetch;
import io.crate.planner.operators.RootRelationBoundary;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class NodeOperationsUpgraderTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_collect_phase_routing_index_name_compatibility() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t (id int primary key)")
            .startShards("doc.t");

        ClusterState clusterState = clusterService.state();
        Index index = resolveIndex("doc.t", "doc", clusterState.metadata());
        String indexName = index.getName();
        String indexUUID = index.getUUID();

        Collect collect = e.plan("select id from doc.t");
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) collect.collectPhase();
        var orderBy = new OrderBy(List.of(new InputColumn(0)));
        collectPhase.orderBy(orderBy);
        collectPhase.nodePageSizeHint(20);

        assertThat(collectPhase.routing().locations().get("n1").get(indexUUID)).isNotNull();
        assertThat(collectPhase.routing().locations().get("n1").get(indexName)).isNull();

        Collection<? extends NodeOperation> nodeOperations = List.of(
            new NodeOperation(
                collectPhase,
                List.of("node1", "node2"),
                1,
                (byte) 0
            ));

        // Ensure downgrading to 5.10 format works as expected
        Collection<? extends NodeOperation> nodeOperations_5_10 = NodeOperationsUpgrader.downgrade(
            nodeOperations,
            Version.V_5_10_11,
            clusterState.metadata()
        );
        RoutedCollectPhase collectPhase_5_10 = (RoutedCollectPhase) nodeOperations_5_10.iterator().next().executionPhase();

        assertThat(collectPhase_5_10.orderBy()).isEqualTo(orderBy);
        assertThat(collectPhase_5_10.nodePageSizeHint()).isEqualTo(20);
        assertThat(collectPhase_5_10.routing().locations().get("n1").get(indexUUID)).isNull();
        assertThat(collectPhase_5_10.routing().locations().get("n1").get(indexName)).isNotNull();

        // Ensure upgrading back to the current format works as expected
        Collection<? extends NodeOperation> nodeOperations_upgraded = NodeOperationsUpgrader.upgrade(
            nodeOperations_5_10,
            Version.V_5_10_11,
            clusterState.metadata()
        );
        RoutedCollectPhase collectPhase_upgraded = (RoutedCollectPhase) nodeOperations_upgraded.iterator().next().executionPhase();
        assertThat(collectPhase_upgraded.routing().locations().get("n1").get(indexUUID)).isNotNull();
        assertThat(collectPhase.routing().locations().get("n1").get(indexName)).isNull();
        assertThat(collectPhase_upgraded).isEqualTo(collectPhase);
    }


    @Test
    public void test_count_phase_routing_index_name_compatibility() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t (id int primary key)")
            .startShards("doc.t");

        ClusterState clusterState = clusterService.state();
        Index index = resolveIndex("doc.t", "doc", clusterState.metadata());
        String indexName = index.getName();
        String indexUUID = index.getUUID();

        CountPlan countPlan = e.plan("select count(*) from doc.t");
        CountPhase countPhase = (CountPhase) countPlan.countPhase();
        assertThat(countPhase.routing().locations().get("n1").get(indexUUID)).isNotNull();
        assertThat(countPhase.routing().locations().get("n1").get(indexName)).isNull();

        Collection<? extends NodeOperation> nodeOperations = List.of(
            new NodeOperation(
                countPhase,
                List.of("node1", "node2"),
                1,
                (byte) 0
            ));

        // Ensure downgrading to 5.10 format works as expected
        Collection<? extends NodeOperation> nodeOperations_5_10 = NodeOperationsUpgrader.downgrade(
            nodeOperations,
            Version.V_5_10_11,
            clusterState.metadata()
        );
        CountPhase countPhase_5_10 = (CountPhase) nodeOperations_5_10.iterator().next().executionPhase();
        assertThat(countPhase_5_10.routing().locations().get("n1").get(indexUUID)).isNull();
        assertThat(countPhase_5_10.routing().locations().get("n1").get(indexName)).isNotNull();

        // Ensure upgrading back to the current format works as expected
        Collection<? extends NodeOperation> nodeOperations_upgraded = NodeOperationsUpgrader.upgrade(
            nodeOperations_5_10,
            Version.V_5_10_11,
            clusterState.metadata()
        );
        CountPhase countPhase_upgraded = (CountPhase) nodeOperations_upgraded.iterator().next().executionPhase();
        assertThat(countPhase_upgraded.routing().locations().get("n1").get(indexUUID)).isNotNull();
        assertThat(countPhase.routing().locations().get("n1").get(indexName)).isNull();
        assertThat(countPhase_upgraded).isEqualTo(countPhase);
    }

    @Test
    public void test_fetch_phase_index_name_compatibility() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t (id int primary key)")
            .startShards("doc.t");

        RelationName relationName = new RelationName("doc", "t");
        ClusterState clusterState = clusterService.state();
        Index index = resolveIndex("doc.t", "doc", clusterState.metadata());
        String indexName = index.getName();
        String indexUUID = index.getUUID();

        RootRelationBoundary root = e.logicalPlan("select id from doc.t where id > 1 limit 1");
        Fetch fetch = (Fetch) root.source();
        QueryThenFetch queryThenFetch = e.buildPlan(fetch);

        FetchPhase fetchPhase = queryThenFetch.fetchPhase();
        assertThat(fetchPhase.bases().get(indexUUID)).isNotNull();
        assertThat(fetchPhase.bases().get(indexName)).isNull();
        assertThat(fetchPhase.tableIndices().get(relationName)).contains(indexUUID);

        Collection<? extends NodeOperation> nodeOperations = List.of(
            new NodeOperation(
                fetchPhase,
                List.of("node1", "node2"),
                1,
                (byte) 0
            ));

        // Ensure downgrading to 5.10 format works as expected
        Collection<? extends NodeOperation> nodeOperations_5_10 = NodeOperationsUpgrader.downgrade(
            nodeOperations,
            Version.V_5_10_11,
            clusterState.metadata()
        );

        FetchPhase fetchPhase_5_10 = (FetchPhase) nodeOperations_5_10.iterator().next().executionPhase();
        assertThat(fetchPhase_5_10.bases().get(indexUUID)).isNull();
        assertThat(fetchPhase_5_10.bases().get(indexName)).isNotNull();
        assertThat(fetchPhase_5_10.tableIndices().get(relationName)).contains(indexName);

        // Ensure upgrading back to the current format works as expected
        Collection<? extends NodeOperation> nodeOperations_upgraded = NodeOperationsUpgrader.upgrade(
            nodeOperations_5_10,
            Version.V_5_10_11,
            clusterState.metadata()
        );
        FetchPhase fetchPhase_upgraded = (FetchPhase) nodeOperations_upgraded.iterator().next().executionPhase();
        assertThat(fetchPhase_upgraded.bases().get(indexUUID)).isNotNull();
        assertThat(fetchPhase_upgraded.bases().get(indexName)).isNull();
        assertThat(fetchPhase_upgraded.tableIndices().get(relationName)).contains(indexUUID);
    }

    @Test
    public void test_node_operations_upgrader_returns_same_for_current_version() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t (id int primary key)")
            .startShards("doc.t");

        Collect collect = e.plan("select id from doc.t");
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) collect.collectPhase();

        Collection<? extends NodeOperation> nodeOperations = List.of(
            new NodeOperation(
                collectPhase,
                List.of("node1", "node2"),
                1,
                (byte) 0
            ));

        // Nothing should change when downgrading to the current version
        Collection<? extends NodeOperation> nodeOperations2 = NodeOperationsUpgrader.downgrade(
            nodeOperations,
            Version.CURRENT,
            clusterService.state().metadata()
        );
        assertThat(nodeOperations2).isEqualTo(nodeOperations);

        // Nothing should change when upgrading to the current version
        Collection<? extends NodeOperation> nodeOperations3 = NodeOperationsUpgrader.upgrade(
            nodeOperations,
            Version.CURRENT,
            clusterService.state().metadata()
        );
        assertThat(nodeOperations3).isEqualTo(nodeOperations);
    }
}
