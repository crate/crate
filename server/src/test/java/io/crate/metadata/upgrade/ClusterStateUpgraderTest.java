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

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.test.IntegTestCase.resolveIndex;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ClusterStateUpgraderTest extends CrateDummyClusterServiceUnitTest {

    private final MetadataUpgradeService metadataUpgradeService = new MetadataUpgradeService(
        createNodeContext(),
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
        null);

    @Test
    public void test_routing_table_index_name_compatibility() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t (id int primary key)")
            .startShards("doc.t");

        ClusterState clusterState = clusterService.state();
        Index index = resolveIndex("doc.t", "doc", clusterState.metadata());
        String indexUUID = index.getUUID();
        String indexName = index.getName();

        assertThat(clusterState.routingTable().index(indexUUID)).isNotNull();
        assertThat(clusterState.routingTable().index(indexName)).isNull();

        // Ensure downgrading to 5.10 format works as expected
        ClusterState clusterState_5_10 = ClusterStateUpgrader.downgrade(clusterState, Version.V_5_10_11);
        assertThat(clusterState_5_10.routingTable().index(indexUUID)).isNull();
        assertThat(clusterState_5_10.routingTable().index(indexName)).isNotNull();

        // Ensure upgrading back to the current format works as expected
        ClusterState upgradedClusterState = ClusterStateUpgrader.upgrade(
            clusterState_5_10,
            Version.V_5_10_11,
            metadataUpgradeService
        );
        assertThat(upgradedClusterState.routingTable().index(indexUUID)).isNotNull();
        assertThat(upgradedClusterState.routingTable().index(indexName)).isNull();
        assertThat(upgradedClusterState.routingTable()).isEqualTo(clusterState.routingTable());
    }

    @Test
    public void test_cluster_blocks_index_name_compatibility() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true)
            .build();
        SQLExecutor e = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t (id int primary key)", settings)
            .startShards("doc.t");

        ClusterState clusterState = clusterService.state();
        Index index = resolveIndex("doc.t", "doc", clusterState.metadata());
        String indexUUID = index.getUUID();
        String indexName = index.getName();

        clusterState = ClusterState.builder(clusterState)
            .blocks(ClusterBlocks.builder().addIndexBlock(indexUUID, IndexMetadata.INDEX_READ_ONLY_BLOCK))
            .build();

        assertThat(clusterState.blocks().hasIndexBlock(indexUUID, IndexMetadata.INDEX_READ_ONLY_BLOCK)).isTrue();
        assertThat(clusterState.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_BLOCK)).isFalse();

        // Ensure downgrading to 5.10 format works as expected
        ClusterState clusterState_5_10 = ClusterStateUpgrader.downgrade(clusterState, Version.V_5_10_11);
        assertThat(clusterState_5_10.blocks().hasIndexBlock(indexUUID, IndexMetadata.INDEX_READ_ONLY_BLOCK)).isFalse();
        assertThat(clusterState_5_10.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_BLOCK)).isTrue();

        // Ensure upgrading back to the current format works as expected
        ClusterState upgradedClusterState = ClusterStateUpgrader.upgrade(
            clusterState_5_10,
            Version.V_5_10_11,
            metadataUpgradeService
        );
        assertThat(upgradedClusterState.blocks().hasIndexBlock(indexUUID, IndexMetadata.INDEX_READ_ONLY_BLOCK)).isTrue();
        assertThat(upgradedClusterState.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_BLOCK)).isFalse();
        assertThat(upgradedClusterState.blocks()).isEqualTo(clusterState.blocks());
    }

    @Test
    public void test_state_is_not_changed_when_upgrading_downgrading_to_current_version() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true)
            .build();
        SQLExecutor e = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t (id int primary key)", settings)
            .startShards("doc.t");

        ClusterState clusterState = clusterService.state();

        ClusterState clusterState2 = ClusterStateUpgrader.downgrade(clusterState, Version.CURRENT);
        assertThat(clusterState2).isEqualTo(clusterState);

        ClusterState clusterState3 = ClusterStateUpgrader.upgrade(clusterState, Version.CURRENT, metadataUpgradeService);
        assertThat(clusterState3).isEqualTo(clusterState);
    }
}
