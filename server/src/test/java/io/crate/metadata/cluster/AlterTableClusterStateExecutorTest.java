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

package io.crate.metadata.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.analyze.TableParameters;
import io.crate.execution.ddl.tables.AlterTableRequest;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AlterTableClusterStateExecutorTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testMarkArchivedSettings() {
        Settings.Builder builder = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 4);
        Settings preparedSettings = AlterTableClusterStateExecutor.markArchivedSettings(builder.build());
        assertThat(preparedSettings.keySet()).containsExactlyInAnyOrder(SETTING_NUMBER_OF_SHARDS, ARCHIVED_SETTINGS_PREFIX + "*");
    }

    @Test
    public void test_group_settings_are_not_filtered_out() {
        String fullName = INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "." + "_name";
        Settings settingToFilter = Settings.builder()
            .put(fullName , "node1").build();

        List<Setting<?>> supportedSettings = TableParameters.PARTITIONED_TABLE_PARAMETER_INFO_FOR_TEMPLATE_UPDATE
            .supportedSettings()
            .values()
            .stream()
            .toList();

        Settings filteredSettings = AlterTableClusterStateExecutor.filterSettings(settingToFilter, supportedSettings);
        assertThat(filteredSettings.isEmpty()).isFalse();
        assertThat(filteredSettings.get(fullName)).isEqualTo("node1");
    }

    @Test
    public void test_uses_table_oid_if_source_name_points_to_another_table() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (x int) with (number_of_replicas = 0)")
            .addTable("create table t2 (x int) with (number_of_replicas = 0)");
        RelationName t1Name = new RelationName("doc", "t1");
        RelationName t2Name = new RelationName("doc", "t2");

        Metadata metadata = e.getPlannerContext().clusterState().metadata();
        RelationMetadata.Table t1 = metadata.getRelation(t1Name);
        Metadata metadataAfterSwap = swapRelations(metadata, t1Name, t2Name);
        ClusterState stateAfterSwap = ClusterState.builder(clusterService.state())
            .metadata(metadataAfterSwap)
            .build();

        AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        MetadataUpdateSettingsService updateSettingsService = new MetadataUpdateSettingsService(
            clusterService,
            allocationService,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            mock(IndicesService.class),
            new ShardLimitValidator(Settings.EMPTY, clusterService)
        );
        AlterTableClusterStateExecutor executor = new AlterTableClusterStateExecutor(
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            updateSettingsService,
            e.getPlannerContext().nodeContext()
        );

        ClusterState result = executor.execute(
            stateAfterSwap,
            new AlterTableRequest(
                t1Name,
                t1.oid(),
                List.of(),
                false,
                false,
                Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, "1").build()
            )
        );

        RelationMetadata.Table currentT1 = result.metadata().getRelation(t1Name);
        RelationMetadata.Table currentT2 = result.metadata().getRelation(t2Name);
        assertThat(currentT1.settings().get(SETTING_NUMBER_OF_REPLICAS)).isEqualTo("0");
        assertThat(currentT2.settings().get(SETTING_NUMBER_OF_REPLICAS)).isEqualTo("1");
        assertThat(result.metadata().index(currentT1.indexUUIDs().getFirst()).getSettings().get(SETTING_NUMBER_OF_REPLICAS))
            .isEqualTo("0");
        assertThat(result.metadata().index(currentT2.indexUUIDs().getFirst()).getSettings().get(SETTING_NUMBER_OF_REPLICAS))
            .isEqualTo("1");
    }

    @Test
    public void test_falls_back_to_relation_name_if_table_oid_is_unassigned() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (x int) with (number_of_replicas = 0)")
            .addTable("create table t2 (x int) with (number_of_replicas = 0)");
        RelationName t1Name = new RelationName("doc", "t1");
        RelationName t2Name = new RelationName("doc", "t2");

        Metadata metadata = e.getPlannerContext().clusterState().metadata();
        Metadata metadataAfterSwap = swapRelations(metadata, t1Name, t2Name);
        ClusterState stateAfterSwap = ClusterState.builder(clusterService.state())
            .metadata(metadataAfterSwap)
            .build();

        AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        MetadataUpdateSettingsService updateSettingsService = new MetadataUpdateSettingsService(
            clusterService,
            allocationService,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            mock(IndicesService.class),
            new ShardLimitValidator(Settings.EMPTY, clusterService)
        );
        AlterTableClusterStateExecutor executor = new AlterTableClusterStateExecutor(
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            updateSettingsService,
            e.getPlannerContext().nodeContext()
        );

        ClusterState result = executor.execute(
            stateAfterSwap,
            new AlterTableRequest(
                t1Name,
                OID_UNASSIGNED,
                List.of(),
                false,
                false,
                Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, "1").build()
            )
        );

        RelationMetadata.Table currentT1 = result.metadata().getRelation(t1Name);
        RelationMetadata.Table currentT2 = result.metadata().getRelation(t2Name);
        assertThat(currentT1.settings().get(SETTING_NUMBER_OF_REPLICAS)).isEqualTo("1");
        assertThat(currentT2.settings().get(SETTING_NUMBER_OF_REPLICAS)).isEqualTo("0");
        assertThat(result.metadata().index(currentT1.indexUUIDs().getFirst()).getSettings().get(SETTING_NUMBER_OF_REPLICAS))
            .isEqualTo("1");
        assertThat(result.metadata().index(currentT2.indexUUIDs().getFirst()).getSettings().get(SETTING_NUMBER_OF_REPLICAS))
            .isEqualTo("0");
    }

    @Test
    public void test_partition_update_uses_table_oid_if_source_name_points_to_another_table() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS
            )
            .addTable("create table other (x int) with (number_of_replicas = 0)");
        RelationName partedName = new RelationName("doc", "parted");
        RelationName otherName = new RelationName("doc", "other");

        Metadata metadata = e.getPlannerContext().clusterState().metadata();
        RelationMetadata.Table parted = metadata.getRelation(partedName);
        Metadata metadataAfterSwap = swapRelations(metadata, partedName, otherName);
        ClusterState stateAfterSwap = ClusterState.builder(clusterService.state())
            .metadata(metadataAfterSwap)
            .build();

        AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        MetadataUpdateSettingsService updateSettingsService = new MetadataUpdateSettingsService(
            clusterService,
            allocationService,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            mock(IndicesService.class),
            new ShardLimitValidator(Settings.EMPTY, clusterService)
        );
        AlterTableClusterStateExecutor executor = new AlterTableClusterStateExecutor(
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            updateSettingsService,
            e.getPlannerContext().nodeContext()
        );

        ClusterState result = executor.execute(
            stateAfterSwap,
            new AlterTableRequest(
                partedName,
                parted.oid(),
                List.of("1395874800000"),
                true,
                false,
                Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, "1").build()
            )
        );

        String partitionIndexUUID = result.metadata().getIndex(
            otherName,
            List.of("1395874800000"),
            true,
            IndexMetadata::getIndexUUID
        );
        assertThat(result.metadata().index(partitionIndexUUID).getSettings().get(SETTING_NUMBER_OF_REPLICAS))
            .isEqualTo("1");
    }

    @Test
    public void test_partition_update_falls_back_to_relation_name_if_table_oid_is_unassigned() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(
                "create table doc.parted (p int) partitioned by (p) with (number_of_replicas = 0)",
                List.of("1")
            )
            .addTable(
                "create table doc.other_parted (p int) partitioned by (p) with (number_of_replicas = 0)",
                List.of("1")
            );
        RelationName partedName = new RelationName("doc", "parted");
        RelationName otherName = new RelationName("doc", "other_parted");

        Metadata metadata = e.getPlannerContext().clusterState().metadata();
        String originalPartedIndexUUID = metadata.getIndex(
            partedName,
            List.of("1"),
            true,
            IndexMetadata::getIndexUUID
        );
        String originalOtherIndexUUID = metadata.getIndex(
            otherName,
            List.of("1"),
            true,
            IndexMetadata::getIndexUUID
        );
        Metadata metadataAfterSwap = swapRelations(metadata, partedName, otherName);
        ClusterState stateAfterSwap = ClusterState.builder(clusterService.state())
            .metadata(metadataAfterSwap)
            .build();

        AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        MetadataUpdateSettingsService updateSettingsService = new MetadataUpdateSettingsService(
            clusterService,
            allocationService,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            mock(IndicesService.class),
            new ShardLimitValidator(Settings.EMPTY, clusterService)
        );
        AlterTableClusterStateExecutor executor = new AlterTableClusterStateExecutor(
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            updateSettingsService,
            e.getPlannerContext().nodeContext()
        );

        ClusterState result = executor.execute(
            stateAfterSwap,
            new AlterTableRequest(
                partedName,
                OID_UNASSIGNED,
                List.of("1"),
                true,
                false,
                Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, "1").build()
            )
        );

        assertThat(result.metadata().index(originalOtherIndexUUID).getSettings().get(SETTING_NUMBER_OF_REPLICAS))
            .isEqualTo("1");
        assertThat(result.metadata().index(originalPartedIndexUUID).getSettings().get(SETTING_NUMBER_OF_REPLICAS))
            .isEqualTo("0");
    }

    private static Metadata swapRelations(Metadata metadata, RelationName leftName, RelationName rightName) {
        RelationMetadata.Table left = metadata.getRelation(leftName);
        RelationMetadata.Table right = metadata.getRelation(rightName);
        return Metadata.builder(metadata)
            .dropRelation(leftName)
            .dropRelation(rightName)
            .setTable(
                leftName,
                right.columns().stream().map(ref -> ref.withRelation(leftName)).toList(),
                right.settings(),
                right.routingColumn(),
                right.columnPolicy(),
                right.pkConstraintName(),
                right.checkConstraints(),
                right.primaryKeys(),
                right.partitionedBy(),
                right.state(),
                right.indexUUIDs(),
                right.tableVersion() + 1,
                right.oid()
            )
            .setTable(
                rightName,
                left.columns().stream().map(ref -> ref.withRelation(rightName)).toList(),
                left.settings(),
                left.routingColumn(),
                left.columnPolicy(),
                left.pkConstraintName(),
                left.checkConstraints(),
                left.primaryKeys(),
                left.partitionedBy(),
                left.state(),
                left.indexUUIDs(),
                left.tableVersion() + 1,
                left.oid()
            )
            .build();
    }
}
