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

package org.elasticsearch.snapshots;

import static io.crate.analyze.SnapshotSettings.IGNORE_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.snapshots.RestoreService.resolveRelations;
import static org.elasticsearch.test.IntegTestCase.resolveIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TableOrPartition;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RestoreServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void resolve_indices_multiple_tables_specified() throws Exception {
        SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE my_schema.table1 (id INT, name STRING)")
            .addTable("CREATE TABLE my_schema.table2 (id INT, name STRING)");

        Metadata metadata = clusterService.state().metadata();
        String indexUUID1 = resolveIndex("my_schema.table1", null, metadata).getUUID();
        String indexUUID2 = resolveIndex("my_schema.table2", null, metadata).getUUID();

        RelationName relationName1 = new RelationName("my_schema", "table1");
        RelationName relationName2 = new RelationName("my_schema", "table2");
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(relationName1, null),
            new TableOrPartition(relationName2, null)
        );

        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new SnapshotId("snapshot1", UUIDs.randomBase64UUID()),
            List.of(),
            SnapshotState.SUCCESS
        );
        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(
            "repo1",
            snapshotInfo.snapshotId().getUUID(),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Settings.EMPTY,
            TimeValue.timeValueSeconds(30),
            true,
            true,
            Strings.EMPTY_ARRAY,
            true,
            Strings.EMPTY_ARRAY
        );

        Map<RelationName, RestoreService.RestoreRelation> restoreRelations = resolveRelations(
            tablesToRestore,
            restoreRequest,
            metadata,
            snapshotInfo
        );

        RestoreService.RestoreRelation relation1 = restoreRelations.get(relationName1);
        assertThat(relation1).isNotNull();
        assertThat(relation1.restoreIndices()).containsExactly(new RestoreService.RestoreIndex(indexUUID1, List.of()));
        RestoreService.RestoreRelation relation2 = restoreRelations.get(relationName2);
        assertThat(relation2).isNotNull();
        assertThat(relation2.restoreIndices()).containsExactly(new RestoreService.RestoreIndex(indexUUID2, List.of()));
    }

    @Test
    public void test_resolve_index_with_ignore_unavailable() throws Exception {
        SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE doc.my_table (id INT, name STRING) PARTITIONED BY (name)");

        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "my_table");
        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new SnapshotId("snapshot1", UUIDs.randomBase64UUID()),
            List.of(),
            SnapshotState.SUCCESS
        );
        var restoreRequest = new RestoreService.RestoreRequest(
            "repo1",
            snapshotInfo.snapshotId().getName(),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Settings.builder().put(IGNORE_UNAVAILABLE.getKey(), true).build(),
            TimeValue.timeValueSeconds(30),
            true,
            true,
            Strings.EMPTY_ARRAY,
            true,
            Strings.EMPTY_ARRAY
        );
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(relationName, "046jcchm6krj4e1g60o30c0")
        );

        Metadata metadata = clusterService.state().metadata();
        Map<RelationName, RestoreService.RestoreRelation> restoreRelations = resolveRelations(
            tablesToRestore,
            restoreRequest,
            metadata,
            snapshotInfo
        );

        RestoreService.RestoreRelation relation = restoreRelations.get(relationName);
        // Since the concrete partition does not exist, the relation itself should not be resolved as well
        assertThat(relation).isNull();
    }

    @Test
    public void test_resolve_partitioned_table_index_from_snapshot() throws Exception {
        SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE doc.restoreme (id INT, name STRING) PARTITIONED BY (name)", ".partitioned.restoreme.046jcchm6krj4e1g60o30c0");

        Metadata metadata = clusterService.state().metadata();
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "restoreme");
        String indexUUID1 = resolveIndex("doc.restoreme", PartitionName.decodeIdent("046jcchm6krj4e1g60o30c0"), null, metadata).getUUID();

        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new SnapshotId("snapshot1", UUIDs.randomBase64UUID()),
            List.of(),
            SnapshotState.SUCCESS
        );

        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(
            "repo1",
            snapshotInfo.snapshotId().getName(),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Settings.EMPTY,
            TimeValue.timeValueSeconds(30),
            true,
            true,
            Strings.EMPTY_ARRAY,
            true,
            Strings.EMPTY_ARRAY
        );
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(relationName, "046jcchm6krj4e1g60o30c0")
        );

        Map<RelationName, RestoreService.RestoreRelation> restoreRelations = resolveRelations(
            tablesToRestore,
            restoreRequest,
            metadata,
            snapshotInfo
        );

        RestoreService.RestoreRelation relation = restoreRelations.get(relationName);
        assertThat(relation).isNotNull();
        assertThat(relation.restoreIndices()).containsExactly(new RestoreService.RestoreIndex(indexUUID1, List.of("626572800000")));
    }

    @Test
    public void test_resolve_empty_partitioned_table() throws Exception{
        SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE doc.restoreme (id INT, name STRING) PARTITIONED BY (name)");

        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "restoreme");
        Metadata metadata = clusterService.state().metadata();

        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new SnapshotId("snapshot1", UUIDs.randomBase64UUID()),
            List.of(),
            SnapshotState.SUCCESS
        );

        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(
            "repo1",
            snapshotInfo.snapshotId().getName(),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Settings.EMPTY,
            TimeValue.timeValueSeconds(30),
            true,
            true,
            Strings.EMPTY_ARRAY,
            true,
            Strings.EMPTY_ARRAY
        );
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(relationName, null)
        );

        Map<RelationName, RestoreService.RestoreRelation> restoreRelations = resolveRelations(
            tablesToRestore,
            restoreRequest,
            metadata,
            snapshotInfo
        );

        RestoreService.RestoreRelation relation = restoreRelations.get(relationName);
        assertThat(relation).isNotNull();
        assertThat(relation.targetName()).isEqualTo(relationName);
        assertThat(relation.restoreIndices()).isEmpty();
    }


    @Test
    public void test_resolve_multi_tables_index_names_from_snapshot() throws Exception {
        SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE doc.my_table (id INT, name STRING)")
            .addTable("CREATE TABLE doc.my_partitioned_table (id INT, name STRING) PARTITIONED BY (name)", ".partitioned.my_partitioned_table.046jcchm6krj4e1g60o30c0");

        RelationName relationName1 = new RelationName(Schemas.DOC_SCHEMA_NAME, "my_table");
        RelationName relationName2 = new RelationName(Schemas.DOC_SCHEMA_NAME, "my_partitioned_table");
        Metadata metadata = clusterService.state().metadata();
        String indexUUID1 = resolveIndex("doc.my_table", null, metadata).getUUID();
        String indexUUID2 = resolveIndex("doc.my_partitioned_table", PartitionName.decodeIdent("046jcchm6krj4e1g60o30c0"), null, metadata).getUUID();

        List<String> resolvedIndices = new ArrayList<>();
        List<TableOrPartition> tablesToRestore = List.of(
            new TableOrPartition(relationName1, null),
            new TableOrPartition(relationName2, "046jcchm6krj4e1g60o30c0")
        );
        SnapshotInfo snapshotInfo = new SnapshotInfo(
            new SnapshotId("snapshot1", UUIDs.randomBase64UUID()),
            List.of(),
            SnapshotState.SUCCESS
        );
        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(
            "repo1",
            snapshotInfo.snapshotId().getUUID(),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            Settings.EMPTY,
            TimeValue.timeValueSeconds(30),
            true,
            true,
            Strings.EMPTY_ARRAY,
            true,
            Strings.EMPTY_ARRAY
        );

        Map<RelationName, RestoreService.RestoreRelation> restoreRelations = resolveRelations(
            tablesToRestore,
            restoreRequest,
            metadata,
            snapshotInfo
        );

        RestoreService.RestoreRelation relation1 = restoreRelations.get(relationName1);
        assertThat(relation1).isNotNull();
        assertThat(relation1.restoreIndices()).containsExactly(new RestoreService.RestoreIndex(indexUUID1, List.of()));
        RestoreService.RestoreRelation relation2 = restoreRelations.get(relationName2);
        assertThat(relation2).isNotNull();
        assertThat(relation2.restoreIndices()).containsExactly(new RestoreService.RestoreIndex(indexUUID2, List.of("626572800000")));
    }
}
