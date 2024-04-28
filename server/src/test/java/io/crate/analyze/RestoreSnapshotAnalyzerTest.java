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

import static io.crate.analyze.RestoreSnapshotAnalyzer.METADATA_CUSTOM_TYPE_MAP;
import static io.crate.analyze.RestoreSnapshotAnalyzer.USER_MANAGEMENT_METADATA;
import static io.crate.analyze.TableDefinitions.TEST_DOC_LOCATIONS_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS;
import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.Row;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.settings.AnalyzerSettings;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.CreateSnapshotPlan;
import io.crate.planner.node.ddl.RestoreSnapshotPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RestoreSnapshotAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        RepositoriesMetadata repositoriesMetadata = new RepositoriesMetadata(
            Collections.singletonList(
                new RepositoryMetadata(
                    "my_repo",
                    "fs",
                    Settings.builder().put("location", "/tmp/my_repo").build()
            )));
        ClusterState clusterState = ClusterState.builder(new ClusterName("testing"))
            .metadata(Metadata.builder()
                .putCustom(RepositoriesMetadata.TYPE, repositoriesMetadata))
            .build();
        ClusterServiceUtils.setState(clusterService, clusterState);
        e = SQLExecutor.of(clusterService)
            .addTable(USER_TABLE_DEFINITION)
            .addTable(TEST_DOC_LOCATIONS_TABLE_DEFINITION)
            .addPartitionedTable(TEST_PARTITIONED_TABLE_DEFINITION, TEST_PARTITIONED_TABLE_PARTITIONS)
            .addBlobTable("create blob table my_blobs");
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    @SuppressWarnings({"unchecked"})
    private <T> T analyze(SQLExecutor e, String statement) {
        AnalyzedStatement analyzedStatement = e.analyze(statement);
        if (analyzedStatement instanceof AnalyzedCreateSnapshot) {
            return (T) CreateSnapshotPlan.createRequest(
                (AnalyzedCreateSnapshot) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                Row.EMPTY,
                SubQueryResults.EMPTY,
                e.schemas(),
                clusterService.state().metadata());
        } else if (analyzedStatement instanceof AnalyzedRestoreSnapshot) {
            return (T) RestoreSnapshotPlan.bind(
                (AnalyzedRestoreSnapshot) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                Row.EMPTY,
                SubQueryResults.EMPTY,
                e.schemas());
        } else {
            return e.analyze(statement);
        }
    }

    @Test
    public void testCreateSnapshotAll() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot ALL WITH (wait_for_completion=true)");
        assertThat(request.indices()).usingRecursiveComparison().ignoringCollectionOrder()
            .isEqualTo(AnalyzedCreateSnapshot.ALL_INDICES.toArray(new String[0]));
        assertThat(request.repository()).isEqualTo("my_repo");
        assertThat(request.snapshot()).isEqualTo("my_snapshot");
        assertThat(request.settings().getAsStructuredMap()).containsExactly(
            Map.entry("wait_for_completion", "true"));
    }

    @Test
    public void testCreateSnapshotUnknownRepo() throws Exception {
        assertThatThrownBy(() -> analyze(e, "CREATE SNAPSHOT unknown_repo.my_snapshot ALL"))
            .isExactlyInstanceOf(RepositoryMissingException.class)
            .hasMessage("[unknown_repo] missing");
    }

    @Test
    public void testCreateSnapshotUnsupportedParameter() throws Exception {
        assertThatThrownBy(() -> analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot ALL with (foo=true)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'foo' is not supported");
    }

    @Test
    public void testCreateSnapshotUnknownTables() throws Exception {
        assertThatThrownBy(() -> analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, t2, custom.users"))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 't2' unknown");
    }

    @Test
    public void testCreateSnapshotUnknownSchema() throws Exception {
        assertThatThrownBy(() -> analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, myschema.users"))
            .isExactlyInstanceOf(SchemaUnknownException.class)
            .hasMessage("Schema 'myschema' unknown");
    }

    @Test
    public void testCreateSnapshotUnknownPartition() throws Exception {
        assertThatThrownBy(
            () -> analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE parted PARTITION (date='1970-01-01')"))
            .isExactlyInstanceOf(PartitionUnknownException.class)
            .hasMessage("No partition for table 'doc.parted' with ident '04130' exists");
    }

    @Test
    public void testCreateSnapshotUnknownTableIgnore() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, t2 WITH (ignore_unavailable=true)");
        assertThat(request.indices()).containsExactly("users");
        assertThat(request.indicesOptions().ignoreUnavailable()).isTrue();
    }

    @Test
    public void testCreateSnapshotUnknownSchemaIgnore() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, my_schema.t2 WITH (ignore_unavailable=true)");
        assertThat(request.indices()).containsExactly("users");
        assertThat(request.indicesOptions().ignoreUnavailable()).isTrue();
    }

    @Test
    public void testCreateSnapshotCreateSnapshotTables() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, locations " +
            "WITH (wait_for_completion=true)");
        assertThat(request.includeGlobalState()).isFalse();
        assertThat(request.indices()).containsExactlyInAnyOrder("users", "locations");
        assertThat(request.repository()).isEqualTo("my_repo");
        assertThat(request.snapshot()).isEqualTo("my_snapshot");
        assertThat(request.settings().getAsStructuredMap()).containsExactly(Map.entry("wait_for_completion", "true"));
    }

    @Test
    public void testCreateSnapshotNoRepoName() throws Exception {
        assertThatThrownBy(() -> analyze(e, "CREATE SNAPSHOT my_snapshot TABLE users "))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot must be specified by \"<repository_name>\".\"<snapshot_name>\"");
    }

    @Test
    public void testCreateSnapshotInvalidRepoName() throws Exception {
        assertThatThrownBy(() -> analyze(e, "CREATE SNAPSHOT my.repo.my_snapshot ALL"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid repository name 'my.repo'");
    }

    @Test
    public void testCreateSnapshotSnapshotSysTable() throws Exception {
        assertThatThrownBy(() -> analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE sys.shards"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"sys.shards\" doesn't support or allow " +
                        "CREATE SNAPSHOT operations");
    }

    @Test
    public void testCreateSnapshotNoWildcards() throws Exception {
        assertThatThrownBy(() -> analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE \"foobar*\""))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'foobar*' unknown");
    }

    @Test
    public void testCreateSnapshotFromBlobTable() throws Exception {
        assertThatThrownBy(() -> analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE blob.my_blobs"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"blob.my_blobs\" doesn't support or allow CREATE SNAPSHOT operations");
    }

    @Test
    public void testCreateSnapshotListTablesTwice() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, locations, users");
        assertThat(request.indices()).containsExactlyInAnyOrder("users", "locations");
    }

    @Test
    public void testCreateSnapshotListPartitionsAndPartitionedTable() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE parted, parted PARTITION (date=1395961200000)");
        assertThat(request.indices()).containsExactlyInAnyOrder(
                ".partitioned.parted.04732cpp6ks3ed1o60o30c1g",
                ".partitioned.parted.0400",
                ".partitioned.parted.04732cpp6ksjcc9i60o30c1g");
        assertThat(request.includeGlobalState()).isFalse();
        assertThat(request.templates()).containsExactly(".partitioned.parted.");
    }

    @Test
    public void testDropSnapshot() throws Exception {
        AnalyzedDropSnapshot statement = analyze(e, "DROP SNAPSHOT my_repo.my_snap_1");
        assertThat(statement.repository()).isEqualTo("my_repo");
        assertThat(statement.snapshot()).isEqualTo("my_snap_1");
    }

    @Test
    public void testDropSnapshotUnknownRepo() throws Exception {
        assertThatThrownBy(() -> analyze(e, "DROP SNAPSHOT unknown_repo.my_snap_1"))
            .isExactlyInstanceOf(RepositoryMissingException.class)
            .hasMessage("[unknown_repo] missing");
    }

    @Test
    public void testRestoreSnapshotAll() throws Exception {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot ALL");
        assertThat(statement.repository()).isEqualTo("my_repo");
        assertThat(statement.snapshot()).isEqualTo("my_snapshot");
        assertThat(statement.restoreTables().isEmpty()).isTrue();
        assertThat(statement.includeTables()).isTrue();
        assertThat(statement.includeCustomMetadata()).isTrue();
        assertThat(statement.includeGlobalSettings()).isTrue();
    }

    @Test
    public void testRestoreSnapshotSingleTable() throws Exception {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLE custom.restoreme");
        assertThat(statement.restoreTables()).hasSize(1);
        var table = statement.restoreTables().iterator().next();
        assertThat(table.tableIdent()).isEqualTo(new RelationName("custom", "restoreme"));
        assertThat(table.partitionName()).isNull();
        assertThat(statement.includeTables()).isTrue();
        assertThat(statement.includeCustomMetadata()).isFalse();
        assertThat(statement.includeGlobalSettings()).isFalse();
    }

    @Test
    public void testRestoreUnsupportedParameter() throws Exception {
        assertThatThrownBy(() -> analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLE users WITH (foo=true)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'foo' is not supported");
    }

    @Test
    public void testRestoreSinglePartition() throws Exception {
        BoundRestoreSnapshot statement = analyze(
            e,
            "RESTORE SNAPSHOT my_repo.my_snapshot TABLE parted PARTITION (date=123)");
        PartitionName partition = new PartitionName(
            new RelationName("doc", "parted"), List.of("123"));
        assertThat(statement.restoreTables()).hasSize(1);
        var table = statement.restoreTables().iterator().next();
        assertThat(table.partitionName()).isEqualTo(partition);
        assertThat(table.tableIdent()).isEqualTo(new RelationName(Schemas.DOC_SCHEMA_NAME, "parted"));
        assertThat(statement.includeTables()).isTrue();
        assertThat(statement.includeCustomMetadata()).isFalse();
        assertThat(statement.includeGlobalSettings()).isFalse();
    }

    @Test
    public void testRestoreSinglePartitionToUnknownTable() throws Exception {
        BoundRestoreSnapshot statement = analyze(
            e,
            "RESTORE SNAPSHOT my_repo.my_snapshot TABLE unknown_parted PARTITION (date=123)");
        PartitionName partitionName = new PartitionName(
            new RelationName("doc", "unknown_parted"), List.of("123"));
        assertThat(statement.restoreTables()).hasSize(1);
        var table = statement.restoreTables().iterator().next();
        assertThat(table.partitionName()).isEqualTo(partitionName);
        assertThat(table.tableIdent()).isEqualTo(new RelationName(Schemas.DOC_SCHEMA_NAME, "unknown_parted"));
    }

    @Test
    public void testRestoreUnknownRepo() throws Exception {
        assertThatThrownBy(() -> analyze(e, "RESTORE SNAPSHOT unknown_repo.my_snapshot ALL"))
            .isExactlyInstanceOf(RepositoryMissingException.class)
            .hasMessage("[unknown_repo] missing");
    }

    @Test
    public void test_restore_all_tables() {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLES");
        assertThat(statement.repository()).isEqualTo("my_repo");
        assertThat(statement.snapshot()).isEqualTo("my_snapshot");
        assertThat(statement.restoreTables().isEmpty()).isTrue();
        assertThat(statement.includeTables()).isTrue();
        assertThat(statement.includeCustomMetadata()).isFalse();
        assertThat(statement.customMetadataTypes().isEmpty()).isTrue();
        assertThat(statement.includeGlobalSettings()).isFalse();
        assertThat(statement.globalSettings().isEmpty()).isTrue();
    }

    @Test
    public void test_restore_all_metadata() {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot METADATA");
        assertThat(statement.repository()).isEqualTo("my_repo");
        assertThat(statement.snapshot()).isEqualTo("my_snapshot");
        assertThat(statement.restoreTables().isEmpty()).isTrue();
        assertThat(statement.includeTables()).isFalse();
        assertThat(statement.includeCustomMetadata()).isTrue();
        assertThat(statement.customMetadataTypes().isEmpty()).isTrue();
        assertThat(statement.includeGlobalSettings()).isTrue();
        assertThat(statement.globalSettings()).containsExactly(AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX);
    }

    @Test
    public void test_restore_analyzers() {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot ANALYZERS");
        assertThat(statement.repository()).isEqualTo("my_repo");
        assertThat(statement.snapshot()).isEqualTo("my_snapshot");
        assertThat(statement.restoreTables().isEmpty()).isTrue();
        assertThat(statement.includeTables()).isFalse();
        assertThat(statement.includeCustomMetadata()).isFalse();
        assertThat(statement.customMetadataTypes().isEmpty()).isTrue();
        assertThat(statement.includeGlobalSettings()).isTrue();
        assertThat(statement.globalSettings()).containsExactly(AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX);
    }

    @Test
    public void test_restore_custom_metadata() {
        for (var meta : List.of("VIEWS", "UDFS")) {
            BoundRestoreSnapshot statement =
                analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot " + meta);
            assertThat(statement.repository()).isEqualTo("my_repo");
            assertThat(statement.snapshot()).isEqualTo("my_snapshot");
            assertThat(statement.restoreTables().isEmpty()).isTrue();
            assertThat(statement.includeTables()).isFalse();
            assertThat(statement.includeCustomMetadata()).isTrue();
            assertThat(statement.customMetadataTypes())
                .containsExactlyInAnyOrderElementsOf(METADATA_CUSTOM_TYPE_MAP.get(meta));
            assertThat(statement.includeGlobalSettings()).isFalse();
            assertThat(statement.globalSettings().isEmpty()).isTrue();
        }
    }

    @Test
    public void test_restore_user_mananagement_metadata() {
        for (var meta : List.of("USERS", "PRIVILEGES", "USERMANAGEMENT")) {
            RestoreSnapshotAnalyzer.DEPRECATION_LOGGER.resetLRU();

            BoundRestoreSnapshot statement =
                analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot " + meta);
            assertThat(statement.repository()).isEqualTo("my_repo");
            assertThat(statement.snapshot()).isEqualTo("my_snapshot");
            assertThat(statement.includeTables()).isFalse();
            assertThat(statement.restoreTables().isEmpty()).isTrue();
            assertThat(statement.includeCustomMetadata()).isTrue();
            assertThat(statement.customMetadataTypes()).containsExactlyInAnyOrderElementsOf(USER_MANAGEMENT_METADATA);
            assertThat(statement.includeGlobalSettings()).isFalse();
            assertThat(statement.globalSettings().isEmpty()).isTrue();

            if ("USERS".equals(meta)) {
                assertWarnings("USERS keyword is deprecated, please use USERMANAGEMENT instead");
            }
            if ("PRIVILEGES".equals(meta)) {
                assertWarnings("PRIVILEGES keyword is deprecated, please use USERMANAGEMENT instead");
            }
        }
    }

    @Test
    public void test_restore_tables_and_views() {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLES, VIEWS");
        assertThat(statement.repository()).isEqualTo("my_repo");
        assertThat(statement.snapshot()).isEqualTo("my_snapshot");
        assertThat(statement.includeTables()).isTrue();
        assertThat(statement.includeCustomMetadata()).isTrue();
        assertThat(statement.customMetadataTypes()).containsExactlyElementsOf(METADATA_CUSTOM_TYPE_MAP.get("VIEWS"));
        assertThat(statement.includeGlobalSettings()).isFalse();
        assertThat(statement.globalSettings().isEmpty()).isTrue();
    }

    @Test
    public void test_restore_unknown_metadata() {
        Assertions.assertThatThrownBy(() -> analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot UNKNOWN_META"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown metadata type 'UNKNOWN_META'");
    }
}
