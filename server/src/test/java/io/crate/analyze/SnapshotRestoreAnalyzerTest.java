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
import static io.crate.analyze.TableDefinitions.TEST_DOC_LOCATIONS_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS;
import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.ArrayMatching.arrayContainingInAnyOrder;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.Row;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.PartitionAlreadyExistsException;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.RepositoryUnknownException;
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

public class SnapshotRestoreAnalyzerTest extends CrateDummyClusterServiceUnitTest {

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
        e = SQLExecutor.builder(clusterService)
            .addTable(USER_TABLE_DEFINITION)
            .addTable(TEST_DOC_LOCATIONS_TABLE_DEFINITION)
            .addPartitionedTable(TEST_PARTITIONED_TABLE_DEFINITION, TEST_PARTITIONED_TABLE_PARTITIONS)
            .addBlobTable("create blob table my_blobs")
            .build();
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
                e.schemas());
        } else if (analyzedStatement instanceof AnalyzedRestoreSnapshot) {
            return (T) RestoreSnapshotPlan.bind(
                (AnalyzedRestoreSnapshot) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                Row.EMPTY,
                SubQueryResults.EMPTY,
                e.schemas());
        } else {
            return (T) e.analyze(statement);
        }
    }

    @Test
    public void testCreateSnapshotAll() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot ALL WITH (wait_for_completion=true)");
        assertThat(
            request.indices(),
            arrayContainingInAnyOrder(AnalyzedCreateSnapshot.ALL_INDICES.toArray()));
        assertThat(request.repository(), is("my_repo"));
        assertThat(request.snapshot(), is("my_snapshot"));
        assertThat(
            request.settings().getAsStructuredMap(),
            hasEntry("wait_for_completion", "true"));
    }

    @Test
    public void testCreateSnapshotUnknownRepo() throws Exception {
        expectedException.expect(RepositoryUnknownException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        analyze(e, "CREATE SNAPSHOT unknown_repo.my_snapshot ALL");
    }

    @Test
    public void testCreateSnapshotUnsupportedParameter() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'foo' not supported");
        analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot ALL with (foo=true)");
    }

    @Test
    public void testCreateSnapshotUnknownTables() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 't2' unknown");
        analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, t2, custom.users");
    }

    @Test
    public void testCreateSnapshotUnknownSchema() throws Exception {
        expectedException.expect(SchemaUnknownException.class);
        expectedException.expectMessage("Schema 'myschema' unknown");
        analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, myschema.users");
    }

    @Test
    public void testCreateSnapshotUnknownPartition() throws Exception {
        expectedException.expect(PartitionUnknownException.class);
        expectedException.expectMessage("No partition for table 'doc.parted' with ident '04130' exists");
        analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE parted PARTITION (date='1970-01-01')");
    }

    @Test
    public void testCreateSnapshotUnknownTableIgnore() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, t2 WITH (ignore_unavailable=true)");
        assertThat(request.indices(), arrayContaining("users"));
        assertThat(request.indicesOptions().ignoreUnavailable(), is(true));
    }

    @Test
    public void testCreateSnapshotUnknownSchemaIgnore() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, my_schema.t2 WITH (ignore_unavailable=true)");
        assertThat(request.indices(), arrayContaining("users"));
        assertThat(request.indicesOptions().ignoreUnavailable(), is(true));
    }

    @Test
    public void testCreateSnapshotCreateSnapshotTables() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, locations " +
            "WITH (wait_for_completion=true)");
        assertThat(request.includeGlobalState(), is(false));
        assertThat(request.indices(), arrayContainingInAnyOrder("users", "locations"));
        assertThat(request.repository(), is("my_repo"));
        assertThat(request.snapshot(), is("my_snapshot"));
        assertThat(
            request.settings().getAsStructuredMap(),
            hasEntry("wait_for_completion", "true"));
    }

    @Test
    public void testCreateSnapshotNoRepoName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Snapshot must be specified by \"<repository_name>\".\"<snapshot_name>\"");
        analyze(e, "CREATE SNAPSHOT my_snapshot TABLE users ");
    }

    @Test
    public void testCreateSnapshotInvalidRepoName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid repository name 'my.repo'");
        analyze(e, "CREATE SNAPSHOT my.repo.my_snapshot ALL");
    }

    @Test
    public void testCreateSnapshotSnapshotSysTable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow " +
                                        "CREATE SNAPSHOT operations, as it is read-only.");
        analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE sys.shards");
    }

    @Test
    public void testCreateSnapshotNoWildcards() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'foobar*' unknown");
        analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE \"foobar*\"");
    }

    @Test
    public void testCreateSnapshotFromBlobTable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"blob.my_blobs\" doesn't support or allow CREATE SNAPSHOT operations.");
        analyze(e, "CREATE SNAPSHOT my_repo.my_snapshot TABLE blob.my_blobs");
    }

    @Test
    public void testCreateSnapshotListTablesTwice() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE users, locations, users");
        assertThat(request.indices(), arrayContainingInAnyOrder("users", "locations"));
    }

    @Test
    public void testCreateSnapshotListPartitionsAndPartitionedTable() throws Exception {
        CreateSnapshotRequest request = analyze(
            e,
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE parted, parted PARTITION (date=1395961200000)");
        assertThat(
            request.indices(),
            arrayContainingInAnyOrder(
                ".partitioned.parted.04732cpp6ks3ed1o60o30c1g",
                ".partitioned.parted.0400",
                ".partitioned.parted.04732cpp6ksjcc9i60o30c1g")
        );
        assertThat(request.includeGlobalState(), is(false));
        assertThat(request.templates(), arrayContainingInAnyOrder(".partitioned.parted."));
    }

    @Test
    public void testDropSnapshot() throws Exception {
        AnalyzedDropSnapshot statement = analyze(e, "DROP SNAPSHOT my_repo.my_snap_1");
        assertThat(statement.repository(), is("my_repo"));
        assertThat(statement.snapshot(), is("my_snap_1"));
    }

    @Test
    public void testDropSnapshotUnknownRepo() throws Exception {
        expectedException.expect(RepositoryUnknownException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        analyze(e, "DROP SNAPSHOT unknown_repo.my_snap_1");
    }

    @Test
    public void testRestoreSnapshotAll() throws Exception {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot ALL");
        assertThat(statement.repository(), is("my_repo"));
        assertThat(statement.snapshot(), is("my_snapshot"));
        assertThat(statement.restoreTables().isEmpty(), is(true));
        assertThat(statement.includeTables(), is(true));
        assertThat(statement.includeCustomMetadata(), is(true));
        assertThat(statement.includeGlobalSettings(), is(true));
    }

    @Test
    public void testRestoreSnapshotSingleTable() throws Exception {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLE custom.restoreme");
        assertThat(statement.restoreTables().size(), is(1));
        var table = statement.restoreTables().iterator().next();
        assertThat(table.tableIdent(), is(new RelationName("custom", "restoreme")));
        assertThat(table.partitionName(), is(nullValue()));
        assertThat(statement.includeTables(), is(true));
        assertThat(statement.includeCustomMetadata(), is(false));
        assertThat(statement.includeGlobalSettings(), is(false));
    }

    @Test
    public void testRestoreExistingTable() throws Exception {
        expectedException.expect(RelationAlreadyExists.class);
        expectedException.expectMessage("Relation 'doc.users' already exists.");
        analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLE users");
    }

    @Test
    public void testRestoreUnsupportedParameter() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'foo' not supported");
        analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLE users WITH (foo=true)");
    }

    @Test
    public void testRestoreSinglePartition() throws Exception {
        BoundRestoreSnapshot statement = analyze(
            e,
            "RESTORE SNAPSHOT my_repo.my_snapshot TABLE parted PARTITION (date=123)");
        PartitionName partition = new PartitionName(
            new RelationName("doc", "parted"), List.of("123"));
        assertThat(statement.restoreTables().size(), is(1));
        var table = statement.restoreTables().iterator().next();
        assertThat(table.partitionName(), is(partition));
        assertThat(table.tableIdent(), is(new RelationName(Schemas.DOC_SCHEMA_NAME, "parted")));
        assertThat(statement.includeTables(), is(true));
        assertThat(statement.includeCustomMetadata(), is(false));
        assertThat(statement.includeGlobalSettings(), is(false));
    }

    @Test
    public void testRestoreSinglePartitionToUnknownTable() throws Exception {
        BoundRestoreSnapshot statement = analyze(
            e,
            "RESTORE SNAPSHOT my_repo.my_snapshot TABLE unknown_parted PARTITION (date=123)");
        PartitionName partitionName = new PartitionName(
            new RelationName("doc", "unknown_parted"), List.of("123"));
        assertThat(statement.restoreTables().size(), is(1));
        var table = statement.restoreTables().iterator().next();
        assertThat(table.partitionName(), is(partitionName));
        assertThat(table.tableIdent(), is(new RelationName(Schemas.DOC_SCHEMA_NAME, "unknown_parted")));
    }

    @Test
    public void testRestoreSingleExistingPartition() throws Exception {
        expectedException.expect(PartitionAlreadyExistsException.class);
        expectedException.expectMessage("Partition '.partitioned.parted.04732cpp6ksjcc9i60o30c1g' already exists");
        analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLE parted PARTITION (date=1395961200000)");
    }

    @Test
    public void testRestoreUnknownRepo() throws Exception {
        expectedException.expect(RepositoryUnknownException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        analyze(e, "RESTORE SNAPSHOT unknown_repo.my_snapshot ALL");
    }

    @Test
    public void test_restore_all_tables() {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLES");
        assertThat(statement.repository(), is("my_repo"));
        assertThat(statement.snapshot(), is("my_snapshot"));
        assertThat(statement.restoreTables().isEmpty(), is(true));
        assertThat(statement.includeTables(), is(true));
        assertThat(statement.includeCustomMetadata(), is(false));
        assertThat(statement.customMetadataTypes().isEmpty(), is(true));
        assertThat(statement.includeGlobalSettings(), is(false));
        assertThat(statement.globalSettings().isEmpty(), is(true));
    }

    @Test
    public void test_restore_all_metadata() {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot METADATA");
        assertThat(statement.repository(), is("my_repo"));
        assertThat(statement.snapshot(), is("my_snapshot"));
        assertThat(statement.restoreTables().isEmpty(), is(true));
        assertThat(statement.includeTables(), is(false));
        assertThat(statement.includeCustomMetadata(), is(true));
        assertThat(statement.customMetadataTypes().isEmpty(), is(true));
        assertThat(statement.includeGlobalSettings(), is(true));
        assertThat(statement.globalSettings(), contains(AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX));
    }

    @Test
    public void test_restore_analyzers() {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot ANALYZERS");
        assertThat(statement.repository(), is("my_repo"));
        assertThat(statement.snapshot(), is("my_snapshot"));
        assertThat(statement.restoreTables().isEmpty(), is(true));
        assertThat(statement.includeTables(), is(false));
        assertThat(statement.includeCustomMetadata(), is(false));
        assertThat(statement.customMetadataTypes().isEmpty(), is(true));
        assertThat(statement.includeGlobalSettings(), is(true));
        assertThat(statement.globalSettings(), contains(AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX));
    }

    @Test
    public void test_restore_custom_metadata() {
        for (var entry : METADATA_CUSTOM_TYPE_MAP.entrySet()) {
            BoundRestoreSnapshot statement =
                analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot " + entry.getKey());
            assertThat(statement.repository(), is("my_repo"));
            assertThat(statement.snapshot(), is("my_snapshot"));
            assertThat(statement.restoreTables().isEmpty(), is(true));
            assertThat(statement.includeTables(), is(false));
            assertThat(statement.includeCustomMetadata(), is(true));
            assertThat(statement.customMetadataTypes(), contains(entry.getValue()));
            assertThat(statement.includeGlobalSettings(), is(false));
            assertThat(statement.globalSettings().isEmpty(), is(true));
        }
    }

    @Test
    public void test_restore_multiple_metadata() {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot USERS, PRIVILEGES");
        assertThat(statement.repository(), is("my_repo"));
        assertThat(statement.snapshot(), is("my_snapshot"));
        assertThat(statement.includeTables(), is(false));
        assertThat(statement.includeCustomMetadata(), is(true));
        assertThat(statement.customMetadataTypes(),
                   containsInAnyOrder(
                       METADATA_CUSTOM_TYPE_MAP.get("USERS"),
                       METADATA_CUSTOM_TYPE_MAP.get("PRIVILEGES")
                   )
        );
        assertThat(statement.includeGlobalSettings(), is(false));
        assertThat(statement.globalSettings().isEmpty(), is(true));
    }

    @Test
    public void test_restore_tables_and_custom_metadata() {
        BoundRestoreSnapshot statement =
            analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot TABLES, VIEWS");
        assertThat(statement.repository(), is("my_repo"));
        assertThat(statement.snapshot(), is("my_snapshot"));
        assertThat(statement.includeTables(), is(true));
        assertThat(statement.includeCustomMetadata(), is(true));
        assertThat(statement.customMetadataTypes(), contains(METADATA_CUSTOM_TYPE_MAP.get("VIEWS")));
        assertThat(statement.includeGlobalSettings(), is(false));
        assertThat(statement.globalSettings().isEmpty(), is(true));
    }

    @Test
    public void test_restore_unknown_metadata() {
        Assertions.assertThatThrownBy(() -> analyze(e, "RESTORE SNAPSHOT my_repo.my_snapshot UNKNOWN_META"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown metadata type 'UNKNOWN_META'");
    }
}
