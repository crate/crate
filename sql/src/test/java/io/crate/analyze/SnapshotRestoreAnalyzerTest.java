/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import io.crate.exceptions.*;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import static io.crate.analyze.TableDefinitions.*;
import static org.hamcrest.Matchers.*;

public class SnapshotRestoreAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() {
        RepositoriesMetaData repositoriesMetaData = new RepositoriesMetaData(
            new RepositoryMetaData(
                "my_repo",
                "fs",
                Settings.builder().put("location", "/tmp/my_repo").build()
            ));
        ClusterState clusterState = ClusterState.builder(new ClusterName("testing"))
            .metaData(MetaData.builder()
                .putCustom(RepositoriesMetaData.TYPE, repositoriesMetaData))
            .build();
        ClusterServiceUtils.setState(clusterService, clusterState);
        TableIdent myBlobsIdent = new TableIdent(BlobSchemaInfo.NAME, "my_blobs");
        TestingBlobTableInfo myBlobsTableInfo = TableDefinitions.createBlobTable(myBlobsIdent, clusterService);
        executor = SQLExecutor.builder(clusterService)
            .addDocTable(USER_TABLE_INFO)
            .addDocTable(TEST_DOC_LOCATIONS_TABLE_INFO)
            .addDocTable(TEST_PARTITIONED_TABLE_INFO)
            .addBlobTable(myBlobsTableInfo)
            .build();
    }

    private <T> T analyze(String statement) {
        return executor.analyze(statement);
    }

    @Test
    public void testCreateSnapshotAll() throws Exception {
        CreateSnapshotAnalyzedStatement statement = analyze("CREATE SNAPSHOT my_repo.my_snapshot ALL WITH (wait_for_completion=true)");
        assertThat(statement.indices(), is(CreateSnapshotAnalyzedStatement.ALL_INDICES));
        assertThat(statement.snapshot().getRepository(), is("my_repo"));
        assertThat(statement.snapshot().getSnapshotId().getName(), is("my_snapshot"));
        assertThat(statement.snapshotSettings().getAsMap(),
            allOf(
                hasEntry("wait_for_completion", "true"),
                hasEntry("ignore_unavailable", "false")
            ));
    }

    @Test
    public void testCreateSnapshotUnknownRepo() throws Exception {
        expectedException.expect(RepositoryUnknownException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        analyze("CREATE SNAPSHOT unknown_repo.my_snapshot ALL");
    }

    @Test
    public void testCreateSnapshotUnsupportedParameter() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'foo' not supported");
        analyze("CREATE SNAPSHOT my_repo.my_snapshot ALL with (foo=true)");
    }

    @Test
    public void testCreateSnapshotUnknownTables() throws Exception {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("Table 'doc.t2' unknown");
        analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE users, t2, custom.users");
    }

    @Test
    public void testCreateSnapshotUnknownSchema() throws Exception {
        expectedException.expect(SchemaUnknownException.class);
        expectedException.expectMessage("Schema 'myschema' unknown");
        analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE users, myschema.users");
    }

    @Test
    public void testCreateSnapshotUnknownPartition() throws Exception {
        expectedException.expect(PartitionUnknownException.class);
        expectedException.expectMessage("No partition for table 'doc.parted' with ident '04130' exists");
        analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE parted PARTITION (date='1970-01-01')");
    }

    @Test
    public void testCreateSnapshotUnknownTableIgnore() throws Exception {
        CreateSnapshotAnalyzedStatement statement = analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE users, t2 WITH (ignore_unavailable=true)");
        assertThat(statement.indices(), contains("users"));
        assertThat(statement.snapshotSettings().getAsBoolean(SnapshotSettings.IGNORE_UNAVAILABLE.name(), false), is(true));
    }

    @Test
    public void testCreateSnapshotUnknownSchemaIgnore() throws Exception {
        CreateSnapshotAnalyzedStatement statement = analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE users, my_schema.t2 WITH (ignore_unavailable=true)");
        assertThat(statement.indices(), contains("users"));
        assertThat(statement.snapshotSettings().getAsBoolean(SnapshotSettings.IGNORE_UNAVAILABLE.name(), false), is(true));
    }

    @Test
    public void testCreateSnapshotCreateSnapshotTables() throws Exception {
        CreateSnapshotAnalyzedStatement statement = analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE users, locations WITH (wait_for_completion=true)");
        assertThat(statement.indices(), containsInAnyOrder("users", "locations"));
        assertThat(statement.snapshot().getRepository(), is("my_repo"));
        assertThat(statement.snapshot().getSnapshotId().getName(), is("my_snapshot"));
        assertThat(statement.snapshotSettings().getAsMap().size(), is(2));
        assertThat(statement.snapshotSettings().getAsMap(),
            allOf(
                hasEntry("wait_for_completion", "true"),
                hasEntry("ignore_unavailable", "false")
            ));
    }

    @Test
    public void testCreateSnapshotNoRepoName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Snapshot must be specified by \"<repository_name>\".\"<snapshot_name>\"");
        analyze("CREATE SNAPSHOT my_snapshot TABLE users ");
    }

    @Test
    public void testCreateSnapshotInvalidRepoName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid repository name 'my.repo'");
        analyze("CREATE SNAPSHOT my.repo.my_snapshot ALL");
    }

    @Test
    public void testCreateSnapshotSnapshotSysTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow " +
                                        "CREATE SNAPSHOT operations, as it is read-only.");
        analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE sys.shards");
    }

    @Test
    public void testCreateSnapshotNoWildcards() throws Exception {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("Table 'doc.user*' unknown");
        analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE \"user*\"");
    }

    @Test
    public void testCreateSnapshotFromBlobTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("The relation \"blob.my_blobs\" doesn't support or allow CREATE SNAPSHOT operations.");
        analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE blob.my_blobs");
    }

    @Test
    public void testCreateSnapshotListTablesTwice() throws Exception {
        CreateSnapshotAnalyzedStatement statement = analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE users, locations, users");
        assertThat(statement.indices(), hasSize(2));
        assertThat(statement.indices(), containsInAnyOrder("users", "locations"));
    }

    @Test
    public void testCreateSnapshotListPartitionsAndPartitionedTable() throws Exception {
        CreateSnapshotAnalyzedStatement statement = analyze("CREATE SNAPSHOT my_repo.my_snapshot TABLE parted, parted PARTITION (date=1395961200000)");
        assertThat(statement.indices(), hasSize(3));
        assertThat(statement.indices(), containsInAnyOrder(".partitioned.parted.04732cpp6ks3ed1o60o30c1g", ".partitioned.parted.0400", ".partitioned.parted.04732cpp6ksjcc9i60o30c1g"));
    }

    @Test
    public void testDropSnapshot() throws Exception {
        DropSnapshotAnalyzedStatement statement = analyze("drop snapshot my_repo.my_snap_1");
        assertThat(statement.repository(), is("my_repo"));
        assertThat(statement.snapshot(), is("my_snap_1"));
    }

    @Test
    public void testDropSnapshotUnknownRepo() throws Exception {
        expectedException.expect(RepositoryUnknownException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        analyze("drop snapshot unknown_repo.my_snap_1");
    }


    @Test
    public void testRestoreSnapshotAll() throws Exception {
        RestoreSnapshotAnalyzedStatement statement = analyze("RESTORE SNAPSHOT my_repo.my_snapshot ALL");
        assertThat(statement.snapshotName(), is("my_snapshot"));
        assertThat(statement.repositoryName(), is("my_repo"));
        assertThat(statement.restoreAll(), is(true));
        assertThat(statement.restoreAll(), is(true));
        assertThat(statement.settings().getAsMap(), // default settings
            allOf(
                hasEntry("wait_for_completion", "false"),
                hasEntry("ignore_unavailable", "false")
            ));
    }

    @Test
    public void testRestoreSnapshotSingleTable() throws Exception {
        RestoreSnapshotAnalyzedStatement statement = analyze(
            "RESTORE SNAPSHOT my_repo.my_snapshot TABLE custom.restoreme");
        assertThat(statement.restoreTables().get(0).tableIdent(), is(new TableIdent("custom", "restoreme")));
        assertThat(statement.restoreTables().get(0).partitionName(), is(nullValue()));
        assertThat(statement.settings().getAsMap(),
            allOf(
                hasEntry("wait_for_completion", "false"),
                hasEntry("ignore_unavailable", "false")
            ));
    }

    @Test
    public void testRestoreExistingTable() throws Exception {
        expectedException.expect(TableAlreadyExistsException.class);
        expectedException.expectMessage("The table 'doc.users' already exists.");
        analyze("RESTORE SNAPSHOT my_repo.my_snapshot TABLE users");
    }

    @Test
    public void testRestoreUnsupportedParameter() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'foo' not supported");
        analyze("RESTORE SNAPSHOT my_repo.my_snapshot TABLE users WITH (foo=true)");
    }

    @Test
    public void testRestoreSinglePartition() throws Exception {
        RestoreSnapshotAnalyzedStatement statement = analyze(
            "RESTORE SNAPSHOT my_repo.my_snapshot TABLE parted PARTITION (date=123)");
        PartitionName partition = new PartitionName("parted", ImmutableList.of(new BytesRef("123")));
        assertThat(statement.restoreTables().size(), is(1));
        assertThat(statement.restoreTables().get(0).partitionName(), is(partition));
        assertThat(statement.restoreTables().get(0).tableIdent(), is(new TableIdent(Schemas.DOC_SCHEMA_NAME, "parted")));
    }

    @Test
    public void testRestoreSinglePartitionToUnknownTable() throws Exception {
        RestoreSnapshotAnalyzedStatement statement = analyze(
            "RESTORE SNAPSHOT my_repo.my_snapshot TABLE unknown_parted PARTITION (date=123)");
        PartitionName partitionName = new PartitionName("unknown_parted", ImmutableList.of(new BytesRef("123")));
        assertThat(statement.restoreTables().size(), is(1));
        assertThat(statement.restoreTables().get(0).partitionName(), is(partitionName));
        assertThat(statement.restoreTables().get(0).tableIdent(), is(new TableIdent(Schemas.DOC_SCHEMA_NAME, "unknown_parted")));
    }

    @Test
    public void testRestoreSingleExistingPartition() throws Exception {
        expectedException.expect(PartitionAlreadyExistsException.class);
        expectedException.expectMessage("Partition '.partitioned.parted.04732cpp6ksjcc9i60o30c1g' already exists");
        analyze("RESTORE SNAPSHOT my_repo.my_snapshot TABLE parted PARTITION (date=1395961200000)");
    }

    @Test
    public void testRestoreUnknownRepo() throws Exception {
        expectedException.expect(RepositoryUnknownException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        analyze("RESTORE SNAPSHOT unknown_repo.my_snapshot ALL");
    }
}
