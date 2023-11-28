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

package io.crate.integrationtests;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.SetOnce;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.ESBlobStoreTestCase;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.MockKeywordPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.common.unit.TimeValue;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.testing.Asserts;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseRandomizedSchema;
import io.crate.user.metadata.RolesMetadata;
import io.crate.user.metadata.UsersMetadata;

public class SnapshotRestoreIntegrationTest extends IntegTestCase {

    private static final String REPOSITORY_NAME = "my_repo";
    private static final String SNAPSHOT_NAME = "my_snapshot";

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private File defaultRepositoryLocation;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMPORARY_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockKeywordPlugin.class);
        return plugins;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        defaultRepositoryLocation = TEMPORARY_FOLDER.newFolder();
        execute("CREATE REPOSITORY " + REPOSITORY_NAME + " TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{defaultRepositoryLocation.getAbsolutePath()});
        assertThat(response.rowCount()).isEqualTo(1L);
        execute(
            "CREATE REPOSITORY my_repo_ro TYPE \"fs\" with (location=?, compress=true, readonly=true)",
            new Object[]{defaultRepositoryLocation.getAbsolutePath()}
        );

        var dummyLang = new UserDefinedFunctionsIntegrationTest.DummyLang();
        Iterable<UserDefinedFunctionService> udfServices = cluster().getInstances(UserDefinedFunctionService.class);
        for (UserDefinedFunctionService udfService : udfServices) {
            udfService.registerLanguage(dummyLang);
        }
    }

    @After
    public void cleanUp() {
        var stmts = List.of(
            "REVOKE ALL FROM my_user",
            "DROP ANALYZER a1",
            "DROP FUNCTION custom(string)"
        );
        for (var stmt : stmts) {
            try {
                execute(stmt);
            } catch (Exception e) {
                // pass, exception may raise cause entity does not exist
            }
        }

        execute("DROP USER IF EXISTS my_user");
        execute("DROP VIEW IF EXISTS my_view");
        execute("DROP TABLE IF EXISTS my_table");
    }

    private void createTableAndSnapshot(String tableName, String snapshotName) {
        createTableAndSnapshot(tableName, snapshotName, false);
    }

    private void createTableAndSnapshot(String tableName, String snapshotName, boolean partitioned) {
        createTable(tableName, partitioned);
        createSnapshot(snapshotName, tableName);
    }

    private void createTable(String tableName, boolean partitioned) {
        createTable(tableName, partitioned, true);
    }

    private void createTable(String tableName, boolean partitioned, boolean withData) {
        execute("CREATE TABLE " + tableName + " (" +
                "  id long primary key, " +
                "  name string, " +
                "  date timestamp with time zone " + (partitioned ? "primary key," : ",") +
                "  ft string index using fulltext with (analyzer='default')" +
                ") " + (partitioned ? "partitioned by (date) " : "") +
                "clustered into 1 shards with (number_of_replicas=0)");
        if (withData) {
            execute("INSERT INTO " + tableName + " (id, name, date, ft) VALUES (?, ?, ?, ?)", new Object[][]{
                {1L, "foo", "1970-01-01", "The quick brown fox jumps over the lazy dog."},
                {2L, "bar", "2015-10-27T11:29:00+01:00", "Morgenstund hat Gold im Mund."},
                {3L, "baz", "1989-11-09", "Reden ist Schweigen. Silber ist Gold."},
            });
            execute("REFRESH TABLE " + tableName);
        }
    }

    private void createSnapshot(String snapshotName, String... tables) {
        execute("CREATE SNAPSHOT " + REPOSITORY_NAME + "." + snapshotName + " TABLE " + String.join(", ", tables) +
                " WITH (wait_for_completion=true)");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    private static String snapshotName() {
        return String.format(Locale.ENGLISH, "%s.%s", REPOSITORY_NAME, SNAPSHOT_NAME);
    }

    @Test
    public void testDropSnapshot() throws Exception {
        String snapshotName = "my_snap_1";
        createTableAndSnapshot("my_table", snapshotName);

        execute("drop snapshot " + REPOSITORY_NAME + "." + snapshotName);
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("select * from sys.snapshots where name = ?", new Object[]{snapshotName});
        assertThat(response.rowCount()).isEqualTo(0L);
        assertAllRepoSnapshotFilesAreDeleted(defaultRepositoryLocation);
    }

    @Test
    public void testDropUnknownSnapshot() throws Exception {
        String snapshot = "unknown_snap";
        Asserts.assertSQLError(() -> execute("drop snapshot " + REPOSITORY_NAME + "." + snapshot))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(NOT_FOUND, 4048)
            .hasMessageContaining(String.format(Locale.ENGLISH, "[%s:%s] is missing", REPOSITORY_NAME, snapshot));
    }

    @Test
    public void testDropSnapshotUnknownRepository() throws Exception {
        String repository = "unknown_repo";
        String snapshot = "unknown_snap";
        Asserts.assertSQLError(() -> execute("drop snapshot " + repository + "." + snapshot))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(NOT_FOUND, 4047)
            .hasMessageContaining(String.format(Locale.ENGLISH, "[%s] missing", repository));
    }

    @Test
    public void testCreateSnapshot() throws Exception {
        createTable("backmeup", false);
        execute("CREATE SNAPSHOT " + snapshotName() + " TABLE backmeup WITH (wait_for_completion=true)");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("select name, \"repository\", concrete_indices, state from sys.snapshots order by 2");
        assertThat(response).hasRows(
            String.format("my_snapshot| my_repo| [%s.backmeup]| SUCCESS", sqlExecutor.getCurrentSchema()),
            // shows up twice because both repos have the same data path
            String.format("my_snapshot| my_repo_ro| [%s.backmeup]| SUCCESS", sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void testCreateSnapshotWithoutWaitForCompletion() throws Exception {
        // this test just verifies that no exception is thrown if wait_for_completion is false
        execute("CREATE SNAPSHOT my_repo.snapshot_no_wait ALL WITH (wait_for_completion=false)");
        assertThat(response.rowCount()).isEqualTo(1L);
        waitForCompletion(REPOSITORY_NAME, "snapshot_no_wait", TimeValue.timeValueSeconds(20));
    }

    private void waitForCompletion(String repository, String snapshotName, TimeValue timeout) throws Exception {
        long start = System.currentTimeMillis();
        Snapshot snapshot = new Snapshot(repository, new SnapshotId(repository, snapshotName));
        while (System.currentTimeMillis() - start < timeout.millis()) {
            var response = execute(
                "select state from sys.snapshots where repository = ? and name = ?",
                new Object[] { repository, snapshotName }
            );
            if (response.rowCount() > 0 && response.rows()[0][0] == "SUCCESS") {
                // Make sure that snapshot clean up operations are finished
                ClusterStateResponse stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get();
                SnapshotsInProgress snapshotsInProgress = stateResponse.getState().custom(SnapshotsInProgress.TYPE);
                if (snapshotsInProgress == null || snapshotsInProgress.snapshot(snapshot) == null) {
                    return;
                }
            }
            Thread.sleep(100);
        }
        Asserts.fail("Timeout waiting for snapshot completion!");
    }

    @Test
    public void testCreateSnapshotFromPartition() throws Exception {
        createTable("custom.backmeup", true);

        execute("CREATE SNAPSHOT " + snapshotName() +
                " TABLE custom.backmeup PARTITION (date='1970-01-01')  WITH (wait_for_completion=true)");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("select name, \"repository\", concrete_indices, tables, state from sys.snapshots order by 2");
        assertThat(response).hasRows(
            "my_snapshot| my_repo| [custom..partitioned.backmeup.04130]| [custom.backmeup]| SUCCESS",
            // shows up twice because the repos have the same fs path.
            "my_snapshot| my_repo_ro| [custom..partitioned.backmeup.04130]| [custom.backmeup]| SUCCESS");
    }

    @Test
    public void testCreateSnapshotAllBlobsExcluded() throws Exception {
        execute("CREATE TABLE t1 (id INTEGER, name STRING)");
        execute("CREATE BLOB TABLE b1");
        ensureYellow();
        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("select concrete_indices from sys.snapshots");
        assertThat(response.rows()[0][0]).isEqualTo(List.of(getFqn("t1")));
    }

    @Test
    public void testCreateExistingSnapshot() throws Exception {
        createTable("backmeup", randomBoolean());

        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        assertThat(response.rowCount()).isEqualTo(1L);
        Asserts.assertSQLError(() -> execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(CONFLICT, 4096)
            .hasMessageContaining("Invalid snapshot name [my_snapshot], snapshot with the same name already exists");
    }

    @Test
    public void testCreateSnapshotUnknownRepo() throws Exception {
        Asserts.assertSQLError(() -> execute("CREATE SNAPSHOT unknown_repo.my_snapshot ALL WITH (wait_for_completion=true)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(NOT_FOUND, 4047)
            .hasMessageContaining("[unknown_repo] missing");
    }

    @Test
    public void testInvalidSnapshotName() throws Exception {
        Asserts.assertSQLError(() -> execute("CREATE SNAPSHOT my_repo.\"MY_UPPER_SNAPSHOT\" ALL WITH (wait_for_completion=true)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(CONFLICT, 4096)
            .hasMessageContaining("Invalid snapshot name [MY_UPPER_SNAPSHOT], must be lowercase");
    }

    @Test
    public void testSnapshotWithMetadataDoesNotDeleteExistingStuff() throws Exception {
        createTable("my_other", true);
        execute("CREATE SNAPSHOT " + snapshotName() + " TABLE my_other with (wait_for_completion=true)");

        execute("alter table my_other add column x double");
        execute("delete from my_other");

        execute("CREATE TABLE survivor (bla string, blubb float) partitioned by (blubb) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into survivor (bla, blubb) values (?, ?)", new Object[][]{
            {"foo", 1.2},
            {"bar", 1.4},
            {"baz", 1.2}
        });
        execute("refresh table survivor");

        execute("restore snapshot " + snapshotName() + " ALL with (wait_for_completion=true)");

        execute("select * from survivor order by bla");
        assertThat(response).hasRows(
            "bar| 1.4",
            "baz| 1.2",
            "foo| 1.2");
    }

    @Test
    public void testSnapshotWithMetadataConcurrentlyModified() throws Exception {
        int shards = randomFrom(1, 3, 5);
        int replicas = randomIntBetween(2, 10);
        long documents = randomLongBetween(2, 100);

        execute("CREATE TABLE test (" +
                "  id long primary key)" +
                "clustered into " + shards + " shards with (column_policy = 'dynamic', number_of_replicas=" + replicas +
                ")");

        ensureYellow();

        CompletableFuture<SQLResponse> createSnapshot = null;
        // Insert data with dynamic column creation so we trigger a dynamic mapping update for each of them
        for (var i = 0; i < documents; i++) {
            execute("INSERT INTO test (id, field_" + i + ") VALUES (?, ?)", new Object[][]{{i, "value_" + i},});
            execute("REFRESH TABLE test");
            if (createSnapshot == null) {
                createSnapshot = sqlExecutor.execute(
                    "CREATE SNAPSHOT " + snapshotName() + " TABLE test with (wait_for_completion=true)", null);
            }
        }

        if (createSnapshot != null) {
            createSnapshot.get();
        }

        execute("DROP table test");

        execute("select state from sys.snapshots where name=?", new Object[]{SNAPSHOT_NAME});
        assertThat(response.rows()[0][0]).isEqualTo("SUCCESS");

        execute("RESTORE SNAPSHOT " + snapshotName() + " ALL with (wait_for_completion=true)");

        waitNoPendingTasksOnAll();

        SnapshotsInProgress finalSnapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
        assertThat(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false))
            .isFalse();

        ImmutableOpenMap<String, IndexMetadata> state = clusterService().state().metadata().indices();
        IndexMetadata indexMetadata = state.values().iterator().next().value;
        int sizeOfProperties = ((Map<?, ?>) indexMetadata.mapping().sourceAsMap().get("properties")).size();

        execute("select count(*) from test");

        assertThat((Long)response.rows()[0][0])
            .as("Documents were restored but the restored index mapping was older than some " +
                "documents and misses some of their fields")
            .isLessThanOrEqualTo(sizeOfProperties);
    }

    @Test
    public void testRestoreSnapshotAll() throws Exception {
        createTableAndSnapshot("my_table", SNAPSHOT_NAME);

        execute("drop table my_table");

        execute("RESTORE SNAPSHOT " + snapshotName() + " ALL with (" +
                "ignore_unavailable=false, " +
                "wait_for_completion=true)");
        ensureGreen();
        execute("select * from my_table order by id");
        assertThat(response.rowCount()).isEqualTo(3L);
    }

    @Test
    public void testRestoreSnapshotSinglePartition() throws Exception {
        createTableAndSnapshot("my_parted_table", SNAPSHOT_NAME, true);
        waitNoPendingTasksOnAll();

        execute("delete from my_parted_table");
        waitNoPendingTasksOnAll();
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_parted_table PARTITION (date='1970-01-01') with (" +
                "ignore_unavailable=false, " +
                "wait_for_completion=true)");

        execute("select date from my_parted_table");
        assertThat(response).hasRows("0");
    }

    @Test
    public void testRestoreSinglePartitionSnapshotIntoDroppedPartition() throws Exception {
        createTable("parted_table", true);
        execute("CREATE SNAPSHOT " + snapshotName() +
                " TABLE parted_table PARTITION (date=0) WITH (wait_for_completion=true)");
        execute("delete from parted_table where date=0");
        waitNoPendingTasksOnAll();
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE parted_table PARTITION (date=0) with (" +
                "ignore_unavailable=false, " +
                "wait_for_completion=true)");
        execute("select date from parted_table order by id");
        assertThat(response).hasRows("0",
                                     "1445941740000",
                                     "626572800000");
    }

    @Test
    public void testRestoreSinglePartitionSnapshotIntoDroppedTable() throws Exception {
        createTable("parted_table", true);
        execute("CREATE SNAPSHOT " + snapshotName() +
                " TABLE parted_table PARTITION (date=0) WITH (wait_for_completion=true)");
        execute("drop table parted_table");
        waitNoPendingTasksOnAll();
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE parted_table PARTITION (date=0) with (" +
                "ignore_unavailable=false, " +
                "wait_for_completion=true)");
        execute("select date from parted_table order by id");
        assertThat(response).hasRows("0");
    }

    @Test
    public void testRestoreFullPartedTableSnapshotSinglePartitionIntoDroppedTable() throws Exception {
        createTableAndSnapshot("my_parted_table", SNAPSHOT_NAME, true);

        execute("drop table my_parted_table");
        waitNoPendingTasksOnAll();
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_parted_table PARTITION (date=0) with (" +
                "ignore_unavailable=false, " +
                "wait_for_completion=true)");

        execute("select date from my_parted_table");
        assertThat(response).hasRows("0");
    }

    @Test
    public void testRestoreSnapshotIgnoreUnavailable() throws Exception {
        createTableAndSnapshot("my_table", SNAPSHOT_NAME, true);

        execute("drop table my_table");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_table, not_my_table with (" +
                "ignore_unavailable=true, " +
                "wait_for_completion=true)");
        execute("select table_schema || '.' || table_name from information_schema.tables where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response).hasRows(getFqn("my_table"));
    }

    @Test
    public void testRestoreOnlyOneTable() throws Exception {
        createTable("my_table_1", false);
        createTable("my_table_2", false);
        createSnapshot(SNAPSHOT_NAME, "my_table_1", "my_table_2");
        waitNoPendingTasksOnAll();

        execute("drop table my_table_1");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_table_1 with (" +
                "wait_for_completion=true)");

        execute("select table_schema || '.' || table_name from information_schema.tables where table_schema = ? order by 1",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response).hasRows(getFqn("my_table_1"), getFqn("my_table_2"));
    }

    @Test
    public void test_parallel_restore_operations() throws Exception {
        createTable("my_table_1", false);
        createTable("my_table_2", false);
        createSnapshot(SNAPSHOT_NAME, "my_table_1", "my_table_2");
        waitNoPendingTasksOnAll();
        execute("drop table my_table_1");
        execute("drop table my_table_2");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_table_1 with (" +
                "wait_for_completion=false)");
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_table_2 with (" +
                "wait_for_completion=false)");

        assertBusy(() -> {
            execute(
                "select table_name from information_schema.tables where table_schema = ? order by 1",
                new Object[] { sqlExecutor.getCurrentSchema() }
            );
            assertThat(response).hasRows(
                "my_table_1",
                "my_table_2");
        });
    }

    /**
     * Test to restore a concrete partitioned table.
     * <p>
     * This requires a patch in ES in order to restore templates when concrete tables are passed as an restore argument:
     * https://github.com/crate/elasticsearch/commit/3c14e74a3e50ea7d890f436db72ff18c2953ebc4
     */
    @Test
    public void testRestoreOnlyOnePartitionedTable() throws Exception {
        createTable("my_parted_1", true);
        createTable("my_parted_2", true);
        createSnapshot(SNAPSHOT_NAME, "my_parted_1", "my_parted_2");

        execute("drop table my_parted_1");
        execute("drop table my_parted_2");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_parted_1 with (" +
                "wait_for_completion=true)");

        execute("select table_schema || '.' || table_name from information_schema.tables where table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response).hasRows(getFqn("my_parted_1"));
    }

    @Test
    public void testRestoreEmptyPartitionedTableUsingALL() throws Exception {
        execute("create table employees(section integer, name string) partitioned by (section)");
        ensureYellow();

        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        execute("drop table employees");
        ensureYellow();
        execute("RESTORE SNAPSHOT " + snapshotName() + " ALL with (wait_for_completion=true)");
        ensureYellow();

        execute("select table_schema || '.' || table_name from information_schema.tables where table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response).hasRows(getFqn("employees"));
    }

    @Test
    public void testRestoreEmptyPartitionedTable() throws Exception {
        execute("create table employees(section integer, name string) partitioned by (section)");
        ensureYellow();

        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        execute("drop table employees");
        ensureYellow();
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE employees with (wait_for_completion=true)");
        ensureYellow();

        execute("select table_schema || '.' || table_name from information_schema.tables where table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response).hasRows(getFqn("employees"));
    }

    @Test
    public void testResolveUnknownTableFromSnapshot() throws Exception {
        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute(
                        "RESTORE SNAPSHOT " + snapshotName() + " TABLE employees with (wait_for_completion=true)"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
                .hasMessageContaining(String.format("[%s..partitioned.employees.] template not found",
                                                    sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void test_cannot_create_snapshot_in_read_only_repo() {
        Asserts.assertSQLError(() -> execute("create snapshot my_repo_ro.s1 ALL WITH (wait_for_completion=true)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("cannot create snapshot in a readonly repository");
    }

    public void test_snapshot_with_corrupted_shard_index_file() throws Exception {
        execute("CREATE TABLE t1 (x int)");
        int numberOfDocs = randomIntBetween(0, 10);
        Object[][] bulkArgs = new Object[numberOfDocs][1];
        for (int i = 0; i < numberOfDocs; i++) {
            bulkArgs[i] = new Object[] { randomInt() };
        }
        execute("INSERT INTO t1 (x) VALUES (?)", bulkArgs);
        execute("REFRESH TABLE t1");

        var snapShotName1 = "s1";
        var fullSnapShotName1 = REPOSITORY_NAME + "." + snapShotName1;
        execute("CREATE SNAPSHOT " + fullSnapShotName1 + " ALL WITH (wait_for_completion=true)");

        var repositoryData = getRepositoryData();
        var indexIds = repositoryData.getIndices();
        assertThat(indexIds).hasSize(1);

        var corruptedIndex = indexIds.entrySet().iterator().next().getValue();
        var shardIndexFile = defaultRepositoryLocation.toPath().resolve("indices")
            .resolve(corruptedIndex.getId()).resolve("0")
            .resolve("index-" + repositoryData.shardGenerations().getShardGen(corruptedIndex, 0));

        // Truncating shard index file
        try (var outChan = Files.newByteChannel(shardIndexFile, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }

        assertSnapShotState(snapShotName1, SnapshotState.SUCCESS);

        execute("drop table t1");
        execute("RESTORE SNAPSHOT " + fullSnapShotName1 + " TABLE t1 with (wait_for_completion=true)");
        ensureYellow();

        execute("SELECT COUNT(*) FROM t1");
        assertThat(response.rows()[0][0]).isEqualTo((long) numberOfDocs);

        int numberOfAdditionalDocs = randomIntBetween(0, 10);
        bulkArgs = new Object[numberOfAdditionalDocs][1];
        for (int i = 0; i < numberOfAdditionalDocs; i++) {
            bulkArgs[i] = new Object[]{ randomInt() };
        }
        execute("INSERT INTO t1 (x) VALUES (?)", bulkArgs);
        execute("REFRESH TABLE t1");

        var snapShotName2 = "s2";
        var fullSnapShotName2 = REPOSITORY_NAME + ".s2";

        execute("CREATE SNAPSHOT " + fullSnapShotName2 + " ALL WITH (wait_for_completion=true)");
        assertSnapShotState(snapShotName2, SnapshotState.PARTIAL);
    }

    @Test
    public void test_restore_all_restores_complete_state() throws Exception {
        createSnapshotWithTablesAndMetadata();

        // restore ALL
        execute("RESTORE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        waitNoPendingTasksOnAll();

        execute("select table_name from information_schema.tables where table_name = 'my_table'");
        assertThat(response).hasRows("my_table");

        execute("SELECT table_name FROM information_schema.views WHERE table_name = 'my_view'");
        assertThat(response).hasRows("my_view");

        execute("select name from sys.users where name = 'my_user'");
        assertThat(response).hasRows("my_user");

        execute("SELECT type FROM sys.privileges WHERE grantee = 'my_user'");
        assertThat(response).hasRows("DQL");

        execute("SELECT routine_name, routine_type FROM information_schema.routines WHERE" +
                " routine_name IN ('a1', 'custom') ORDER BY 1");
        assertThat(response).hasRows(
            "a1| ANALYZER",
            "custom| FUNCTION");
    }

    @Test
    public void test_restore_all_tables_only() throws Exception {
        createTable("t2", true);
        createSnapshotWithTablesAndMetadata();
        execute("drop table t2");

        // restore all tables
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLES WITH (wait_for_completion=true)");
        waitNoPendingTasksOnAll();

        execute("select table_name from information_schema.tables where table_schema = ? order by 1",
                $(sqlExecutor.getCurrentSchema()));
        assertThat(response).hasRows(
            "my_table",
            "t2");
    }

    @Test
    public void test_restore_metadata_only_does_not_restore_tables() throws Exception {
        createSnapshotWithTablesAndMetadata();

        // restore METADATA only
        execute("RESTORE SNAPSHOT " + snapshotName() + " METADATA WITH (wait_for_completion=true)");
        waitNoPendingTasksOnAll();

        execute("SELECT table_name FROM information_schema.views WHERE table_name = 'my_view'");
        assertThat(response).hasRows("my_view");

        execute("SELECT name FROM sys.users WHERE name = 'my_user'");
        assertThat(response).hasRows("my_user");

        execute("SELECT type FROM sys.privileges WHERE grantee = 'my_user'");
        assertThat(response).hasRows("DQL");

        execute("SELECT routine_name, routine_type FROM information_schema.routines WHERE" +
                " routine_name IN ('a1', 'custom') ORDER BY 1");
        assertThat(response).hasRows(
            "a1| ANALYZER",
            "custom| FUNCTION");

        // NO tables must be restored
        execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'my_table'");
        assertThat(response.rowCount()).isEqualTo(0L);

    }

    /**
     * Restoring ANALYZERS will result in restoring analyzer settings out of the global settings
     */
    @Test
    public void test_restore_analyzers_only() throws Exception {
        createSnapshotWithTablesAndMetadata();

        execute("RESTORE SNAPSHOT " + snapshotName() + " ANALYZERS WITH (wait_for_completion=true)");
        waitNoPendingTasksOnAll();

        execute("SELECT routine_name, routine_type FROM information_schema.routines WHERE" +
                " routine_name IN ('a1', 'custom') ORDER BY 1");
        assertThat(response).hasRows("a1| ANALYZER");

        // All other MUST NOT be restored
        execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'my_table'");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("SELECT table_name FROM information_schema.views WHERE table_name = 'my_view'");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("SELECT name FROM sys.users WHERE name = 'my_user'");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("SELECT type FROM sys.privileges WHERE grantee = 'my_user'");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    /**
     * Restoring USERS will result in restoring custom metadata only and NO global settings.
     * This test a regression which resulted in restoring the custom metadata only if global settings
     * were also marked to be restored.
     */
    @Test
    public void test_restore_custom_metadata_only() throws Exception {
        createSnapshotWithTablesAndMetadata();

        execute("RESTORE SNAPSHOT " + snapshotName() + " USERS WITH (wait_for_completion=true)");
        waitNoPendingTasksOnAll();

        execute("SELECT name FROM sys.users WHERE name = 'my_user'");
        assertThat(response).hasRows("my_user");
    }

    @Test
    public void test_create_snapshot_tables_does_not_store_global_state() {
        createTable("custom.t1", false);
        execute("CREATE USER my_user");

        execute("CREATE SNAPSHOT " + snapshotName() + " TABLE custom.t1 WITH (wait_for_completion=true)");

        execute("DROP TABLE custom.t1");
        execute("DROP USER my_user");

        // restore everything from the snapshot to validate that it only contains the table
        execute("RESTORE SNAPSHOT " + snapshotName() + " ALL");

        execute("SELECT table_name FROM information_schema.tables WHERE table_name = 't1'");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("SELECT name FROM sys.users WHERE name = 'my_user'");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void test_restore_empty_partitioned_table_does_not_restore_other_tables() {
        createTable("custom.empty_parted1", true, false);
        createTable("custom.empty_parted2", true, false);
        createTable("custom.parted", true);
        createTable("custom.t1", false);
        createTable("custom.t2", false);

        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");

        execute("DROP TABLE custom.empty_parted1");
        execute("DROP TABLE custom.empty_parted2");
        execute("DROP TABLE custom.parted");
        execute("DROP TABLE custom.t1");
        execute("DROP TABLE custom.t2");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE custom.empty_parted1");
        execute("SELECT table_name FROM information_schema.tables WHERE table_schema='custom'");
        assertThat(response).hasRows("empty_parted1");

        execute("DROP TABLE custom.empty_parted1");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE custom.empty_parted1, custom.empty_parted2");
        execute("SELECT table_name FROM information_schema.tables WHERE table_schema='custom' ORDER BY 1");
        assertThat(response).hasRows(
            "empty_parted1",
            "empty_parted2");
    }

    @Test
    public void test_can_restore_snapshots_taken_after_swap_table() throws Exception {
        execute("CREATE TABLE t01 as SELECT a, random() b FROM generate_series(1, 10, 1) as t(a)");
        execute("CREATE TABLE t02 as SELECT a, random() b FROM generate_series(10, 20, 1) as t(a)");
        execute("CREATE SNAPSHOT my_repo.s1 ALL WITH (wait_for_completion=true)");
        int i = 2;
        boolean expectt01 = true;
        for (; i < randomIntBetween(3, 10); i++) {
            expectt01 = !expectt01;
            execute("ALTER CLUSTER SWAP TABLE t01 TO t02");
            execute("CREATE SNAPSHOT my_repo.s" + i + " ALL WITH (wait_for_completion=true)");
        }
        execute("DROP TABLE t01");
        execute("DROP TABLE t02");
        execute("RESTORE SNAPSHOT my_repo.s" + (i - 1) + " ALL");
        execute("refresh table t01");
        String[] t01 = new String[] {
            "1",
            "2",
            "3"
        };
        String[] t02 = new String[] {
            "10",
            "11",
            "12"
        };
        assertThat(execute("select a from t01 order by a limit 3")).hasRows(expectt01 ? t01 : t02);
    }

    @Test
    public void test_can_restore_snapshot_taken_before_swap_table() throws Exception {
        execute("CREATE TABLE t01 as SELECT a, random() b FROM generate_series(1, 10, 1) as t(a)");
        execute("CREATE TABLE t02 as SELECT a, random() b FROM generate_series(10, 20, 1) as t(a)");

        execute("CREATE SNAPSHOT my_repo.s1 ALL WITH (wait_for_completion=true)");
        execute("alter cluster swap table t01 to t02");
        execute("CREATE SNAPSHOT my_repo.s2 ALL WITH (wait_for_completion=true)");
        execute("DROP TABLE t01");
        execute("DROP TABLE t02");
        execute("RESTORE SNAPSHOT my_repo.s1 table t01");
        execute("refresh table t01");
        assertThat(execute("select a from t01 order by a limit 3")).hasRows(
            "1",
            "2",
            "3"
        );
    }

    @Test
    public void test_can_restore_snapshots_after_swapped_table_back_and_forth() throws Exception {
        execute("CREATE TABLE t01 AS SELECT a, random() b FROM generate_series(1, 10, 1) as t(a)");
        execute("CREATE TABLE t02 AS SELECT a, random() b FROM generate_series(10, 20, 1) as t(a)");
        execute("CREATE SNAPSHOT my_repo.s1 ALL WITH (wait_for_completion=true)");
        execute("ALTER CLUSTER SWAP TABLE t01 TO t02");
        execute("ALTER CLUSTER SWAP TABLE t02 TO t01");
        execute("CREATE SNAPSHOT my_repo.s2 ALL WITH (wait_for_completion=true)");
        execute("DROP TABLE t01");
        execute("DROP TABLE t02");
        execute("RESTORE SNAPSHOT my_repo.s2 ALL");
        execute("refresh table t01");
        assertThat(execute("select a from t01 order by a limit 3")).hasRows(
            "1",
            "2",
            "3"
        );
    }

    @Test
    public void test_can_restore_snapshot_after_swap_table_with_drop_source() throws Exception {
        execute("CREATE TABLE t01 AS SELECT a, random() b FROM generate_series(1, 10, 1) as t(a)");
        execute("CREATE TABLE t02 AS SELECT a, random() b FROM generate_series(10, 20, 1) as t(a)");
        execute("CREATE SNAPSHOT my_repo.s1 ALL WITH (wait_for_completion=true)");

        execute("ALTER CLUSTER SWAP TABLE t01 TO t02 WITH (drop_source = true)");

        execute("CREATE SNAPSHOT my_repo.s2 ALL WITH (wait_for_completion=true)");
        execute("DROP TABLE t02");
        execute("RESTORE SNAPSHOT my_repo.s2 ALL");
        execute("refresh table t02");
        assertThat(execute("select a from t02 order by a limit 3")).hasRows(
            "1",
            "2",
            "3"
        );
    }

    private void createSnapshotWithTablesAndMetadata() throws Exception {
        createTable("my_table", false);
        // creates custom metadata
        execute("CREATE USER my_user");
        execute("GRANT DQL TO my_user");
        execute("CREATE VIEW my_view AS SELECT * FROM my_table LIMIT 1");
        execute("CREATE FUNCTION custom(string) RETURNS STRING LANGUAGE dummy_lang AS '42'");
        // creates persistent cluster settings
        execute("CREATE ANALYZER a1 (TOKENIZER keyword)");

        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        assertThat(response.rowCount()).isEqualTo(1L);
        waitNoPendingTasksOnAll();

        // drop all created
        execute("REVOKE ALL FROM my_user");
        execute("DROP USER my_user");
        execute("DROP VIEW my_view");
        execute("DROP TABLE my_table");
        execute("DROP ANALYZER a1");
        execute("DROP FUNCTION custom(string)");
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void test_restore_non_partitioned_tables_with_different_fqn() throws Exception {
        execute_statements_that_restore_tables_with_different_fqn(false);
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void test_restore_partitioned_tables_with_different_fqn() throws Exception {
        execute_statements_that_restore_tables_with_different_fqn(true);

        // Restore specific partition.
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE source.my_table_1 PARTITION (date='1970-01-01') with (" +
            "ignore_unavailable=false, " +
            "wait_for_completion=true, " +
            "table_rename_replacement = '$1_single_partition')"
        );
        execute("select concat(table_schema, '.', table_name) as fqn from information_schema.tables " +
            "where table_schema = 'source' and table_name like 'my_table_1%' " +
            "order by fqn"
        );
        assertThat(response).hasRows(
            "source.my_table_1",
            "source.my_table_1_single_partition"
        );

        execute("select partition_ident from information_schema.table_partitions " +
            "where table_name = 'my_table_1_single_partition'");
        assertThat(response).hasRowCount(1);
        assertThat(response).hasRows( "04130");
    }


    /**
     * Tracks special case of passing '_all' as templates which should be handled specifically.
     */
    @Test
    @UseRandomizedSchema(random = false)
    public void test_restore_partitioned_tables_rename_all() throws Exception {
        // One with doc schema and another with custom schema.
        createTable("source.my_table_1", true);
        createTable("my_table_2", true);

        createSnapshot(SNAPSHOT_NAME, "source.my_table_1", "my_table_2");
        waitNoPendingTasksOnAll();

        execute("RESTORE SNAPSHOT " + snapshotName() + " ALL with (" +
            "wait_for_completion=true," +
            "schema_rename_replacement = 'schema_prefix_$1'," +
            "table_rename_replacement = 'table_postfix_$1')"
        );

        execute("select concat(table_schema, '.', table_name) as fqn from information_schema.tables " +
            "where table_name like '%my_table%' " +
            "order by fqn"
        );
        assertThat(response).hasRows(
            "doc.my_table_2",
            "schema_prefix_doc.table_postfix_my_table_2",
            "schema_prefix_source.table_postfix_my_table_1",
            "source.my_table_1"
        );
    }

    @Test
    public void test_restore_old_users() throws IOException {
        File repoDir = TEMPORARY_FOLDER.getRoot().toPath().toAbsolutePath().toFile();
        try (InputStream stream = Files.newInputStream(getDataPath("/repos/oldusersmetadata_repo.zip"))) {
            TestUtil.unzip(stream, repoDir.toPath());
        }
        execute(
            "CREATE REPOSITORY users_repo TYPE \"fs\" with (location=?, compress=true, readonly=true)",
            new Object[]{repoDir.getAbsolutePath()}
        );
        execute("CREATE USER \"John\" WITH (password='johns-password')");
        execute("CREATE USER \"Arthur\"");
        execute("CREATE USER \"Marios\"");
        execute("SELECT * FROM sys.users ORDER BY name");
        assertThat(response).hasRows(
            "Arthur| NULL| false",
            "John| ********| false",
            "Marios| NULL| false",
            "crate| NULL| true");

        // Snapshot contains the following users:
        // CREATE USER "Arthur" WITH (password='arthurs-password');
        // CREATE USER "Ford" WITH (password='fords-password');
        // CREATE USER "John";
        execute("RESTORE SNAPSHOT users_repo.usersnap USERS with (wait_for_completion=true)");

        execute("SELECT * FROM sys.users ORDER BY name");
        assertThat(response).hasRows(
            "Arthur| ********| false",
            "Ford| ********| false",
            "John| NULL| false",
            "crate| NULL| true");

        // Before any CREATE/ALTER/DROP operation, RolesMetadata still has the users&roles defined
        // but only UsersMetadata are used
        RolesMetadata rolesMetadata = cluster().clusterService().state().metadata().custom(RolesMetadata.TYPE);
        assertThat(rolesMetadata).isNotNull();
        Assertions.assertThat(rolesMetadata.roles()).containsOnlyKeys("Arthur", "John", "Marios");
        UsersMetadata usersMetadata = cluster().clusterService().state().metadata().custom(UsersMetadata.TYPE);
        assertThat(usersMetadata).isNotNull();
        assertThat(usersMetadata.users()).containsOnlyKeys("Arthur", "Ford", "John");

        execute("ALTER USER \"John\" SET (password='johns-new-password')");
        execute("SELECT * FROM sys.users ORDER BY name");
        assertThat(response).hasRows(
            "Arthur| ********| false",
            "Ford| ********| false",
            "John| ********| false",
            "crate| NULL| true");

        // After CREATE/ALTER/DROP operation, current RolesMetadata is dropped and
        // recreated from UsersMetadata, thus fully overriden by these restored UsersMetadata
        rolesMetadata = cluster().clusterService().state().metadata().custom(RolesMetadata.TYPE);
        assertThat(rolesMetadata).isNotNull();
        Assertions.assertThat(rolesMetadata.roles()).containsOnlyKeys("Arthur", "Ford", "John");
        usersMetadata = cluster().clusterService().state().metadata().custom(UsersMetadata.TYPE);
        assertThat(usersMetadata).isNull();
    }

    private void execute_statements_that_restore_tables_with_different_fqn(boolean partitioned) throws Exception {
        // One with doc schema and another with custom schema.
        createTable("source.my_table_1", partitioned);
        createTable("my_table_2", partitioned);

        createSnapshot(SNAPSHOT_NAME, "source.my_table_1", "my_table_2");
        waitNoPendingTasksOnAll();

        restoreWithDifferentName();
        restoreIntoDifferentSchema();
    }

    private void restoreWithDifferentName() {
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE source.my_table_1, my_table_2 with (" +
            "wait_for_completion=true," +
            "table_rename_replacement = 'my_prefix_$1')"
        );

        execute("select concat(table_schema, '.', table_name) as fqn from information_schema.tables " +
            "where table_name like '%my_table%' " +
            "order by fqn"
        );
        assertThat(response).hasRows(
            "doc.my_prefix_my_table_2",
            "doc.my_table_2",
            "source.my_prefix_my_table_1",
            "source.my_table_1"
        );
    }

    private void restoreIntoDifferentSchema() {
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE source.my_table_1, my_table_2 with (" +
            "wait_for_completion=true," +
            "schema_rename_replacement = 'target')"
        );

        execute("select concat(table_schema, '.', table_name) as fqn from information_schema.tables " +
            "where table_name like 'my_table%' " + // No % at the beginning to exclude irrelevant tables from the restoreWithDifferentName() call.
            "order by fqn"
        );
        assertThat(response).hasRows(
            "doc.my_table_2",
            "source.my_table_1",
            "target.my_table_1",
            "target.my_table_2"
        );
    }

    private void assertSnapShotState(String snapShotName, SnapshotState state) {
        execute(
            "SELECT state, array_length(concrete_indices, 1) FROM sys.snapshots where name = ? and repository = ?",
            new Object[]{snapShotName, REPOSITORY_NAME});

        assertThat(response.rows()[0][0]).isEqualTo(state.name());
        assertThat(response.rows()[0][1]).isEqualTo(1);
    }

    private static void assertAllRepoSnapshotFilesAreDeleted(File location) throws IOException {
        //Make sure the file location does not consist of any .dat file
        Files.walk(location.toPath())
            .filter(Files::isRegularFile)
            .forEach(x -> assertThat(x.getFileName().endsWith(".dat")).isFalse());
    }

    private RepositoryData getRepositoryData() throws Exception {
        RepositoriesService service = cluster().getInstance(RepositoriesService.class, cluster().getMasterName());
        Repository repository = service.repository(REPOSITORY_NAME);
        ThreadPool threadPool = cluster().getInstance(ThreadPool.class, cluster().getMasterName());
        final SetOnce<RepositoryData> repositoryData = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            repositoryData.set(ESBlobStoreTestCase.getRepositoryData(repository));
            latch.countDown();
        });
        latch.await();
        return repositoryData.get();
    }
}
