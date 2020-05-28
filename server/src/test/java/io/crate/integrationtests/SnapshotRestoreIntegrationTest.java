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

package io.crate.integrationtests;

import com.google.common.base.Joiner;
import io.crate.action.sql.SQLActionException;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SnapshotRestoreIntegrationTest extends SQLTransportIntegrationTest {

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

    @Before
    public void createRepository() throws Exception {
        defaultRepositoryLocation = TEMPORARY_FOLDER.newFolder();
        execute("CREATE REPOSITORY " + REPOSITORY_NAME + " TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{defaultRepositoryLocation.getAbsolutePath()});
        assertThat(response.rowCount(), is(1L));
        execute(
            "CREATE REPOSITORY my_repo_ro TYPE \"fs\" with (location=?, compress=true, readonly=true)",
            new Object[]{defaultRepositoryLocation.getAbsolutePath()}
        );
    }

    private void createTableAndSnapshot(String tableName, String snapshotName) {
        createTableAndSnapshot(tableName, snapshotName, false);
    }

    private void createTableAndSnapshot(String tableName, String snapshotName, boolean partitioned) {
        createTable(tableName, partitioned);
        createSnapshot(snapshotName, tableName);
    }

    private void createTable(String tableName, boolean partitioned) {
        execute("CREATE TABLE " + tableName + " (" +
                "  id long primary key, " +
                "  name string, " +
                "  date timestamp with time zone " + (partitioned ? "primary key," : ",") +
                "  ft string index using fulltext with (analyzer='default')" +
                ") " + (partitioned ? "partitioned by (date) " : "") +
                "clustered into 1 shards with (number_of_replicas=0)");
        execute("INSERT INTO " + tableName + " (id, name, date, ft) VALUES (?, ?, ?, ?)", new Object[][]{
            {1L, "foo", "1970-01-01", "The quick brown fox jumps over the lazy dog."},
            {2L, "bar", "2015-10-27T11:29:00+01:00", "Morgenstund hat Gold im Mund."},
            {3L, "baz", "1989-11-09", "Reden ist Schweigen. Silber ist Gold."},
        });
        execute("REFRESH TABLE " + tableName);
    }

    private void createSnapshot(String snapshotName, String... tables) {
        execute("CREATE SNAPSHOT " + REPOSITORY_NAME + "." + snapshotName + " TABLE " + Joiner.on(", ").join(tables) +
                " WITH (wait_for_completion=true)");
        assertThat(response.rowCount(), is(1L));
    }

    private static String snapshotName() {
        return String.format(Locale.ENGLISH, "%s.%s", REPOSITORY_NAME, SNAPSHOT_NAME);
    }

    @Test
    public void testDropSnapshot() throws Exception {
        String snapshotName = "my_snap_1";
        createTableAndSnapshot("my_table", snapshotName);

        execute("drop snapshot " + REPOSITORY_NAME + "." + snapshotName);
        assertThat(response.rowCount(), is(1L));

        execute("select * from sys.snapshots where name = ?", new Object[]{snapshotName});
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testDropUnknownSnapshot() throws Exception {
        String snapshot = "unknown_snap";
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "Snapshot '%s.%s' unknown", REPOSITORY_NAME, snapshot));
        execute("drop snapshot " + REPOSITORY_NAME + "." + snapshot);
    }

    @Test
    public void testDropSnapshotUnknownRepository() throws Exception {
        String repository = "unknown_repo";
        String snapshot = "unknown_snap";
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "Repository '%s' unknown", repository));
        execute("drop snapshot " + repository + "." + snapshot);
    }

    @Test
    public void testCreateSnapshot() throws Exception {
        createTable("backmeup", false);
        execute("CREATE SNAPSHOT " + snapshotName() + " TABLE backmeup WITH (wait_for_completion=true)");
        assertThat(response.rowCount(), is(1L));

        execute("select name, \"repository\", concrete_indices, state from sys.snapshots order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is(String.format(
                "my_snapshot| my_repo| [%s.backmeup]| SUCCESS\n" +
                // shows up twice because both repos have the same data path
                "my_snapshot| my_repo_ro| [%s.backmeup]| SUCCESS\n",
                sqlExecutor.getCurrentSchema(),
                sqlExecutor.getCurrentSchema())));
    }

    @Test
    public void testCreateSnapshotWithoutWaitForCompletion() throws Exception {
        // this test just verifies that no exception is thrown if wait_for_completion is false
        execute("CREATE SNAPSHOT my_repo.snapshot_no_wait ALL WITH (wait_for_completion=false)");
        assertThat(response.rowCount(), is(1L));
        waitForCompletion(REPOSITORY_NAME, "snapshot_no_wait", TimeValue.timeValueSeconds(20));
    }

    private SnapshotInfo waitForCompletion(String repository, String snapshotName, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        Snapshot snapshot = new Snapshot(repository, new SnapshotId(repository, snapshotName));
        while (System.currentTimeMillis() - start < timeout.millis()) {
            List<SnapshotInfo> snapshotInfos = client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshotName).get().getSnapshots();
            assertThat(snapshotInfos.size(), equalTo(1));
            if (snapshotInfos.get(0).state().completed()) {
                // Make sure that snapshot clean up operations are finished
                ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
                SnapshotsInProgress snapshotsInProgress = stateResponse.getState().custom(SnapshotsInProgress.TYPE);
                if (snapshotsInProgress == null || snapshotsInProgress.snapshot(snapshot) == null) {
                    return snapshotInfos.get(0);
                }
            }
            Thread.sleep(100);
        }
        fail("Timeout waiting for snapshot completion!");
        return null;
    }

    @Test
    public void testCreateSnapshotFromPartition() throws Exception {
        createTable("custom.backmeup", true);

        execute("CREATE SNAPSHOT " + snapshotName() +
                " TABLE custom.backmeup PARTITION (date='1970-01-01')  WITH (wait_for_completion=true)");
        assertThat(response.rowCount(), is(1L));

        execute("select name, \"repository\", concrete_indices, state from sys.snapshots order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("my_snapshot| my_repo| [custom..partitioned.backmeup.04130]| SUCCESS\n" +
               // shows up twice because the repos have the same fs path.
               "my_snapshot| my_repo_ro| [custom..partitioned.backmeup.04130]| SUCCESS\n"));
    }

    @Test
    public void testCreateSnapshotAllBlobsExcluded() throws Exception {
        execute("CREATE TABLE t1 (id INTEGER, name STRING)");
        execute("CREATE BLOB TABLE b1");
        ensureYellow();
        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        assertThat(response.rowCount(), is(1L));

        execute("select concrete_indices from sys.snapshots");
        assertThat(response.rows()[0][0], is(List.of(getFqn("t1"))));
    }

    @Test
    public void testCreateExistingSnapshot() throws Exception {
        createTable("backmeup", randomBoolean());

        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        assertThat(response.rowCount(), is(1L));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Invalid snapshot name [my_snapshot], snapshot with the same name already exists");
        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
    }

    @Test
    public void testCreateSnapshotUnknownRepo() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        execute("CREATE SNAPSHOT unknown_repo.my_snapshot ALL WITH (wait_for_completion=true)");
    }

    @Test
    public void testInvalidSnapshotName() throws Exception {

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Invalid snapshot name [MY_UPPER_SNAPSHOT], must be lowercase");
        execute("CREATE SNAPSHOT my_repo.\"MY_UPPER_SNAPSHOT\" ALL WITH (wait_for_completion=true)");
    }

    @Test
    public void testSnapshotWithMetadataDoesNotDeleteExistingStuff() throws Exception {
        createTable("my_other", true);
        execute("CREATE SNAPSHOT " + snapshotName() + " TABLE my_other with (wait_for_completion=true)");

        execute("alter table my_other add column x double");
        waitForMappingUpdateOnAll("my_other", "x");
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
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "bar| 1.4\n" +
            "baz| 1.2\n" +
            "foo| 1.2\n"));
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

        ActionFuture<SQLResponse> createSnapshot = null;
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
            createSnapshot.actionGet();
        }

        execute("DROP table test");

        execute("select state from sys.snapshots where name=?", new Object[]{SNAPSHOT_NAME});
        assertThat(response.rows()[0][0], is("SUCCESS"));

        execute("RESTORE SNAPSHOT " + snapshotName() + " ALL with (wait_for_completion=true)");

        waitNoPendingTasksOnAll();

        SnapshotsInProgress finalSnapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));

        ImmutableOpenMap<String, IndexMetaData> state = clusterService().state().metaData().indices();
        IndexMetaData indexMetaData = state.values().iterator().next().value;
        int sizeOfProperties = ((Map<?, ?>) indexMetaData.getMappings().values().iterator().next().value.sourceAsMap().get("properties")).size();

        execute("select count(*) from test");

        assertThat(
            "Documents were restored but the restored index mapping was older than some documents and misses some of their fields",
            (Long)response.rows()[0][0], lessThanOrEqualTo((long) sizeOfProperties));
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
        assertThat(response.rowCount(), is(3L));
    }

    @Test
    public void testRestoreSnapshotSinglePartition() throws Exception {
        createTableAndSnapshot("my_parted_table", SNAPSHOT_NAME, true);

        execute("delete from my_parted_table");
        waitNoPendingTasksOnAll();
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_parted_table PARTITION (date='1970-01-01') with (" +
                "ignore_unavailable=false, " +
                "wait_for_completion=true)");

        execute("select date from my_parted_table");
        assertThat(TestingHelpers.printedTable(response.rows()), is("0\n"));
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
        assertThat(TestingHelpers.printedTable(response.rows()), is("0\n1445941740000\n626572800000\n"));
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
        assertThat(TestingHelpers.printedTable(response.rows()), is("0\n"));
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
        assertThat(TestingHelpers.printedTable(response.rows()), is("0\n"));
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
        assertThat(TestingHelpers.printedTable(response.rows()), is(getFqn("my_table") + "\n"));
    }

    @Test
    public void testRestoreOnlyOneTable() throws Exception {
        createTable("my_table_1", false);
        createTable("my_table_2", false);
        createSnapshot(SNAPSHOT_NAME, "my_table_1", "my_table_2");

        execute("drop table my_table_1");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_table_1 with (" +
                "wait_for_completion=true)");

        execute("select table_schema || '.' || table_name from information_schema.tables where table_schema = ? order by 1",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(TestingHelpers.printedTable(response.rows()), is(getFqn("my_table_1") + "\n" + getFqn("my_table_2") + "\n"));
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
        assertThat(TestingHelpers.printedTable(response.rows()), is(getFqn("my_parted_1") + "\n"));
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
        assertThat(TestingHelpers.printedTable(response.rows()), is(getFqn("employees") + "\n"));
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
        assertThat(TestingHelpers.printedTable(response.rows()), is(getFqn("employees") + "\n"));
    }

    @Test
    public void testResolveUnknownTableFromSnapshot() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format("ResourceNotFoundException: [%s..partitioned.employees.] template not found", sqlExecutor.getCurrentSchema()));
        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        ensureYellow();
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE employees with (wait_for_completion=true)");
    }

    @Test
    public void test_cannot_create_snapshot_in_read_only_repo() {
        expectedException.expectMessage("cannot create snapshot in a readonly repository");
        execute("create snapshot my_repo_ro.s1 ALL WITH (wait_for_completion=true)");
    }

    public void test_snapshot_with_corrupted_shard_index_file() throws Exception {
        execute("CREATE TABLE t1 (x int)");
        var numberOfDocs = randomLongBetween(0, 10);
        for (int i = 0; i < numberOfDocs; i++) {
            execute("INSERT INTO t1 (x) VALUES (?)", new Object[]{randomInt()});
        }
        execute("REFRESH TABLE t1");

        var snapShotName1 = "s1";
        var fullSnapShotName1 =  REPOSITORY_NAME + "." + snapShotName1;
        execute("CREATE SNAPSHOT " + fullSnapShotName1 + " ALL WITH (wait_for_completion=true)");

        var repositoryData = getRepositoryData();
        var indexIds = repositoryData.getIndices();
        assertThat(indexIds.size(), equalTo(1));

        var corruptedIndex = indexIds.entrySet().iterator().next().getValue();
        var shardIndexFile = defaultRepositoryLocation.toPath().resolve("indices")
            .resolve(corruptedIndex.getId()).resolve("0")
            .resolve("index-0");

        // Truncating shard index file
        try (var outChan = Files.newByteChannel(shardIndexFile, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }

        assertSnapShotState(snapShotName1);

        execute("drop table t1");
        execute("RESTORE SNAPSHOT " +  fullSnapShotName1 + " TABLE t1 with (wait_for_completion=true)");
        ensureYellow();

        execute("SELECT COUNT(*) FROM t1");
        assertThat(response.rows()[0][0], is(numberOfDocs));

        var numberOfAdditionalDocs = randomLongBetween(0, 10);
        for (int i = 0; i < numberOfAdditionalDocs; i++) {
            execute("INSERT INTO t1 (x) VALUES (?)", new Object[]{randomInt()});
        }
        execute("REFRESH TABLE t1");

        var snapShotName2 = "s2";
        var fullSnapShotName2 = REPOSITORY_NAME + ".s2";

        execute("CREATE SNAPSHOT " + fullSnapShotName2 + " ALL WITH (wait_for_completion=true)");

        assertSnapShotState(snapShotName2);

        execute("drop table t1");
        execute("RESTORE SNAPSHOT " + fullSnapShotName2 + " TABLE t1 with (wait_for_completion=true)");
        ensureYellow();

        execute("SELECT COUNT(*) FROM t1");
        assertThat(response.rows()[0][0], is(numberOfDocs + numberOfAdditionalDocs));

    }

    private void assertSnapShotState(String snapShotName) {
        execute(
            "SELECT state, array_length(concrete_indices, 1) FROM sys.snapshots where name = ? and repository = ?",
            new Object[]{snapShotName, REPOSITORY_NAME});

        assertThat(response.rows()[0][0], is("SUCCESS"));
        assertThat(response.rows()[0][1], is(1));
    }

    private RepositoryData getRepositoryData() throws Exception {
        RepositoriesService service = internalCluster().getInstance(RepositoriesService.class, internalCluster().getMasterName());
        Repository repository = service.repository(REPOSITORY_NAME);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, internalCluster().getMasterName());
        final SetOnce<RepositoryData> repositoryData = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            repositoryData.set(repository.getRepositoryData());
            latch.countDown();
        });
        latch.await();
        return repositoryData.get();
    }
}
