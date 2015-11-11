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
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.action.sql.TransportSQLAction;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;


public class SnapshotRestoreIntegrationTest extends SQLTransportIntegrationTest {

    private static final String REPOSITORY_NAME = "my_repo";
    private static final String SNAPSHOT_NAME = "my_snapshot";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("path.repo", TEMPORARY_FOLDER.getRoot().getAbsolutePath())
                .build();
    }

    @Before
    public void createRepository() throws Exception {
        execute("CREATE REPOSITORY " + REPOSITORY_NAME + " TYPE \"fs\" with (location=?, compress=True)",
                new Object[]{TEMPORARY_FOLDER.newFolder().getAbsolutePath()});
        assertThat(response.rowCount(), is(1L));
        waitNoPendingTasksOnAll();
    }

    @After
    public void dropRepository() throws Exception {
        execute("DROP REPOSITORY " + REPOSITORY_NAME);
        assertThat(response.rowCount(), is(1L));

        execute("reset GLOBAL cluster.routing.allocation.enable");
        waitNoPendingTasksOnAll();
    }

    private void createTableAndSnapshot(String tableName, String snapshotName) {
        createTableAndSnapshot(tableName, snapshotName, false);
    }

    private void createTableAndSnapshot(String tableName, String snapshotName, boolean partitioned) {
        createTable(tableName, partitioned);
        createSnapshot(snapshotName, tableName);
    }

    private void createTable(String tableName, boolean partitioned) {
        execute("CREATE TABLE " + tableName +  " (" +
                "  id long primary key, " +
                "  name string, " +
                "  date timestamp " + (partitioned ? "primary key," : ",") +
                "  ft string index using fulltext with (analyzer='german')" +
                ") " + (partitioned ? "partitioned by (date) " : "") +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("INSERT INTO " + tableName + " (id, name, date, ft) VALUES (?, ?, ?, ?)", new Object[][]{
                {1L, "foo", "1970-01-01", "The quick brown fox jumps over the lazy dog."},
                {2L, "bar", "2015-10-27T11:29:00+01:00", "Morgenstund hat Gold im Mund."},
                {3L, "baz", "1989-11-09", "Reden ist Schweigen. Silber ist Gold."},
        });
        execute("REFRESH TABLE " + tableName);
    }

    private void createSnapshot(String snapshotName, String... tables) {
        execute("CREATE SNAPSHOT " + REPOSITORY_NAME + "." + snapshotName + " TABLE " + Joiner.on(", ").join(tables) + " WITH (wait_for_completion=true)");
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

        execute("select name, \"repository\", concrete_indices, state from sys.snapshots");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("my_snapshot| my_repo| [backmeup]| SUCCESS\n"));
    }

    @Test
    public void testCreateSnapshotWithoutWaitForCompletion() throws Exception {
        // this test just verifies that no exception is thrown if wait_for_completion is false
        execute("CREATE SNAPSHOT my_repo.snapshot_no_wait ALL WITH (wait_for_completion=false)");
        assertThat(response.rowCount(), is(1L));
        waitForSnapshotCompletion("snapshot_no_wait");
    }

    private void waitForSnapshotCompletion(final String snapshotName) throws Exception {
        // wait for success so that @after dropRepository doesn't fail
        assertBusy(new Runnable() {
            @Override
            public void run() {
                SQLRequest request = new SQLRequest(
                        "select count(*) from sys.snapshots where state = 'SUCCESS' and name = ?", $(snapshotName));
                for (TransportSQLAction transportSQLAction : internalCluster().getInstances(TransportSQLAction.class)) {
                    SQLResponse response = transportSQLAction.execute(request).actionGet(5, TimeUnit.SECONDS);
                    assertThat(((long) response.rows()[0][0]), is(1L));
                }
            }
        });
    }

    @Test
    public void testCreateSnapshotFromPartition() throws Exception {
        createTable("custom.backmeup", true);

        execute("CREATE SNAPSHOT " + snapshotName() + " TABLE custom.backmeup PARTITION (date='1970-01-01')  WITH (wait_for_completion=true)");
        assertThat(response.rowCount(), is(1L));

        execute("select name, \"repository\", concrete_indices, state from sys.snapshots");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("my_snapshot| my_repo| [custom..partitioned.backmeup.04130]| SUCCESS\n"));
    }

    @Test
    public void testCreateExistingSnapshot() throws Exception {
        createTable("backmeup", randomBoolean());

        execute("CREATE SNAPSHOT " + snapshotName() + " ALL WITH (wait_for_completion=true)");
        assertThat(response.rowCount(), is(1L));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Snapshot \"my_repo\".\"my_snapshot\" already exists");
        execute("CREATE SNAPSHOT my_repo.my_snapshot ALL WITH (wait_for_completion=true)");
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
    public void testCreateNotPartialSnapshotFails() throws Exception {
        execute("set global cluster.routing.allocation.enable=none");
        execute("CREATE TABLE backmeup (" +
                "  id long primary key, " +
                "  name string" +
                ") with (number_of_replicas=0)");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Error creating snapshot 'my_repo.my_snapshot': Tables don't have primary shards [backmeup]");
        execute("CREATE SNAPSHOT " + snapshotName() + " TABLE backmeup WITH (wait_for_completion=true)");
    }

    @Test
    public void testCreateSnapshotInURLRepoFails() throws Exception {
        execute("CREATE REPOSITORY uri_repo TYPE url WITH (url=?)",
                new Object[]{ TEMPORARY_FOLDER.newFolder().toURI().toString() });
        waitNoPendingTasksOnAll();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("URL repository doesn't support this operation");
        execute("CREATE SNAPSHOT uri_repo.my_snapshot ALL WITH (wait_for_completion=true)");
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

        client().admin().cluster().prepareRestoreSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME)
                .setIncludeAliases(true)
                .setRestoreGlobalState(true)
                .execute().actionGet();
        ensureGreen();

        execute("select * from survivor order by bla");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "bar| 1.4\n" +
                "baz| 1.2\n" +
                "foo| 1.2\n"));
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
    public void testRestoreSnapshotSinglePartitionWithDroppedTable() throws Exception {
        createTableAndSnapshot("my_parted_table", SNAPSHOT_NAME, true);

        execute("drop table my_parted_table");
        waitNoPendingTasksOnAll();
        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_parted_table PARTITION (date='1970-01-01') with (" +
                "ignore_unavailable=false, " +
                "wait_for_completion=true)");

        execute("select date from my_parted_table");
        assertThat(TestingHelpers.printedTable(response.rows()), is("0\n"));
    }

    @Test
    public void testRestoreSnapshotIgnoreUnavailable() throws Exception {
        createTableAndSnapshot("my_table", SNAPSHOT_NAME);

        execute("drop table my_table");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_table, not_my_table with (" +
                "ignore_unavailable=true, " +
                "wait_for_completion=true)");
        execute("select schema_name || '.' || table_name from information_schema.tables where schema_name='doc'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("doc.my_table\n"));
    }

    @Test
    public void testRestoreOnlyOneTable() throws Exception {
        createTable("my_table_1", false);
        createTable("my_table_2", false);
        createSnapshot(SNAPSHOT_NAME, "my_table_1", "my_table_2");

        execute("drop table my_table_1");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_table_1 with (" +
                "wait_for_completion=true)");

        execute("select schema_name || '.' || table_name from information_schema.tables where schema_name='doc' order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("doc.my_table_1\ndoc.my_table_2\n"));
    }

    @Test
    public void testRestoreOnlyOnePartitionedTable() throws Exception {
        createTable("my_parted_1", true);
        createTable("my_parted_2", true);
        createSnapshot(SNAPSHOT_NAME, "my_parted_1", "my_parted_2");

        execute("drop table my_parted_1");
        execute("drop table my_parted_2");

        execute("RESTORE SNAPSHOT " + snapshotName() + " TABLE my_parted_1 with (" +
                "ignore_unavailable=true, " +
                "wait_for_completion=true)");

        execute("select schema_name || '.' || table_name from information_schema.tables where schema_name='doc'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("doc.my_parted_1\n"));
    }
}
