/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import io.crate.action.sql.SQLActionException;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class SnapshotIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static final String REPOSITORY_NAME = "my_repo";

    @ClassRule
    public static TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                .put("path.repo", TEMPORARY_FOLDER.getRoot().getAbsolutePath())
                .build();
    }

    @Before
    public void createRepository() throws Exception {
        execute("CREATE REPOSITORY " + REPOSITORY_NAME + " TYPE \"fs\" with (location=?, compress=True)",
                new Object[]{new File(TEMPORARY_FOLDER.getRoot(), "backup").getAbsolutePath()});
        assertThat(response.rowCount(), is(1L));
        waitNoPendingTasksOnAll();
    }

    @After
    public void dropRepository() throws Exception {
        execute("DROP REPOSITORY " + REPOSITORY_NAME);
        assertThat(response.rowCount(), is(1L));
    }

    private void createTableAndSnapshot(String tableName, String snapshotName) {
        execute("create table " + tableName + " (id int primary key) with(number_of_replicas=0)");
        ensureYellow();

        for (int i = 0; i < 100; i++) {
            execute("insert into " + tableName + " (id) values (?)", new Object[]{i});
        }
        refresh();

        createSnapshot(snapshotName, tableName);
    }

    private void createSnapshot(String snapshotName, String... tables) {
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster()
                .prepareCreateSnapshot(REPOSITORY_NAME, snapshotName)
                .setWaitForCompletion(true).setIndices(tables).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
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
}
