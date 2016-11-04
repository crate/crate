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

import io.crate.testing.UseJdbc;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(transportClientRatio = 0)
@UseJdbc(0) // missing column types
public class SysSnapshotsTest extends SQLTransportIntegrationTest {

    @ClassRule
    public static TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private static String REPOSITORY_NAME = "test_snapshots_repo";

    private List<String> snapshots = new ArrayList<>();
    private long createdTime;
    private long finishedTime;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMP_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    @Before
    public void setUpSnapshots() throws Exception {
        createRepository(REPOSITORY_NAME);
        createdTime = System.currentTimeMillis();
        createTableAndSnapshot("test_table", "test_snap_1");
        finishedTime = System.currentTimeMillis();
        waitUntilShardOperationsFinished();
    }

    @After
    public void cleanUp() throws Exception {
        Iterator<String> it = snapshots.iterator();
        while (it.hasNext()) {
            deleteSnapshot(it.next());
            it.remove();
        }
        deleteRepository(REPOSITORY_NAME);
    }

    private void createRepository(String name) {
        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository(name)
            .setType("fs")
            .setSettings(Settings.builder()
                .put("location", new File(TEMP_FOLDER.getRoot(), "backup").getAbsolutePath())
                .put("chunk_size", "5k")
                .put("compress", false)
            ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    private void deleteRepository(String name) {
        DeleteRepositoryResponse deleteRepositoryResponse = client().admin().cluster().prepareDeleteRepository(name).get();
        assertThat(deleteRepositoryResponse.isAcknowledged(), equalTo(true));
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
        snapshots.add(snapshotName);

    }

    private void deleteSnapshot(String name) {
        DeleteSnapshotResponse deleteSnapshotResponse = client().admin().cluster()
            .prepareDeleteSnapshot(REPOSITORY_NAME, name).get();
        assertThat(deleteSnapshotResponse.isAcknowledged(), equalTo(true));
    }

    @Test
    public void testQueryAllColumns() throws Exception {
        execute("select * from sys.snapshots");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols().length, is(7));
        assertThat(response.cols(), is(new String[]{"concrete_indices", "finished", "name", "repository", "started", "state", "version"}));
        assertThat(response.columnTypes(), is(
            new DataType[]{
                new ArrayType(StringType.INSTANCE),
                TimestampType.INSTANCE,
                StringType.INSTANCE,
                StringType.INSTANCE,
                TimestampType.INSTANCE,
                StringType.INSTANCE,
                StringType.INSTANCE
            }));
        assertThat((String[]) response.rows()[0][0], arrayContaining("test_table"));
        assertThat((Long) response.rows()[0][1], lessThanOrEqualTo(finishedTime));
        assertThat((String) response.rows()[0][2], is("test_snap_1"));
        assertThat((String) response.rows()[0][3], is(REPOSITORY_NAME));
        assertThat((Long) response.rows()[0][4], greaterThanOrEqualTo(createdTime));
        assertThat((String) response.rows()[0][5], is(SnapshotState.SUCCESS.name()));
        assertThat((String) response.rows()[0][6], is(Version.CURRENT.toString()));

    }
}
