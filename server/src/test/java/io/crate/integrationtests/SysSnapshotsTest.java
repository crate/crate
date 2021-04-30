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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.testing.UseJdbc;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

@ESIntegTestCase.ClusterScope()
@UseJdbc(0) // missing column types
public class SysSnapshotsTest extends SQLIntegrationTestCase {

    @ClassRule
    public static TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private final static String REPOSITORY_NAME = "test_snapshots_repo";

    private List<String> snapshots = new ArrayList<>();
    private long createdTime;
    private long finishedTime;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMP_FOLDER.getRoot().getAbsolutePath())
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that verify an exact wait time
            .build();
    }

    @Before
    public void setUpSnapshots() throws Exception {
        createRepository(REPOSITORY_NAME);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class);
        createdTime = threadPool.absoluteTimeInMillis();
        createTableAndSnapshot("test_table", "test_snap_1");
        finishedTime = threadPool.absoluteTimeInMillis();
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
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster().preparePutRepository(name)
            .setType("fs")
            .setSettings(Settings.builder()
                .put("location", new File(TEMP_FOLDER.getRoot(), "backup").getAbsolutePath())
                .put("chunk_size", "5k")
                .put("compress", false)
            ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    private void deleteRepository(String name) {
        AcknowledgedResponse deleteRepositoryResponse = client().admin().cluster().prepareDeleteRepository(name).get();
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

    private void createSnapshot(String snapshotName, String table) {
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster()
            .prepareCreateSnapshot(REPOSITORY_NAME, snapshotName)
            .setWaitForCompletion(true).setIndices(getFqn(table)).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        snapshots.add(snapshotName);

    }

    private void deleteSnapshot(String name) {
        AcknowledgedResponse deleteSnapshotResponse = client().admin().cluster()
            .prepareDeleteSnapshot(REPOSITORY_NAME, name).get();
        assertThat(deleteSnapshotResponse.isAcknowledged(), equalTo(true));
    }

    @Test
    public void testQueryAllColumns() {
        execute("select * from sys.snapshots");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), arrayContaining("concrete_indices", "failures", "finished", "name", "repository", "started", "state", "tables", "version"));
        ArrayType<String> stringArray = new ArrayType<>(DataTypes.STRING);
        assertThat(response.columnTypes(), arrayContaining(
            stringArray,
            stringArray,
            TimestampType.INSTANCE_WITH_TZ,
            StringType.INSTANCE,
            StringType.INSTANCE,
            TimestampType.INSTANCE_WITH_TZ,
            StringType.INSTANCE,
            stringArray,
            StringType.INSTANCE
        ));
        Object[] firstRow = response.rows()[0];
        assertThat((List<Object>) firstRow[0], Matchers.contains(getFqn("test_table")));
        assertThat((List<Object>) firstRow[1], Matchers.empty());
        assertThat((Long) firstRow[2], lessThanOrEqualTo(finishedTime));
        assertThat(firstRow[3], is("test_snap_1"));
        assertThat(firstRow[4], is(REPOSITORY_NAME));
        assertThat((Long) firstRow[5], greaterThanOrEqualTo(createdTime));
        assertThat(firstRow[6], is(SnapshotState.SUCCESS.name()));
        assertThat((List<Object>) firstRow[7], Matchers.contains(getFqn("test_table")));
        assertThat(firstRow[8], is(Version.CURRENT.toString()));

    }
}
