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

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

@IntegTestCase.ClusterScope()
@UseJdbc(0) // missing column types
public class SysSnapshotsTest extends IntegTestCase {

    @ClassRule
    public static TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMP_FOLDER.getRoot().getAbsolutePath())
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that verify an exact wait time
            .build();
    }

    @Test
    public void testQueryAllColumns() throws Exception {
        execute("create table tbl (id int primary key) ");
        Object[][] bulkArgs = new Object[10][];
        for (int i = 0; i < 10; i++) {
            bulkArgs[i] = new Object[] { i };
        }
        execute("insert into tbl (id) values (?)", bulkArgs);
        execute("refresh table tbl");

        ThreadPool threadPool = cluster().getInstance(ThreadPool.class);
        execute(
            "CREATE REPOSITORY r1 TYPE fs WITH (location = ?, compress = true)",
            new Object[] { TEMP_FOLDER.newFolder("backup_s1").getAbsolutePath() });
        long createdTime = threadPool.absoluteTimeInMillis();
        execute("CREATE SNAPSHOT r1.s1 TABLE tbl WITH (wait_for_completion = true)");
        long finishedTime = threadPool.absoluteTimeInMillis();

        execute("select * from sys.snapshots");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), arrayContaining("concrete_indices", "failures", "finished", "name", "repository", "started", "state", "table_partitions", "tables", "version"));
        ArrayType<String> stringArray = new ArrayType<>(DataTypes.STRING);
        assertThat(response.columnTypes(), arrayContaining(
            stringArray,
            stringArray,
            TimestampType.INSTANCE_WITH_TZ,
            StringType.INSTANCE,
            StringType.INSTANCE,
            TimestampType.INSTANCE_WITH_TZ,
            StringType.INSTANCE,
            new ArrayType<>(ObjectType.builder()
                .setInnerType("values", stringArray)
                .setInnerType("table_schema", StringType.INSTANCE)
                .setInnerType("table_name", StringType.INSTANCE)
                .build()
            ),
            stringArray,
            StringType.INSTANCE
        ));
        Object[] firstRow = response.rows()[0];
        assertThat((List<Object>) firstRow[0], Matchers.contains(getFqn("tbl")));
        assertThat((List<Object>) firstRow[1], Matchers.empty());
        assertThat((Long) firstRow[2], lessThanOrEqualTo(finishedTime));
        assertThat(firstRow[3], is("s1"));
        assertThat(firstRow[4], is("r1"));
        assertThat((Long) firstRow[5], greaterThanOrEqualTo(createdTime));
        assertThat(firstRow[6], is(SnapshotState.SUCCESS.name()));
        assertThat((List<Object>) firstRow[7], Matchers.empty());
        assertThat((List<Object>) firstRow[8], Matchers.contains(getFqn("tbl")));
        assertThat(firstRow[9], is(Version.CURRENT.toString()));
    }

    @Test
    public void test_sys_snapshots_returns_table_partition_information() throws Exception {
        execute("create table tbl (x int, p int) clustered into 1 shards partitioned by (p)");
        execute("insert into tbl (x, p) values (1, 1), (2, 2)");
        execute("refresh table tbl");
        execute(
            "CREATE REPOSITORY r1 TYPE fs WITH (location = ?, compress = true)",
            new Object[] { TEMP_FOLDER.newFolder("backup_s2").getAbsolutePath() });
        execute("CREATE SNAPSHOT r1.s2 TABLE tbl WITH (wait_for_completion = true)");

        execute("select x['table_name'], unnest(x['values']::string[]) "
            + "from (select unnest(table_partitions) from sys.snapshots) t (x) order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "tbl| 1\n" +
            "tbl| 2\n"
        ));
    }
}
