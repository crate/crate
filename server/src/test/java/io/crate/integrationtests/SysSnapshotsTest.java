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

import static io.crate.testing.Asserts.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.sql.tree.ColumnPolicy;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedSchema;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
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
    @SuppressWarnings("unchecked")
    public void testQueryAllColumns() throws Exception {
        execute("create table tbl (id int primary key) clustered into 4 shards");
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
        assertThat(response).hasRowCount(1);
        assertThat(response.cols()).containsExactly(
            "concrete_indices", "failures", "finished", "id", "include_global_state", "name", "reason", "relations",
            "repository", "started", "state", "table_partitions", "tables", "total_shards", "version");
        ArrayType<String> stringArray = new ArrayType<>(DataTypes.STRING);
        assertThat(response.columnTypes()).containsExactly(
            stringArray,
            stringArray,
            TimestampType.INSTANCE_WITH_TZ,
            StringType.INSTANCE,
            BooleanType.INSTANCE,
            StringType.INSTANCE,
            StringType.INSTANCE,
            new ArrayType<>(ObjectType.of(ColumnPolicy.DYNAMIC)
                .setInnerType("table_schema", StringType.INSTANCE)
                .setInnerType("table_name", StringType.INSTANCE)
                .build()
            ),
            StringType.INSTANCE,
            TimestampType.INSTANCE_WITH_TZ,
            StringType.INSTANCE,
            new ArrayType<>(ObjectType.of(ColumnPolicy.DYNAMIC)
                .setInnerType("values", stringArray)
                .setInnerType("table_schema", StringType.INSTANCE)
                .setInnerType("table_name", StringType.INSTANCE)
                .build()
            ),
            stringArray,
            IntegerType.INSTANCE,
            StringType.INSTANCE
        );
        Object[] firstRow = response.rows()[0];
        assertThat((List<Object>) firstRow[0]).containsExactly(getFqn("tbl"));
        assertThat((List<Object>) firstRow[1]).isEmpty();
        assertThat((Long) firstRow[2]).isLessThanOrEqualTo(finishedTime);
        // firstRow[3] is UUID, not selecting it as it's random.
        assertThat((boolean) firstRow[4]).isFalse();
        assertThat(firstRow[5]).isEqualTo("s1");
        assertThat(firstRow[6]).isNull();
        List<Map<String, String>> tableRelations = (List<Map<String, String>>) firstRow[7];
        assertThat(tableRelations).hasSize(1);
        assertThat(tableRelations.getFirst()).hasEntrySatisfying("table_schema", o -> assertThat(o).isNotEmpty());
        assertThat(tableRelations.getFirst()).hasEntrySatisfying("table_name", o -> assertThat(o).isEqualTo("tbl"));
        assertThat(firstRow[8]).isEqualTo("r1");
        assertThat((Long) firstRow[9]).isGreaterThanOrEqualTo(createdTime);
        assertThat(firstRow[10]).isEqualTo(SnapshotState.SUCCESS.name());
        assertThat((List<Object>) firstRow[11]).isEmpty();
        assertThat((List<Object>) firstRow[12]).containsExactly(getFqn("tbl"));
        assertThat((int) firstRow[13]).isEqualTo(4);
        assertThat(firstRow[14]).isEqualTo(Version.CURRENT.toString());
    }

    @Test
    public void test_sys_snapshots_returns_table_partition_information() throws Exception {
        execute("create table tbl (x int, p int) clustered into 1 shards partitioned by (p)");
        execute("insert into tbl (x, p) values (1, 1), (2, 2)");
        execute("refresh table tbl");
        execute("CREATE REPOSITORY r1 TYPE fs WITH (location = ?, compress = true)",
                new Object[] { TEMP_FOLDER.newFolder().getAbsolutePath() });
        execute("CREATE SNAPSHOT r1.s2 TABLE tbl WITH (wait_for_completion = true)");

        execute("select x['table_name'], unnest(x['values']::string[]) "
            + "from (select unnest(table_partitions) from sys.snapshots) t (x) order by 2");
        assertThat(response).hasRows(
            "tbl| 1",
            "tbl| 2"
        );
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void test_sys_snapshots_lists_empty_partitioned_table() throws Exception {
        execute("create table tbl_empty (x int, p int) clustered into 1 shards partitioned by (p)");
        execute("create table tbl (x int, p int) clustered into 1 shards partitioned by (p)");
        execute("insert into tbl (x, p) values (1, 1), (2, 2)");
        execute("refresh table tbl");
        execute("CREATE REPOSITORY r1 TYPE fs WITH (location = ?, compress = true)",
                new Object[] { TEMP_FOLDER.newFolder().getAbsolutePath() });
        execute("CREATE SNAPSHOT r1.s2 ALL WITH (wait_for_completion = true)");

        execute("select x from (select unnest(tables) from sys.snapshots) t (x) order by 1");
        assertThat(response).hasRows(
            "doc.tbl",
            "doc.tbl_empty");
    }
}
