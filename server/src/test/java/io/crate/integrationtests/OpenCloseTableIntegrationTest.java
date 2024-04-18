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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.BAD_REQUEST;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.Asserts;
import io.crate.testing.UseRandomizedSchema;

public class OpenCloseTableIntegrationTest extends IntegTestCase {

    @Before
    public void prepareClosedTable() {
        execute("create table t (i int)");
        ensureYellow();
        execute("alter table t close");
    }

    @Test
    public void test_open_missing_table() {
        Asserts.assertSQLError(() -> execute("alter table test open"))
                .hasPGError(UNDEFINED_TABLE)
                .hasHTTPError(NOT_FOUND, 4041)
                .hasMessageContaining("Relation 'test' unknown");
    }

    @Test
    public void test_open_already_opened_index() {
        execute("alter table t open");
        execute("alter table t open");
    }

    @Test
    public void test_simple_close_open_with_records() {
        execute("alter table t open");
        execute("insert into t values (1), (2)");
        refresh();
        execute("select * from t");
        assertThat(response).hasRowCount(2L);

        execute("alter table t close");
        execute("alter table t open");
        ensureGreen();

        execute("select * from t");
        assertThat(response).hasRowCount(2L);
    }

    @Test
    public void test_get_translog_stats_after_close_and_open_table() throws Exception {
        execute("create table test (x int ) with (number_of_replicas = 0) ");
        long numberOfDocs = randomIntBetween(0, 50);
        long uncommittedOps = 0;
        for (long i = 0; i < numberOfDocs; i++) {
            execute("insert into test values (?)", new Object[]{i});
            if (rarely()) {
                execute("optimize table test with (flush = true)");
                uncommittedOps = 0;
            } else {
                uncommittedOps += 1;
            }
        }
        final long uncommittedTranslogOps = uncommittedOps;
        assertBusy(() -> {
            execute(
                "select sum(translog_stats['number_of_operations']), sum(translog_stats['uncommitted_operations']) " +
                "from sys.shards " +
                "where table_name = 'test' and primary = true " +
                "group by table_name, primary");
            assertThat(response.rowCount()).isGreaterThan(0L);
            assertThat(response.rows()[0][0]).isEqualTo(uncommittedTranslogOps);
            assertThat(response.rows()[0][1]).isEqualTo(uncommittedTranslogOps);
        });

        execute("alter table test close");
        execute("alter table test open");
        ensureYellow();

        execute(
            "select sum(translog_stats['number_of_operations']), sum(translog_stats['uncommitted_operations']) " +
            "from sys.shards " +
            "where table_name = 'test' and primary = true " +
            "group by table_name, primary");
        assertThat(response.rowCount()).isGreaterThan(0);
        assertThat(response.rows()[0][0]).isEqualTo(0L);
        assertThat(response.rows()[0][1]).isEqualTo(0L);
    }

    @Test
    public void test_open_close_is_not_blocked_with_read_and_write_blocks_enabled() {
        execute("create table test (x int) with (number_of_replicas = 0) ");
        int bulkSize = randomIntBetween(10, 20);
        Object[][] bulkArgs = new Object[bulkSize][];
        for (int i = 0; i < bulkSize; i++) {
            bulkArgs[i] = new Object[]{i};
        }
        execute("insert into test values (?)", bulkArgs);
        refresh();

        for (String blockSetting : List.of("blocks.read", "blocks.write")) {
            try {
                execute("alter table test set (\"" + blockSetting + "\" = true)");

                // Closing an index is not blocked
                execute("alter table test close");
                assertThat(isClosed("test")).isTrue();

                // Opening an index is not blocked
                execute("alter table test open");
                ensureYellow();
                assertThat(isClosed("test")).isFalse();
            } finally {
                execute("alter table test reset (\"" + blockSetting + "\")");
            }
        }
    }

    @Test
    public void test_close_is_blocked_with_read_only_and_metadata_blocks_enabled() {
        execute("create table test (x int) with (number_of_replicas = 0) ");
        int bulkSize = randomIntBetween(10, 20);
        Object[][] bulkArgs = new Object[bulkSize][];
        for (int i = 0; i < bulkSize; i++) {
            bulkArgs[i] = new Object[]{i};
        }
        execute("insert into test values (?)", bulkArgs);
        refresh();

        for (String blockSetting : List.of("blocks.read_only", "blocks.metadata")) {
            try {
                execute("alter table test set (\"" + blockSetting + "\" = true)");
                execute("alter table test close");
            } catch (Exception e) {
                assertThat(isClosed("test")).isFalse();
            } finally {
                execute("alter table test reset (\"" + blockSetting + "\")");
            }
        }
    }

    @Test
    public void test_open_is_blocked_with_read_and_write_blocks_enabled() {
        execute("create table test (x int) with (number_of_replicas = 0) ");
        int bulkSize = randomIntBetween(10, 20);
        Object[][] bulkArgs = new Object[bulkSize][];
        for (int i = 0; i < bulkSize; i++) {
            bulkArgs[i] = new Object[]{i};
        }
        execute("insert into test values (?)", bulkArgs);
        refresh();

        for (String blockSetting : List.of(
            "blocks.read_only",
            "blocks.metadata",
            "blocks.read_only_allow_delete"
        )) {
            try {
                execute("alter table test set (\"" + blockSetting + "\" = true)");
                execute("alter table test close");
            } catch (Exception e) {
                assertThat(isClosed("test")).isFalse();
            } finally {
                execute("alter table test reset (\"" + blockSetting + "\")");
            }
        }
    }

    private boolean isClosed(String table) {
        execute(
            "select closed " +
            "from information_schema.tables " +
            "where table_name = ?", new Object[]{table});
        return (boolean) response.rows()[0][0];
    }

    @Test
    public void testOpenCloseTable() throws Exception {
        execute("select closed from information_schema.tables where table_name = 't'");
        assertEquals(1, response.rowCount());
        assertEquals(true, response.rows()[0][0]);

        IndexMetadata indexMetadata = client().admin().cluster().state(new ClusterStateRequest()).get().getState().metadata()
            .indices().get(getFqn("t"));
        assertEquals(IndexMetadata.State.CLOSE, indexMetadata.getState());

        execute("alter table t open");

        indexMetadata = client().admin().cluster().state(new ClusterStateRequest()).get().getState().metadata()
            .indices().get(getFqn("t"));

        execute("select closed from information_schema.tables where table_name = 't'");
        assertEquals(1, response.rowCount());
        assertEquals(false, response.rows()[0][0]);
        assertEquals(IndexMetadata.State.OPEN, indexMetadata.getState());
    }

    @Test
    public void testClosePreventsInsert() throws Exception {
        Asserts.assertSQLError(() -> execute("insert into t values (1), (2), (3)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format("The relation \"%s\" doesn't support or allow INSERT operations," +
                                                       " as it is currently closed.", getFqn("t")));
    }

    @Test
    public void testClosePreventsSelect() throws Exception {
        Asserts.assertSQLError(() -> execute("select * from t"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format("The relation \"%s\" doesn't support or allow READ operations, " +
                                                       "as it is currently closed.", getFqn("t")));
    }

    @Test
    public void testClosePreventsDrop() throws Exception {
        Asserts.assertSQLError(() -> execute("drop table t"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format("The relation \"%s\" doesn't support or allow DROP operations, " +
                                                       "as it is currently closed.", getFqn("t")));
    }

    @Test
    public void testClosePreventsRefresh() throws Exception {
        Asserts.assertSQLError(() -> execute("refresh table t"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format("The relation \"%s\" doesn't support or allow REFRESH operations, as " +
                                                "it is currently closed.", getFqn("t")));
    }

    @Test
    public void testClosePreventsShowCreate() throws Exception {
        Asserts.assertSQLError(() -> execute("show create table t"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format("The relation \"%s\" doesn't support or allow SHOW CREATE operations," +
                                                " as it is currently closed.", getFqn("t")));
    }

    @Test
    public void testClosePreventsOptimize() throws Exception {
        Asserts.assertSQLError(() -> execute("optimize table t"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format("The relation \"%s\" doesn't support or allow OPTIMIZE operations, " +
                                                       "as it is currently closed.", getFqn("t")));
    }

    @Test
    public void test_select_partitioned_table_containing_closed_partition() throws Exception {
        execute("create table partitioned_table (i int) partitioned by (i)");
        execute("insert into partitioned_table values (1), (2), (3), (4), (5)");
        refresh();
        ensureGreen(); // index must be active to be included in close
        execute("alter table partitioned_table partition (i=1) close");
        execute("select i from partitioned_table");
        assertEquals(4, response.rowCount());
        execute("select i from partitioned_table where i = 1");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testSelectClosedPartitionTable() throws Exception {
        execute("create table partitioned_table (i int) partitioned by (i)");
        execute("insert into partitioned_table values (1), (2), (3), (4), (5)");
        refresh();
        ensureGreen(); // index must be active to be included in close
        execute("alter table partitioned_table close");
        Asserts.assertSQLError(() -> execute("select i from partitioned_table"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format("The relation \"%s\" doesn't support or allow READ operations, " +
                                                       "as it is currently closed.", getFqn("partitioned_table")));
    }

    @Test
    public void test_close_open_empty_partitioned_table() {
        execute("create table partitioned_table (i int) partitioned by (i)");
        execute("alter table partitioned_table close");
        assertThat(isClosed("partitioned_table")).isTrue();
        execute("alter table partitioned_table open");
        assertThat(isClosed("partitioned_table")).isFalse();
    }

    @Test
    public void test_insert_into_closed_empty_partitioned_table_is_prevented() {
        execute("create table partitioned_table (i int) partitioned by (i)");
        execute("alter table partitioned_table close");
        Asserts.assertSQLError(() -> execute("insert into partitioned_table values (1)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format("The relation \"%s\" doesn't support or allow INSERT operations," +
                                                              " as it is currently closed.", getFqn("partitioned_table")));
    }

    @UseRandomizedSchema(random = false)
    public void test_auto_expand_closed_tables() throws IOException {
        execute("create table test(a int) clustered into 6 shards with (number_of_replicas = '0-all')");
        ensureGreen();

        execute("alter table test close");
        ensureGreen();

        allowNodes("test", 3);
        ensureGreen();

        execute("select count(*), primary from sys.shards where table_name = 'test' group by 2 order by 2 ");
        // Used to be "6| true", ie no replicas were created after closing table and re-balancing a cluster.
        assertThat(response).hasRows("12| false", "6| true");
    }
}
