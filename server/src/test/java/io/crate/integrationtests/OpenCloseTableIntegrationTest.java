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

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.BAD_REQUEST;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class OpenCloseTableIntegrationTest extends SQLIntegrationTestCase {

    @Before
    public void prepareClosedTable() {
        execute("create table t (i int)");
        ensureYellow();
        execute("alter table t close");
    }

    @Test
    public void test_open_missing_table() {
        assertThrows(
            () -> execute("alter table test open"),
            isSQLError(
                is("Relation 'test' unknown"),
                UNDEFINED_TABLE,
                NOT_FOUND,
                4041
            )
        );
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
        assertThat(response.rowCount(), is(2L));

        execute("alter table t close");
        execute("alter table t open");
        ensureGreen();

        execute("select * from t");
        assertThat(response.rowCount(), is(2L));
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
            assertThat(response.rowCount(), greaterThan(0L));
            assertThat(response.rows()[0][0], is(uncommittedTranslogOps));
            assertThat(response.rows()[0][1], is(uncommittedTranslogOps));
        });

        execute("alter table test close");
        execute("alter table test open");
        ensureYellow();

        execute(
            "select sum(translog_stats['number_of_operations']), sum(translog_stats['uncommitted_operations']) " +
            "from sys.shards " +
            "where table_name = 'test' and primary = true " +
            "group by table_name, primary");
        assertThat(response.rowCount(), greaterThan(0L));
        assertThat(response.rows()[0][0], is(0L));
        assertThat(response.rows()[0][1], is(0L));
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
                assertThat(isClosed("test"), is(true));

                // Opening an index is not blocked
                execute("alter table test open");
                ensureYellow();
                assertThat(isClosed("test"), is(false));
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
                assertThat(isClosed("test"), is(false));
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
                assertThat(isClosed("test"), is(false));
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

        IndexMetadata indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata()
            .indices().get(getFqn("t"));
        assertEquals(IndexMetadata.State.CLOSE, indexMetadata.getState());

        execute("alter table t open");

        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata()
            .indices().get(getFqn("t"));

        execute("select closed from information_schema.tables where table_name = 't'");
        assertEquals(1, response.rowCount());
        assertEquals(false, response.rows()[0][0]);
        assertEquals(IndexMetadata.State.OPEN, indexMetadata.getState());
    }

    @Test
    public void testClosePreventsInsert() throws Exception {
        assertThrows(() -> execute("insert into t values (1), (2), (3)"),
                     isSQLError(is(String.format("The relation \"%s\" doesn't support or allow INSERT operations," +
                                                 " as it is currently closed.", getFqn("t"))),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4007));
    }

    @Test
    public void testClosePreventsSelect() throws Exception {
        assertThrows(() -> execute("select * from t"),
                     isSQLError(is(String.format("The relation \"%s\" doesn't support or allow READ operations, " +
                                                 "as it is currently closed.", getFqn("t"))),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4007));
    }

    @Test
    public void testClosePreventsDrop() throws Exception {
        assertThrows(() -> execute("drop table t"),
                     isSQLError(is(String.format("The relation \"%s\" doesn't support or allow DROP operations, " +
                                                 "as it is currently closed.", getFqn("t"))),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4007));
    }

    @Test
    public void testClosePreventsAlter() throws Exception {
        assertThrows(() -> execute("alter table t add column x string"),
                     isSQLError(is(String.format("The relation \"%s\" doesn't support or allow ALTER operations, " +
                                                 "as it is currently closed.", getFqn("t"))),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4007));
    }

    @Test
    public void testClosePreventsRefresh() throws Exception {
        assertThrows(() -> execute("refresh table t"),
                     isSQLError(is(String.format("The relation \"%s\" doesn't support or allow REFRESH operations, as " +
                                          "it is currently closed.", getFqn("t"))),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4007));
    }

    @Test
    public void testClosePreventsShowCreate() throws Exception {
        assertThrows(() -> execute("show create table t"),
                     isSQLError(is(String.format("The relation \"%s\" doesn't support or allow SHOW CREATE operations," +
                                          " as it is currently closed.", getFqn("t"))),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4007));
    }

    @Test
    public void testClosePreventsOptimize() throws Exception {
        assertThrows(() -> execute("optimize table t"),
                     isSQLError(is(String.format("The relation \"%s\" doesn't support or allow OPTIMIZE operations, " +
                                                 "as it is currently closed.", getFqn("t"))),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4007));
    }

    @Test
    public void testSelectPartitionedTableWhilePartitionIsClosed() throws Exception {
        execute("create table partitioned_table (i int) partitioned by (i)");
        ensureYellow();
        execute("insert into partitioned_table values (1), (2), (3), (4), (5)");
        refresh();
        execute("alter table partitioned_table partition (i=1) close");
        execute("select i from partitioned_table");
        assertEquals(4, response.rowCount());
        execute("select i from partitioned_table where i = 1");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testSelectClosedPartitionTable() throws Exception {
        execute("create table partitioned_table (i int) partitioned by (i)");
        ensureYellow();
        execute("insert into partitioned_table values (1), (2), (3), (4), (5)");
        refresh();
        execute("alter table partitioned_table close");
        assertThrows(() -> execute("select i from partitioned_table"),
                     isSQLError(is(String.format("The relation \"%s\" doesn't support or allow READ operations, " +
                                                 "as it is currently closed.", getFqn("partitioned_table"))),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4007));
    }
}
