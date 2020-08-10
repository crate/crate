/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.junit.Before;
import org.junit.Test;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.BAD_REQUEST;
import static org.hamcrest.Matchers.is;

public class OpenCloseTableIntegrationTest extends SQLTransportIntegrationTest {

    @Before
    public void prepareClosedTable() {
        execute("create table t (i int)");
        ensureYellow();
        execute("alter table t close");
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
