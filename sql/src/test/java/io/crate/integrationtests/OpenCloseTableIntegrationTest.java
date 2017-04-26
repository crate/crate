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

import io.crate.action.sql.SQLActionException;
import io.crate.testing.UseJdbc;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.junit.Before;
import org.junit.Test;

@UseJdbc
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

        IndexMetaData indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData()
            .indices().get("t");
        assertEquals(IndexMetaData.State.CLOSE, indexMetaData.getState());

        execute("alter table t open");

        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData()
            .indices().get("t");

        execute("select closed from information_schema.tables where table_name = 't'");
        assertEquals(1, response.rowCount());
        assertEquals(false, response.rows()[0][0]);
        assertEquals(IndexMetaData.State.OPEN, indexMetaData.getState());
    }

    @Test
    public void testClosePreventsInsert() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t\" doesn't support or allow INSERT operations, " +
                                        "as it is currently closed");
        execute("insert into t values (1), (2), (3)");
    }

    @Test
    public void testClosePreventsSelect() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t\" doesn't support or allow READ operations, " +
                                        "as it is currently closed.");
        execute("select * from t");
    }

    @Test
    public void testClosePreventsDrop() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t\" doesn't support or allow DROP operations, " +
                                        "as it is currently closed");
        execute("drop table t");
    }

    @Test
    public void testClosePreventsAlter() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t\" doesn't support or allow ALTER operations, " +
                                        "as it is currently closed");
        execute("alter table t add column x string");
    }

    @Test
    public void testClosePreventsRefresh() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t\" doesn't support or allow REFRESH " +
                                        "operations, as it is currently closed.");
        execute("refresh table t");
    }

    @Test
    public void testClosePreventsShowCreate() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t\" doesn't support or allow SHOW CREATE " +
                                        "operations, as it is currently closed.");
        execute("show create table t");
    }

    @Test
    public void testClosePreventsOptimize() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t\" doesn't support or allow OPTIMIZE " +
                                        "operations, as it is currently closed.");
        execute("optimize table t");
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
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.partitioned_table\" doesn't support or allow " +
                                        "READ operations, as it is currently closed.");
        execute("select i from partitioned_table");
    }

}
