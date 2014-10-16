/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLResponse;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.junit.Before;
import org.junit.Test;


@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class InformationSchemaQueryTest extends SQLTransportIntegrationTest {

    private boolean createdTables = false;
    private SQLResponse response;

    @Before
    public void tableCreation() throws Exception {
        synchronized (InformationSchemaQueryTest.class) {
            if (!createdTables) {

                execute("create table t1 (col1 integer, col2 string) clustered into 7 shards");
                execute("create table t2 (col1 integer, col2 string) clustered into 10 shards");
                execute("create table t3 (col1 integer, col2 string) with (number_of_replicas=8)");

                createdTables = true;
            }
        }
    }

    public SQLResponse exec(String stmt) {
        response = execute(stmt);
        return response;
    }

    @Test
    public void testGroupByOnInformationSchema() throws Exception {
        exec("select count(*) from information_schema.columns where schema_name = 'doc' group by table_name order by count(*) desc");
        assertEquals(3L, response.rowCount());

        exec("select count(*) from information_schema.columns where schema_name = 'doc' group by column_name order by count(*) desc");
        assertEquals(2L, response.rowCount());
        assertEquals(3L, response.rows()[0][0]);
    }

    @Test
    public void testSelectStar() throws Exception {
        exec("select * from information_schema.tables where schema_name = 'doc'");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testLike() throws Exception {
        exec("select * from information_schema.tables where schema_name = 'doc' and table_name like 't%'");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testIsNull() throws Exception {
        exec("select * from information_schema.tables where table_name is null");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testIsNotNull() throws Exception {
        exec("select * from information_schema.tables where table_name is not null and schema_name = 'doc'");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testWhereAnd() throws Exception {
        exec("select table_name from information_schema.tables where table_name='t1' and " +
                "number_of_shards > 0");
        assertEquals(1L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
    }

    @Test
    public void testWhereAnd2() throws Exception {
        exec("select table_name from information_schema.tables where number_of_shards >= 7 and " +
                "number_of_replicas != '8' order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
    }

    @Test
    public void testWhereAnd3() throws Exception {
        exec("select table_name from information_schema.tables where table_name is not null and " +
                "number_of_shards > 6 order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
    }

    @Test
    public void testWhereOr() throws Exception {
        exec("select table_name from information_schema.tables where table_name='t1' or table_name='t3' " +
                "order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
        assertEquals("t3", response.rows()[1][0]);
    }

    @Test
    public void testWhereOr2() throws Exception {
        exec("select table_name from information_schema.tables where table_name='t1' or table_name='t3' " +
                "or table_name='t2'" +
                "order by table_name desc");
        assertEquals(3L, response.rowCount());
        assertEquals("t3", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
        assertEquals("t1", response.rows()[2][0]);
    }

    @Test
    public void testWhereIn() throws Exception {
        exec("select table_name from information_schema.tables where table_name in ('t1', 't2') order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
    }

    @Test
    public void testNotEqualsString() throws Exception {
        exec("select table_name from information_schema.tables where schema_name = 'doc' and table_name != 't1'");
        assertEquals(2L, response.rowCount());
        assertTrue(!response.rows()[0][0].equals("t1"));
        assertTrue(!response.rows()[1][0].equals("t1"));
    }

    @Test
    public void testNotEqualsNumber() throws Exception {
        exec("select table_name, number_of_shards from information_schema.tables where schema_name = 'doc' and number_of_shards != 7");
        assertEquals(2L, response.rowCount());
        assertTrue(response.rows()[0][1] != 7);
        assertTrue(response.rows()[1][1] != 7);
    }

    @Test
    public void testEqualsNumber() throws Exception {
        exec("select table_name from information_schema.tables where schema_name = 'doc' and number_of_shards = 7");
        assertEquals(1L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
    }

    @Test
    public void testEqualsString() throws Exception {
        exec("select table_name from information_schema.tables where table_name = 't1'");
        assertEquals(1L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
    }

    @Test
    public void testGtNumber() throws Exception {
        exec("select table_name from information_schema.tables where number_of_shards > 7");
        assertEquals(1L, response.rowCount());
        assertEquals("t2", response.rows()[0][0]);
    }

    @Test
    public void testOrderByStringAndLimit() throws Exception {
        exec("select table_name, number_of_shards, number_of_replicas from information_schema.tables " +
                 " where schema_name = 'doc' order by table_name desc limit 2");
        assertEquals(2L, response.rowCount());
        assertEquals("t3", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
    }

    @Test
    public void testOrderByNumberAndLimit() throws Exception {
        exec("select table_name, number_of_shards, number_of_replicas from information_schema.tables " +
                 " order by number_of_shards desc limit 2");
        assertEquals(2L, response.rowCount());
        assertEquals(10, response.rows()[0][1]);
        assertEquals("t2", response.rows()[0][0]);
        assertEquals(7, response.rows()[1][1]);
        assertEquals("t1", response.rows()[1][0]);
    }

    @Test
    public void testLimit1() throws Exception {
        exec("select * from information_schema.tables limit 1");
        assertEquals(1L, response.rowCount());
    }

    @Test( expected = SQLActionException.class )
    public void testSelectUnkownTableFromInformationSchema() throws Exception {
        exec("select * from information_schema.non_existent");
    }

    @Test
    public void testSysShards() throws Exception {
        exec("select * from sys.shards");
        assertEquals(79L, response.rowCount());
    }

    @Test
    public void testIgnoreClosedTables() throws Exception {
        execute("drop table t1");
        execute("drop table t2");
        execute("create table t1 (col1 integer, col2 string) with (number_of_replicas=0)");
        client().admin().indices().close(new CloseIndexRequest("t3"));
        ensureGreen();
        exec("select * from information_schema.tables where schema_name = 'doc'");
        assertEquals(1L, response.rowCount());
        exec("select * from information_schema.columns where table_name = 't3'");
        assertEquals(0, response.rowCount());

        exec("select * from sys.shards");
        assertEquals(5L, response.rowCount()); // t3 isn't included
    }
}
