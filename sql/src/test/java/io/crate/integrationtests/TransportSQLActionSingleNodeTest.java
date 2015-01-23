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


import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class TransportSQLActionSingleNodeTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testUnassignedShards() throws Exception {
        int numReplicas = 3;
        int numShards = 5;
        long expectedUnassignedShards = numShards * numReplicas; // calculation is correct for cluster.numNodes = 1

        execute("create table locations (id integer primary key, name string) " +
                "clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas + ")");
        ensureYellow();

        execute("select count(*) from sys.shards where table_name = 'locations' and state = 'UNASSIGNED'");
        assertThat(response.rowCount(), is(1L));
        assertEquals(1, response.cols().length);
        assertThat((Long) response.rows()[0][0], is(expectedUnassignedShards));
    }

    @Test
    public void testPrimarySecondaryUnassignedShards() throws Exception {
        int numReplicas = 3;
        int numShards = 5;

        execute("create table locations (id integer primary key, name string) " +
                "clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas + ")");
        ensureYellow();

        execute("select \"primary\", state, count(*) from sys.shards where table_name = 'locations' group by \"primary\", state order by \"primary\"");
        assertThat(response.rowCount(), is(2L));
        assertEquals(new Boolean(false), response.rows()[0][0]);
        assertEquals("UNASSIGNED", response.rows()[0][1]);
        assertEquals(15L, response.rows()[0][2]);
        assertEquals(new Boolean(true), response.rows()[1][0]);
        assertEquals("STARTED", response.rows()[1][1]);
        assertEquals(5L, response.rows()[1][2]);
    }

    @Test
    public void testPrimarySecondaryUnassignedShardsWithPartitions() throws Exception {
        int numReplicas = 1;
        int numShards = 5;

        execute("create table locations (id integer primary key, name string) " +
                "partitioned by (id) clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas + ")");
        execute("insert into locations (id, name) values (1, 'name1')");
        execute("insert into locations (id, name) values (2, 'name2')");
        refresh();
        ensureYellow();

        execute("select \"primary\", state, count(*) from sys.shards where table_name = 'locations' group by \"primary\", state order by \"primary\"");
        assertThat(response.rowCount(), is(2L));
        assertEquals(new Boolean(false), response.rows()[0][0]);
        assertEquals("UNASSIGNED", response.rows()[0][1]);
        assertEquals(10L, response.rows()[0][2]);
        assertEquals(new Boolean(true), response.rows()[1][0]);
        assertEquals("STARTED", response.rows()[1][1]);
        assertEquals(10L, response.rows()[1][2]);
    }

    @Test
    public void testPartitionIdentPrimarySecondaryUnassignedShards() throws Exception {
        int numReplicas = 1;
        int numShards = 5;

        execute("create table locations (id integer primary key, name string) " +
                "partitioned by (id) clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas + ")");
        execute("insert into locations (id, name) values (1, 'name1')");
        execute("insert into locations (id, name) values (2, 'name2')");
        refresh();
        ensureYellow();

        execute("select table_name, partition_ident, state from sys.shards where table_name = 'locations' " +
                "group by table_name, partition_ident, state order by partition_ident, state");
        assertThat(response.rowCount(), is(4L));

        String expected = "locations| 04132| STARTED\n" +
                "locations| 04132| UNASSIGNED\n" +
                "locations| 04134| STARTED\n" +
                "locations| 04134| UNASSIGNED\n";

       assertEquals(expected, TestingHelpers.printedTable(response.rows()));
    }

    @Test
    public void testSelectNoResultWithTypes() throws Exception {
        assertResponseWithTypes("select * from sys.nodes where 1=0");
    }

    @Test
    public void testInsertBulkWithTypes() throws Exception {
        execute("create table bla1 (id integer primary key, name string) " +
                "clustered into 2 shards with (number_of_replicas=0)");
        ensureGreen();

        assertBulkResponseWithTypes("insert into bla1 (id, name) values (?, ?)",
                new Object[][]{
                        new Object[]{1, "Ford"},
                        new Object[]{2, "Trillian"}
                });
    }

    private void assertBulkResponseWithTypes(String stmt, Object[][] bulkArgs) {
        SQLBulkRequest request = new SQLBulkRequest(stmt, bulkArgs);
        request.includeTypesOnResponse(true);
        SQLBulkResponse sqlResponse = sqlExecutor.exec(request);
        assertThat(sqlResponse.columnTypes(), is(notNullValue()));
        assertThat(sqlResponse.columnTypes().length, is(sqlResponse.cols().length));
    }

    @Test
    public void testSelectUnknownNoResultWithTypes() throws Exception {
        execute("create table unknown (id integer primary key, name object(ignored)) " +
                "clustered into 2 shards with (number_of_replicas=0)");
        ensureGreen();

        assertResponseWithTypes("select name['bla'] from unknown where 1=0");
    }

    @Test
    public void testDMLStatementsWithTypes() throws Exception {
        execute("create table bla (id integer primary key, name string) " +
                "clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        assertResponseWithTypes("insert into bla (id, name) (select 4, 'Trillian' from sys.cluster)");
        assertResponseWithTypes("insert into bla (id, name) values (1, 'Ford')");
        assertResponseWithTypes("update bla set name='Arthur' where name ='Ford'");
        assertResponseWithTypes("update bla set name='Arthur' where id=1");
        assertResponseWithTypes("delete from bla where id=1");
        assertResponseWithTypes("delete from bla where name='Ford'");

    }

    @Test
    public void testDDLStatementsWithTypes() throws Exception {
        assertResponseWithTypes("create table bla2 (id integer primary key, name string) " +
                "clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        assertResponseWithTypes("alter table bla2 add column blubb string");
        assertResponseWithTypes("refresh table bla2");
        assertResponseWithTypes("drop table bla2");
        assertResponseWithTypes("create blob table blablob2 clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        assertResponseWithTypes("drop blob table blablob2");
        assertResponseWithTypes("create ANALYZER \"german_snowball\" extends snowball WITH (language='german')");
    }

    private void assertResponseWithTypes(String stmt) {
        SQLRequest request = new SQLRequest(stmt);
        request.includeTypesOnResponse(true);
        SQLResponse sqlResponse = sqlExecutor.exec(request);
        assertThat(sqlResponse.columnTypes(), is(notNullValue()));
        assertThat(sqlResponse.columnTypes().length, is(sqlResponse.cols().length));
    }

    @Test
    public void testSubscriptArray() throws Exception {
        execute("create table test (id integer primary key, names array(string)) " +
                "clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test (id, names) values (?, ?)",
                new Object[]{1, Arrays.asList("Arthur", "Ford")});
        refresh();
        execute("select names[1] from test");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Arthur"));
    }

    @Test
    public void testSubscriptArrayNesting() throws Exception {
        execute("create table test (id integer primary key, names array(object as (surname string))) " +
                "clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test (id, names) values (?, ?)",
                new Object[]{1, Arrays.asList(
                        new HashMap<String, String>(){{ put("surname", "Adams"); }}
                )});
        refresh();
        execute("select names[1]['surname'] from test");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Adams"));
    }

    @Test
    public void testSelectRegexpMatchesGroup() throws Exception {
        execute("create table test (id integer primary key, text string) " +
                "clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test (id, text) values (1, 'Time is an illusion')");
        refresh();
        execute("select regexp_matches(text, '(\\w+)\\s(\\w+)')[1] as first_word," +
                "regexp_matches(text, '(\\w+)\\s(\\w+)')[2] as matched from test order by first_word");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Time"));
        assertThat((String) response.rows()[0][1], is("is"));
    }
}
