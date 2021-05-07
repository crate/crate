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

import io.crate.execution.dsl.projection.AbstractIndexWriterProjection;
import io.crate.testing.UseJdbc;

import io.crate.common.unit.TimeValue;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class DeleteIntegrationTest extends SQLIntegrationTestCase {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void testDeleteTableWithoutWhere() throws Exception {
        execute("create table test (x int)");
        execute("insert into test (x) values (1)");
        execute("refresh table test");

        execute("delete from test");
        assertEquals(1, response.rowCount());

        execute("refresh table test");
        execute("select \"_id\" from test");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testDeleteOnPKNoMatch() throws Exception {
        execute("create table t (id string primary key)");
        execute("delete from t where id = 'nope'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testDeleteWithWhereDeletesCorrectRecord() throws Exception {
        execute("create table test (id int primary key)");
        execute("insert into test (id) values (1), (2), (3)");
        execute("refresh table test");

        execute("delete from test where id = 1");
        assertEquals(1, response.rowCount());

        execute("refresh table test");
        execute("select id from test");
        assertEquals(2, response.rowCount());
    }

    @Test
    public void testDeleteWhereIsNull() throws Exception {
        execute("create table test (id integer, name string) with (number_of_replicas=0)");
        execute("insert into test (id, name) values (1, 'foo')"); // name exists
        execute("insert into test (id, name) values (2, null)"); // name is null
        execute("insert into test (id) values (3)"); // name does not exist
        refresh();
        execute("delete from test where name is null");
        refresh();
        execute("select * from test");
        assertEquals(1, response.rowCount());
        execute("select * from test where name is not null");
        assertEquals(1, response.rowCount());
        execute("select * from test where name is null");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testDeleteWithNullArg() throws Exception {
        this.setup.createTestTableWithPrimaryKey();
        execute("insert into test(pk_col) values (1), (2), (3)");
        execute("refresh table test");

        execute("delete from test where pk_col=?", new Object[]{null});
        assertThat(response.rowCount(), is(0L));

        execute("refresh table test");
        execute("select pk_col FROM test");
        assertThat(response.rowCount(), is(3L));
    }

    @Test
    public void testDeleteByIdWithMultiplePrimaryKey() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, author, quote) values (?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day.",
                1, "Douglas", "Don't panic"}
        );
        assertEquals(2L, response.rowCount());
        refresh();

        execute("delete from quotes where id=1 and author='Ford'");
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select quote from quotes where id=1");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testDeleteByQueryWithMultiplePrimaryKey() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, author, quote) values (?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day.",
                1, "Douglas", "Don't panic"}
        );
        assertEquals(2L, response.rowCount());
        refresh();

        execute("delete from quotes where id=1");
        assertEquals(2L, response.rowCount());
        refresh();

        execute("select quote from quotes where id=1");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testDeleteOnIpType() throws Exception {
        execute("create table ip_table (fqdn string, addr ip) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into ip_table (fqdn, addr) values ('localhost', '127.0.0.1'), ('crate.io', '23.235.33.143')");
        execute("refresh table ip_table");
        execute("delete from ip_table where addr = '127.0.0.1'");
        assertThat(response.rowCount(), is(1L));
        refresh();
        execute("select addr from ip_table");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("23.235.33.143"));
    }

    @Test
    public void testDeleteToDeleteRequestByPlanner() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('123', 'bar')");
        assertEquals(1, response.rowCount());
        refresh();

        execute("delete from test where pk_col='123'");
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test where pk_col='123'");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testDeleteToRoutedRequestByPlannerWhereIn() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");
        refresh();

        execute("DELETE FROM test WHERE pk_col IN (?, ?, ?)", new Object[]{"1", "2", "4"});
        assertThat(response.rowCount(), is(2L));
        refresh();

        execute("SELECT pk_col FROM test");
        assertThat(response.rowCount(), is(1L));
        assertEquals(response.rows()[0][0], "3");

    }

    @Test
    public void testDeleteToRoutedRequestByPlannerWhereOr() throws Exception {
        this.setup.createTestTableWithPrimaryKey();
        execute("insert into test (pk_col, message) values ('1', 'foo'), ('2', 'bar'), ('3', 'baz')");
        refresh();
        execute("DELETE FROM test WHERE pk_col=? or pk_col=? or pk_col=?", new Object[]{"1", "2", "4"});
        assertThat(response.rowCount(), is(2L));
        refresh();
        execute("SELECT pk_col FROM test");
        assertThat(response.rowCount(), is(1L));
        assertEquals(response.rows()[0][0], "3");
    }

    @Test
    public void testBulkDeleteNullAndSingleKey() throws Exception {
        this.setup.createTestTableWithPrimaryKey();
        execute("insert into test(pk_col) values (1), (2), (3)");
        execute("refresh table test");

        long[] rowCounts = execute("delete from test where pk_col=?",
            new Object[][]{{2}, {null}, {3}});
        assertThat(rowCounts, is(new long[] { 1L, 0L, 1L }));

        rowCounts = execute("delete from test where pk_col=?", new Object[][]{{null}});
        assertThat(rowCounts, is(new long[] { 0L }));

        execute("refresh table test");
        execute("select pk_col FROM test");
        assertThat(response.rowCount(), is(1L));
        assertEquals(response.rows()[0][0], "1");
    }

    @Test
    public void testDeleteExceedingInternalDefaultBulkSize() throws Exception {
        execute("create table t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        Object[][] bulkArgs = new Object[AbstractIndexWriterProjection.BULK_SIZE_DEFAULT + 10][];
        for (int i = 0; i < bulkArgs.length; i++) {
            bulkArgs[i] = new Object[] { i };
        }
        execute("insert into t1 (x) values (?)", bulkArgs, TimeValue.timeValueMinutes(10));
        execute("refresh table t1");

        // there was an regression which caused this query to timeout
        execute("delete from t1");
        assertThat(response.rowCount(), is((long) bulkArgs.length));
    }

    @Test
    public void testDeleteWithSubQuery() throws Exception {
        execute("create table t1 (id int primary key, x int)");
        execute("insert into t1 (id, x) values (1, 1), (2, 2), (3, 3)");
        execute("refresh table t1");

        execute("delete from t1 where x = (select 1)");
        assertThat(response.rowCount(), is(1L));

        execute("delete from t1 where id = (select 2)");
        assertThat(response.rowCount(), is(1L));

        execute("delete from t1 where id in (select col1 from unnest([1, 2, 3]))");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    @UseJdbc(0) // Row-count of delete is 0 instead of -1
    public void testDeleteWithSubQueryOnPartition() throws Exception {
        execute("create table t (p string, x int) partitioned by (p)");
        execute("insert into t (p, x) values ('a', 1)");
        execute("refresh table t");

        execute("delete from t where p = (select 'a')");
        assertThat(response.rowCount(), is(-1L));

        execute("select count(*) from t");
        assertThat(response.rows()[0][0], is(0L));
    }

    @Test
    public void testDeleteFromAlias() {
        execute("create table t1 (id int primary key, x int)");
        execute("insert into t1 (id, x) values (1, 1), (2, 2), (3, 3)");
        execute("delete from t1 as foo where foo.id = 1");
    }

    @Test
    public void test_delete_partitions_from_subquery_does_not_leave_empty_orphan_partitions() {
        execute("CREATE TABLE t (x int) PARTITIONED by (x)");
        execute("INSERT INTO t (x) VALUES (1), (2)");
        refresh();
        execute(
            "SELECT count(1) " +
            "FROM information_schema.table_partitions " +
            "WHERE table_name = 't'"
        );
        assertThat(response.rows()[0][0], is(2L));

        execute("DELETE FROM t where x IN (SELECT DISTINCT(x) FROM t WHERE x > 1)");
        execute(
            "SELECT count(1) " +
            "FROM information_schema.table_partitions " +
            "WHERE table_name = 't'"
        );
        assertThat(response.rows()[0][0], is(1L));
    }
}
