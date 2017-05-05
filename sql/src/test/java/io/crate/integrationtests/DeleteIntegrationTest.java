/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import io.crate.testing.SQLBulkResponse;
import io.crate.testing.UseJdbc;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

@UseJdbc
public class DeleteIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void testDelete() throws Exception {
        createIndex("test");
        ensureYellow();
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        refresh();
        execute("delete from test");
        assertEquals(1, response.rowCount());
        refresh();
        execute("select \"_id\" from test");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testDeleteWithWhere() throws Exception {
        createIndex("test");
        ensureYellow();
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id3").setSource("{}").execute().actionGet();
        refresh();
        execute("delete from test where \"_id\" = 'id1'");
        assertEquals(1, response.rowCount());
        refresh();
        execute("select \"_id\" from test");
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

        SQLBulkResponse.Result[] r = execute("delete from test where pk_col=?",
            new Object[][]{{2}, {null}, {3}}).results();
        assertThat(r.length, is(3));
        assertThat(r[0].rowCount(), is(1L));
        assertThat(r[1].rowCount(), is(0L));
        assertThat(r[2].rowCount(), is(1L));

        r = execute("delete from test where pk_col=?", new Object[][]{{null}}).results();
        assertThat(r.length, is(1));
        assertThat(r[0].rowCount(), is(0L));

        execute("refresh table test");
        execute("select pk_col FROM test");
        assertThat(response.rowCount(), is(1L));
        assertEquals(response.rows()[0][0], "1");
    }
}
