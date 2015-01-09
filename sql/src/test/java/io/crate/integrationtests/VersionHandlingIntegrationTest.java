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
import io.crate.test.integration.CrateIntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class VersionHandlingIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Test
    public void selectMultiGetRequestWithColumnAlias() throws IOException {
        this.setup.createTestTableWithPrimaryKey();
        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");
        refresh();
        execute("SELECT pk_col as id, message from test where pk_col IN (?,?)", new Object[]{'1', '2'});
        assertThat(response.rowCount(), is(2L));
        assertThat(response.cols(), arrayContainingInAnyOrder("id", "message"));
        assertThat(new String[]{(String) response.rows()[0][0], (String) response.rows()[1][0]}, arrayContainingInAnyOrder("1", "2"));
    }

    @Test
    public void testDeleteWhereVersion() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureGreen();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertEquals(1L, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
        Long version = (Long) response.rows()[0][0];

        execute("delete from test where col1 = 1 and \"_version\" = ?",
                new Object[]{version});
        assertEquals(1L, response.rowCount());

        // Validate that the row is really deleted
        refresh();
        execute("select * from test where col1 = 1");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testDeleteWhereVersionWithConflict() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertEquals(1L, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);

        execute("update test set col2 = ? where col1 = ?", new Object[]{"ok now panic", 1});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("delete from test where col1 = 1 and \"_version\" = 1");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testUpdateWhereVersion() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertEquals(1L, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);

        execute("update test set col2 = ? where col1 = ? and \"_version\" = ?",
                new Object[]{"ok now panic", 1, 1});
        assertEquals(1L, response.rowCount());

        // Validate that the row is really updated
        refresh();
        execute("select col2 from test where col1 = 1");
        assertEquals(1L, response.rowCount());
        assertEquals("ok now panic", response.rows()[0][0]);
    }

    @Test
    public void testUpdateWhereVersionWithConflict() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertEquals(1L, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);

        execute("update test set col2 = ? where col1 = ? and \"_version\" = ?",
                new Object[]{"ok now panic", 1, 1});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("update test set col2 = ? where col2 = ? and \"_version\" = ?",
                new Object[]{"already in panic", "ok now panic", 1});
        assertEquals(0, response.rowCount());

        // Validate that the row is really NOT updated
        refresh();
        execute("select col2 from test where col1 = 1");
        assertEquals(1L, response.rowCount());
        assertEquals("ok now panic", response.rows()[0][0]);
    }

    @Test
    public void testSelectWhereVersionWithoutPrimaryKey() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"_version\" column is only valid in the WHERE clause if the primary key column is also present");
        execute("select _version from test where col2 = 'hello' and _version = 1");
    }

    @Test
    public void testSelectWhereVersionWithPrimaryKey() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();
        execute("select _version from test where col1 = 1 and _version = 1");
        assertEquals(1L, response.rowCount());
    }
}
