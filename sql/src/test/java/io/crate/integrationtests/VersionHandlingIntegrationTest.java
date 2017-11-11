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
import io.crate.analyze.UpdateAnalyzer;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.core.Is.is;

public class VersionHandlingIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void selectMultiGetRequestWithColumnAlias() throws IOException {
        this.setup.createTestTableWithPrimaryKey();
        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");
        refresh();
        execute("SELECT pk_col as id, message from test where pk_col IN (?,?)", new Object[]{"1", "2"});
        assertThat(response.rowCount(), is(2L));
        assertThat(response.cols(), arrayContainingInAnyOrder("id", "message"));
        assertThat(new String[]{(String) response.rows()[0][0], (String) response.rows()[1][0]}, arrayContainingInAnyOrder("1", "2"));
    }

    @Test
    public void testDeleteWhereVersion() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();

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
        ensureYellow();

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
    public void testUpdateWhereVersionWithPrimaryKey() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();

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
    public void testUpdateWhereVersionWithoutPrimaryKey() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(UpdateAnalyzer.VERSION_SEARCH_EX_MSG);
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();
        execute("update test set col2 = ? where \"_version\" = ?",
            new Object[]{"ok now panic", 1});
    }

    @Test
    public void testUpdateWhereVersionWithConflict() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertEquals(1L, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);

        execute("update test set col2 = ? where col1 = ? and \"_version\" = ?",
            new Object[]{"ok now panic", 1, 1});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("update test set col2 = ? where col1 = ? and \"_version\" = ?",
            new Object[]{"hopefully not updated", 1, 1});
        assertEquals(0L, response.rowCount());
        refresh();

        // Validate that the row is really NOT updated
        refresh();
        execute("select col2 from test where col1 = 1");
        assertEquals(1L, response.rowCount());
        assertEquals("ok now panic", response.rows()[0][0]);
    }

    @Test
    public void testSelectWhereVersionWithoutPrimaryKey() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"_version\" column can only be used in the WHERE clause " +
                                        "if there are also equals comparisons on primary key columns");
        execute("select _version from test where col2 = 'hello' and _version = 1");
    }

    @Test
    public void testSelectWhereVersionWithPrimaryKey() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"_version\" column can only be used in the WHERE clause " +
                                        "if there are also equals comparisons on primary key columns");
        execute("select _version from test where col1 = 1 and _version = 50");
    }

    @Test
    public void testSelectGroupByVersion() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"_version\" column can only be used in the WHERE clause " +
                                        "if there are also equals comparisons on primary key columns");
        execute("select col2 from test where col1 = 1 and _version = 50 group by col2");
    }
}
