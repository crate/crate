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
import io.crate.action.sql.SQLBulkResponse;
import io.crate.analyze.UpdateStatementAnalyzer;
import io.crate.test.integration.CrateIntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class UpdateIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testUpdate() throws Exception {
        execute("create table test (message string) clustered into 2 shards");
        ensureYellow();

        execute("insert into test values('hello'),('again'),('hello'),('hello')");
        assertEquals(4, response.rowCount());
        refresh();

        execute("update test set message='b' where message = 'hello'");

        assertEquals(3, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select message from test where message='b'");
        assertEquals(3, response.rowCount());
        assertEquals("b", response.rows()[0][0]);
    }

    @Test
    public void testUpdateByPrimaryKeyUnknownDocument() {
        execute("create table test (id int primary key, message string)");
        ensureYellow();
        execute("update test set message='b' where id = 1");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testUpdateWithExpression() throws Exception {
        execute("create table test (id integer, other_id long, name string)");
        ensureYellow();

        execute("insert into test (id, other_id, name) values(1, 10, 'Ford'),(2, 20, 'Arthur')");
        assertEquals(2, response.rowCount());
        refresh();

        execute("update test set id=(id+10)*other_id");

        assertEquals(2, response.rowCount());
        refresh();

        execute("select id, other_id, name from test order by id");
        assertEquals(2, response.rowCount());
        assertEquals(110, response.rows()[0][0]);
        assertEquals(10L, response.rows()[0][1]);
        assertEquals("Ford", response.rows()[0][2]);
        assertEquals(240, response.rows()[1][0]);
        assertEquals(20L, response.rows()[1][1]);
        assertEquals("Arthur", response.rows()[1][2]);
    }

    @Test
    public void testUpdateByPrimaryKeyWithExpression() throws Exception {
        execute("create table test (id integer primary key, other_id long)");
        ensureYellow();

        execute("insert into test (id, other_id) values(1, 10),(2, 20)");
        assertEquals(2, response.rowCount());
        refresh();

        execute("update test set other_id=(id+10)*id where id = 2");

        assertEquals(1, response.rowCount());
        refresh();

        execute("select other_id from test order by id");
        assertEquals(2, response.rowCount());
        assertEquals(10L, response.rows()[0][0]);
        assertEquals(24L, response.rows()[1][0]);
    }

    @Test
    public void testUpdateMultipleDocuments() throws Exception {
        execute("create table test (message string)");
        ensureYellow();
        execute("insert into test values('hello'),('again'),('hello')");
        assertEquals(3, response.rowCount());
        refresh();

        execute("update test set message='b' where message = 'hello'");

        assertEquals(2, response.rowCount());
        refresh();

        execute("select message from test where message='b'");
        assertEquals(2, response.rowCount());
        assertEquals("b", response.rows()[0][0]);

    }

    @Test
    public void testTwoColumnUpdate() throws Exception {
        execute("create table test (col1 string, col2 string)");
        ensureYellow();

        execute("insert into test values('hello', 'hallo'), ('again', 'nochmal')");
        assertEquals(2, response.rowCount());
        refresh();

        execute("update test set col1='b' where col1 = 'hello'");

        assertEquals(1, response.rowCount());
        refresh();

        execute("select col1, col2 from test where col1='b'");
        assertEquals(1, response.rowCount());
        assertEquals("b", response.rows()[0][0]);
        assertEquals("hallo", response.rows()[0][1]);

    }

    @Test
    public void testUpdateWithArgs() throws Exception {
        execute("create table test (" +
                "  coolness float, " +
                "  details array(object)" +
                ")");
        ensureYellow();

        execute("insert into test values(1.1, ?),(2.2, ?)", new Object[]{new Object[0],
                new Object[]{
                        new HashMap<String, Object>(),
                        new HashMap<String, Object>() {{
                            put("hello", "world");
                        }}
                }
        });
        assertEquals(2, response.rowCount());
        refresh();

        execute("update test set coolness=3.3, details=? where coolness = ?", new Object[]{new Object[0], 2.2});

        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness from test where coolness=3.3");
        assertEquals(1, response.rowCount());
        assertEquals(3.3f, response.rows()[0][0]);

    }

    @Test
    public void testUpdateNestedObjectWithoutDetailedSchema() throws Exception {
        execute("create table test (coolness object)");
        ensureYellow();

        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", 2);
        Object[] args = new Object[]{map};

        execute("insert into test values (?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("update test set coolness['x'] = '3'");

        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness['x'], coolness['y'] from test");
        assertEquals(1, response.rowCount());
        assertEquals("3", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
    }

    @Test
    public void testUpdateWithFunctionWhereArgumentIsInIntegerRangeInsteadOfLong() throws Exception {
        execute("create table t (ts timestamp, day int) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (ts, day) values (0, 1)");
        execute("refresh table t");

        execute("update t set day = extract(day from ts)");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testInsertIntoWithOnDuplicateKeyWithFunctionWhereArgumentIsInIntegerRangeInsteadOfLong() throws Exception {
        execute("create table t (id int primary key, ts timestamp, day int) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (id, ts, day) values (1, 0, 0)");
        execute("refresh table t");
        execute("insert into t (id, ts, day) (select id, ts, day from t) " +
                "on duplicate key update day = extract(day from ts)");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testUpdateNestedNestedObject() throws Exception {
        execute("create table test (" +
                "coolness object as (x object as (y object as (z int), a string, b string))," +
                "a object as (x string, y int)," +
                "firstcol int, othercol int" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", 2);
        Object[] args = new Object[]{map};

        execute("insert into test (a) values (?)", args);
        refresh();

        execute("update test set coolness['x']['y']['z'] = 3");

        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness['x'], a from test");
        assertEquals(1, response.rowCount());
        assertEquals("{y={z=3}}", response.rows()[0][0].toString());
        assertEquals(map, response.rows()[0][1]);

        execute("update test set firstcol = 1, coolness['x']['a'] = 'a', coolness['x']['b'] = 'b', othercol = 2");
        assertEquals(1, response.rowCount());
        refresh();
        waitNoPendingTasksOnAll();

        execute("select coolness['x']['b'], coolness['x']['a'], coolness['x']['y']['z'], " +
                "firstcol, othercol from test");
        assertEquals(1, response.rowCount());
        Object[] firstRow = response.rows()[0];
        assertEquals("b", firstRow[0]);
        assertEquals("a", firstRow[1]);
        assertEquals(3, firstRow[2]);
        assertEquals(1, firstRow[3]);
        assertEquals(2, firstRow[4]);
    }

    @Test
    public void testUpdateNestedObjectDeleteWithArgs() throws Exception {
        execute("create table test (a object as (x object as (y int, z int))) with (number_of_replicas=0)");
        ensureYellow();

        Map<String, Object> map = newHashMap();
        Map<String, Object> nestedMap = newHashMap();
        nestedMap.put("y", 2);
        nestedMap.put("z", 3);
        map.put("x", nestedMap);
        Object[] args = new Object[]{map};

        execute("insert into test (a) values (?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("update test set a['x']['z'] = ?", new Object[]{null});

        assertEquals(1, response.rowCount());
        refresh();

        execute("select a['x']['y'], a['x']['z'] from test");
        assertEquals(1, response.rowCount());
        assertEquals(2, response.rows()[0][0]);
        assertNull(response.rows()[0][1]);
    }

    @Test
    public void testUpdateNestedObjectDeleteWithoutArgs() throws Exception {
        execute("create table test (a object as (x object as (y int, z int))) with (number_of_replicas=0)");
        ensureYellow();

        Map<String, Object> map = newHashMap();
        Map<String, Object> nestedMap = newHashMap();
        nestedMap.put("y", 2);
        nestedMap.put("z", 3);
        map.put("x", nestedMap);
        Object[] args = new Object[]{map};

        execute("insert into test (a) values (?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("update test set a['x']['z'] = null");

        assertEquals(1, response.rowCount());
        refresh();

        execute("select a['x']['z'], a['x']['y'] from test");
        assertEquals(1, response.rowCount());
        assertNull(response.rows()[0][0]);
        assertEquals(2, response.rows()[0][1]);
    }

    @Test
    public void testUpdateWithNestedObjectArrayIdxAccess() throws Exception {
        execute("create table test (coolness array(float)) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test values (?)", new Object[]{new Object[]{2.2, 2.3, 2.4}});
        assertEquals(1, response.rowCount());
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483648");

        execute("update test set coolness[0] = 3.3");
    }

    @Test
    public void testUpdateNestedObjectWithDetailedSchema() throws Exception {
        execute("create table test (coolness object as (x string, y string))");
        ensureYellow();
        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", "2");
        Object[] args = new Object[]{map};

        execute("insert into test values (?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("update test set coolness['x'] = '3'");

        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness from test");
        assertEquals(1, response.rowCount());
        assertEquals("{y=2, x=3}", response.rows()[0][0].toString());
    }

    @Test
    public void testUpdateResetNestedObject() throws Exception {
        execute("create table test (coolness object)");
        ensureYellow();

        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", 2);
        Object[] args = new Object[]{map};

        execute("insert into test values (?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        // update with different map
        Map<String, Object> new_map = new HashMap<>();
        new_map.put("z", 1);

        execute("update test set coolness = ?", new Object[]{new_map});
        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness from test");
        assertEquals(1, response.rowCount());
        assertEquals(new_map, response.rows()[0][0]);

        // update with empty map
        Map<String, Object> empty_map = new HashMap<>();

        execute("update test set coolness = ?", new Object[]{empty_map});
        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness from test");
        assertEquals(1, response.rowCount());
        assertEquals(empty_map, response.rows()[0][0]);
    }

    @Test
    public void testUpdateResetNestedObjectUsingUpdateRequest() throws Exception {
        execute("create table test (id string, data object(ignored))");
        ensureYellow();

        Map<String, Object> data = new HashMap<String, Object>() {{
            put("foo", "bar");
            put("days", new ArrayList<String>() {{
                add("Mon");
                add("Tue");
                add("Wen");
            }});
        }};
        execute("insert into test (id, data) values (?, ?)", new Object[]{"1", data});
        refresh();

        execute("select data from test where id = ?", new Object[]{"1"});
        assertEquals(data, response.rows()[0][0]);

        Map<String, Object> new_data = new HashMap<String, Object>() {{
            put("days", new ArrayList<String>() {{
                add("Mon");
                add("Wen");
            }});
        }};
        execute("update test set data = ? where id = ?", new Object[]{new_data, "1"});
        assertEquals(1, response.rowCount());
        refresh();

        execute("select data from test where id = ?", new Object[]{"1"});
        assertEquals(new_data, response.rows()[0][0]);
    }

    @Test
    public void testUpdateResetNestedNestedObject() throws Exception {
        execute("create table test (coolness object)");
        ensureYellow();

        Map<String, Object> map = new HashMap<String, Object>() {{
            put("x", "1");
            put("y", new HashMap<String, Object>() {{
                put("z", 3);
            }});
        }};

        execute("insert into test values (?)", new Object[]{map});
        assertEquals(1, response.rowCount());
        refresh();
        waitNoPendingTasksOnAll();

        Map<String, Object> new_map = new HashMap<>();
        new_map.put("a", 1);

        execute("update test set coolness['y'] = ?", new Object[]{new_map});
        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness['y'], coolness['x'] from test");
        assertEquals(1, response.rowCount());
        assertEquals(new_map, response.rows()[0][0]);
        assertEquals("1", response.rows()[0][1]);
    }

    @Test
    public void testUpdateToUpdateRequestByPlanner() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('123', 'bar')");
        assertEquals(1, response.rowCount());
        waitNoPendingTasksOnAll(); // wait for new columns to be available
        refresh();

        execute("update test set message='bar1' where pk_col='123'");
        assertEquals(1, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select message from test where pk_col='123'");
        assertEquals(1, response.rowCount());
        assertEquals("bar1", response.rows()[0][0]);
    }

    @Test
    public void testUpdateByIdWithMultiplePrimaryKeyAndClusteredBy() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) clustered by(author) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, author, quote) values(?, ?, ?)",
                new Object[]{1, "Ford", "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());

        execute("update quotes set quote=? where id=1 and author='Ford'",
                new Object[]{"Don't panic"});
        assertEquals(1L, response.rowCount());
        refresh();
        execute("select quote from quotes where id=1 and author='Ford'");
        assertEquals(1L, response.rowCount());
        assertThat((String) response.rows()[0][0], is("Don't panic"));
    }

    @Test
    public void testUpdateByQueryWithMultiplePrimaryKeyAndClusteredBy() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) clustered by(author) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, author, quote) values(?, ?, ?)",
                new Object[]{1, "Ford", "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("update quotes set quote=? where id=1",
                new Object[]{"Don't panic"});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select quote from quotes where id=1 and author='Ford'");
        assertEquals(1L, response.rowCount());
        assertThat((String) response.rows()[0][0], is("Don't panic"));
    }

    @Test
    public void testUpdateVersionHandling() throws Exception {
        execute("create table test (id int primary key, c int) with (number_of_replicas=0, refresh_interval=0)");
        ensureYellow();
        execute("insert into test (id, c) values (1, 1)");
        execute("refresh table test");
        execute("select _version, c from test");

        long version = (Long) response.rows()[0][0];
        assertThat(version, is(1L));

        // with primary key optimization:

        execute("update test set c = 2 where id = 1 and _version = 1"); // this one works
        assertThat(response.rowCount(), is(1L));
        execute("update test set c = 3 where id = 1 and _version = 1"); // this doesn't
        assertThat(response.rowCount(), is(0L));

        execute("refresh table test");
        execute("select _version, c from test");
        assertThat((Long) response.rows()[0][0], is(2L));
        assertThat((Integer) response.rows()[0][1], is(2));

    }

    @Test
    public void testUpdateVersionOrOperator() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(UpdateStatementAnalyzer.VERSION_SEARCH_EX_MSG);

        execute("create table test (id int primary key, c int) with (number_of_replicas=0, refresh_interval=0)");
        ensureGreen();
        execute("insert into test (id, c) values (1, 1)");
        execute("refresh table test");
        execute("update test set c = 4 where _version = 2 or _version=1"); // this one works
    }

    @Test
    public void testUpdateVersionInOperator() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(UpdateStatementAnalyzer.VERSION_SEARCH_EX_MSG);

        execute("create table test (id int primary key, c int) with (number_of_replicas=0, refresh_interval=0)");
        ensureGreen();
        execute("insert into test (id, c) values (1, 1)");
        execute("refresh table test");
        execute("update test set c = 4 where _version in (1,2)");
    }


    @Test
    public void testUpdateRetryOnVersionConflict() throws Exception {
        // issue a bulk update request updating the same document to force a version conflict
        execute("create table test (a string, b int) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (a, b) values ('foo', 1)");
        assertThat(response.rowCount(), is(1L));
        refresh();

        SQLBulkResponse bulkResp = execute("update test set a = ? where b = ?",
                new Object[][]{
                        new Object[]{"bar", 1},
                        new Object[]{"baz", 1},
                        new Object[]{"foobar", 1}});
        assertThat(bulkResp.results().length, is(3));
        // all statements must succeed and return 1 affected row
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(1L));
        }
    }

}
