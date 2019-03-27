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
import io.crate.exceptions.VersionInvalidException;
import io.crate.testing.SQLBulkResponse;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.common.collect.MapBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static com.google.common.collect.Maps.newHashMap;
import static io.crate.testing.TestingHelpers.mapToSortedString;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

public class UpdateIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void testUpdate() throws Exception {
        execute("create table test (message string) clustered into 2 shards");
        ensureYellow();

        execute("insert into test values('hello'),('again'),('hello'),('hello')");
        assertEquals(4, response.rowCount());
        refresh();

        execute("update test set message='b' where message = 'hello'");

        assertEquals(3, response.rowCount());
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
    public void testUpdateNotNullColumn() {
        execute("create table test (id int primary key, message string not null)");
        ensureYellow();
        execute("insert into test (id, message) values(1, 'Ford'),(2, 'Arthur')");
        assertEquals(2, response.rowCount());
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"message\" must not be null");
        execute("update test set message=null where id=1");
    }

    @Test
    public void testUpdateWithNotNull1LevelNestedColumn() {
        execute("create table test (" +
                "stuff object(dynamic) AS (" +
                "  level1 string not null" +
                ") not null)");
        ensureYellow();
        execute("insert into test (stuff) values('{\"level1\":\"value\"}')");
        assertEquals(1, response.rowCount());
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"stuff['level1']\" must not be null");
        execute("update test set stuff['level1']=null");
    }

    @Test
    public void testUpdateWithNotNull2LevelsNestedColumn() {
        execute("create table test (" +
                "stuff object(dynamic) AS (" +
                "  level1 object(dynamic) AS (" +
                "    level2 string not null" +
                "  ) not null" +
                ") not null)");
        ensureYellow();
        execute("insert into test (stuff) values('{\"level1\":{\"level2\":\"value\"}}')");
        assertEquals(1, response.rowCount());
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"stuff['level1']['level2']\" must not be null");
        execute("update test set stuff['level1']['level2']=null");
    }

    @Test
    public void testUpdateNullDynamicColumn() {
        /*
         * Regression test
         * validating dynamically generated columns with NULL values led to NPE
         */
        execute("create table test (id int primary key) with (column_policy = 'dynamic')");
        ensureYellow();
        execute("insert into test (id) values (1)");
        refresh();

        execute("update test set dynamic_col=null");
        refresh();
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testUpdateWithExpression() throws Exception {
        execute("create table test (id integer, other_id long, name string)");
        ensureYellow();

        execute("insert into test (id, other_id, name) values(1, 10, 'Ford'),(2, 20, 'Arthur')");
        assertEquals(2, response.rowCount());
        refresh();

        execute("update test set id=(id+10)*cast(other_id as integer)");

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
    public void testUpdateWithExpressionReferenceUpdated() throws Exception {
        // test that expression in update assignment always refer to existing values, not updated ones

        execute("create table test (dividend integer, divisor integer, quotient integer)");
        ensureYellow();

        execute("insert into test (dividend, divisor, quotient) values(10, 2, 5)");
        assertEquals(1, response.rowCount());
        refresh();

        execute("update test set dividend = 30, quotient = dividend/divisor");
        assertEquals(1, response.rowCount());
        refresh();

        execute("select quotient name from test");
        assertEquals(5, response.rows()[0][0]);
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

        waitForMappingUpdateOnAll("test", "coolness.x");
        execute("select coolness['x'], coolness['y'] from test");
        assertEquals(1, response.rowCount());
        assertEquals("3", response.rows()[0][0]);
        // integer values for unknown columns will be result in a long type for range safety
        assertEquals(2L, response.rows()[0][1]);
    }

    @Test
    public void testUpdateWithFunctionWhereArgumentIsInIntegerRangeInsteadOfLong() {
        execute(
            "create table t (" +
            "   ts timestamp with time zone," +
            "   day int" +
            ") with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (ts, day) values (0, 1)");
        execute("refresh table t");

        execute("update t set day = extract(day from ts)");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testInsertIntoWithOnConflictKeyWithFunctionWhereArgumentIsInIntegerRangeInsteadOfLong() {
        execute(
            "create table t (" +
            "   id int primary key," +
            "   ts timestamp with time zone, day int" +
            ") with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (id, ts, day) values (1, 0, 0)");
        execute("refresh table t");
        execute("insert into t (id, ts, day) (select id, ts, day from t) " +
                "on conflict (id) do update set day = extract(day from ts)");
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
        //noinspection unchecked
        assertEquals("x=3, y=2", mapToSortedString((Map<String, Object>) response.rows()[0][0]));
    }

    /**
     * Disable JDBC/PSQL as object values are streamed via JSON on the PSQL wire protocol which is not type safe.
     */
    @UseJdbc(0)
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
        new_map.put("z", 1L);

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

    /**
     * Disable JDBC/PSQL as object values are streamed via JSON on the PSQL wire protocol which is not type safe.
     */
    @UseJdbc(0)
    @Test
    public void testUpdateResetNestedNestedObject() throws Exception {
        execute("create table test (coolness object)");
        ensureYellow();

        Map<String, Object> map = new HashMap<>() {{
            put("x", "1");
            put("y", new HashMap<String, Object>() {{
                put("z", 3);
            }});
        }};

        execute("insert into test values (?)", new Object[]{map});
        assertEquals(1, response.rowCount());
        refresh();

        Map<String, Object> new_map = new HashMap<>();
        new_map.put("a", 1L);

        execute("update test set coolness['y'] = ?", new Object[]{new_map});
        assertEquals(1, response.rowCount());
        refresh();

        waitForMappingUpdateOnAll("test", "coolness.x");
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
    public void testMultiUpdateWithVersionAndConflict() throws Exception {
        execute("create table test (id int primary key, c int)");
        ensureYellow();
        execute("insert into test (id, c) values (1, 1), (2, 1)");
        refresh();

        // update 2nd row in order to increase version
        execute("update test set c = 2 where id = 2");
        refresh();

        // now update both rows, 2nd will result in conflict, but 1st one was successful and must be replicated
        execute("update test set c = 3 where (id = 1 and _version = 1) or (id = 2 and _version = 1)");
        assertThat(response.rowCount(), is(1L));

        refresh();
        execute("select _version from test order by id");
        assertThat((Long) response.rows()[0][0], is(2L));
        assertThat((Long) response.rows()[1][0], is(2L));
    }

    @Test
    public void testUpdateVersionOrOperator() throws Exception {
        execute("create table test (id int primary key, c int) with (number_of_replicas=0, refresh_interval=0)");
        ensureGreen();
        execute("insert into test (id, c) values (1, 1)");
        execute("refresh table test");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(VersionInvalidException.ERROR_MSG);
        execute("update test set c = 4 where _version = 2 or _version=1");
    }

    @Test
    public void testUpdateVersionInOperator() throws Exception {
        execute("create table test (id int primary key, c int) with (number_of_replicas=0, refresh_interval=0)");
        ensureGreen();
        execute("insert into test (id, c) values (1, 1)");
        execute("refresh table test");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(VersionInvalidException.ERROR_MSG);
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
        refresh();

        // document was changed 4 times (including initial creation), so version must be 4
        execute("select _version from test where b = 1");
        assertThat((Long) response.rows()[0][0], is(4L));
    }

    @Test
    public void testUpdateByIdPartitionColumnPartOfPrimaryKey() throws Exception {
        execute("create table party (" +
                "  id int primary key, " +
                "  type byte primary key, " +
                "  value string" +
                ") partitioned by (type) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into party (id, type, value) values (?, ?, ?)", new Object[][]{
            {1, 2, "foo"},
            {2, 3, "bar"},
            {2, 4, "baz"}
        });
        execute("refresh table party");

        execute("update party set value='updated' where (id=1 and type=2) or (id=2 and type=4)");
        assertThat(response.rowCount(), is(2L));

        execute("refresh table party");

        execute("select id, type, value from party order by id, value");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "1| 2| updated\n" +
            "2| 3| bar\n" +
            "2| 4| updated\n"));

    }

    @Test
    public void testBulkUpdateWithOnlyOneBulkArgIsProducingRowCountResult() throws Exception {
        execute("create table t (name string) with (number_of_replicas = 0)");
        ensureYellow();
        // regression test, used to throw a ClassCastException because the JobLauncher created a
        // QueryResult instead of RowCountResult
        SQLBulkResponse bulkResponse = execute("update t set name = 'Trillian' where name = ?", $$($("Arthur")));
        assertThat(bulkResponse.results().length, is(1));
    }

    @Test
    public void testBulkUpdateWithPKAndMultipleHits() throws Exception {
        execute("create table t (id integer primary key, name string) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t values (?, ?)", $$($(1, "foo"), $(2, "bar"), $(3, "hoschi"), $(4, "crate")));
        refresh();

        SQLBulkResponse bulkResponse = execute("update t set name = 'updated' where id = ? or id = ?", $$($(1, 2), $(3, 4)));
        assertThat(bulkResponse.results().length, is(2));
        for (SQLBulkResponse.Result result : bulkResponse.results()) {
            assertThat(result.rowCount(), is(2L));
        }
    }

    @Test
    public void testUpdateWithGeneratedColumn() {
        execute("create table generated_column (" +
                " id int primary key," +
                " ts timestamp with time zone," +
                " day as date_trunc('day', ts)," +
                " \"user\" object as (name string)," +
                " name as concat(\"user\"['name'], 'bar')" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into generated_column (id, ts, \"user\") values (?, ?, ?)", new Object[]{
            1, "2015-11-18T11:11:00", MapBuilder.newMapBuilder().put("name", "foo").map()});
        refresh();
        execute("update generated_column set ts = ?, \"user\" = ? where id = ?", new Object[]{
            "2015-11-19T17:06:00", MapBuilder.newMapBuilder().put("name", "zoo").map(), 1});
        refresh();
        execute("select day, name from generated_column");
        assertThat((Long) response.rows()[0][0], is(1447891200000L));
        assertThat((String) response.rows()[0][1], is("zoobar"));
    }

    @Test
    public void testGeneratedColumnWithoutRefsToOtherColumnsComputedOnUpdate() {
        execute("create table generated_column (" +
                " \"inserted\" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS current_timestamp(3), " +
                " \"message\" STRING" +
                ")");
        ensureYellow();
        execute("insert into generated_column (message) values (?)", new Object[]{"str"});
        refresh();
        execute("select inserted from generated_column");
        long ts = (long) response.rows()[0][0];
        execute("update generated_column set message = ?", new Object[]{"test"});
        refresh();
        execute("select inserted from generated_column");
        assertThat(response.rows()[0][0], not(ts));
    }

    @Test
    public void testUpdateSetInvalidGeneratedColumnOnly() {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Given value 1745 for generated column gen_col does not match calculation extract(year from ts) = 1970");
        execute("create table computed (" +
                " ts timestamp with time zone," +
                " gen_col as extract(year from ts)" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into computed (ts) values (1)");
        refresh();
        execute("update computed set gen_col=1745");
    }

    @Test
    public void testUpdateNotNullSourceGeneratedColumn() {
        execute("create table generated_column (" +
                " id int primary key," +
                " ts timestamp with time zone," +
                " gen_col as extract(year from ts) not null" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into generated_column (id, ts) values (1, '2015-11-18T11:11:00')");
        assertEquals(1, response.rowCount());
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"gen_col\" must not be null");
        execute("update generated_column set ts=null where id=1");
    }

    @Test
    public void testUpdateNotNullTargetGeneratedColumn() {
        execute("create table generated_column (" +
                " id int primary key," +
                " ts timestamp with time zone," +
                " gen_col as extract(year from ts) not null" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into generated_column (id, ts) values (1, '2015-11-18T11:11:00')");
        assertEquals(1, response.rowCount());
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"gen_col\" must not be null");
        execute("update generated_column set gen_col=null where id=1");
    }

    @Test
    public void testUpdateWithGeneratedColumnSomeReferencesUpdated() throws Exception {
        execute("create table computed (" +
                " firstname string," +
                " surname string," +
                " name as concat(surname, ', ', firstname)" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into computed (firstname, surname) values ('Douglas', 'Adams')");
        refresh();
        execute("update computed set firstname = 'Ford'");
        refresh();
        execute("select name from computed");
        assertThat((String) response.rows()[0][0], is("Adams, Ford"));
    }

    @Test
    public void testUpdateExpressionReferenceGeneratedColumn() throws Exception {
        execute("create table computed (" +
                " a int," +
                " b int," +
                " c as (b + 1)" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into computed (a, b) values (1, 2)");
        refresh();
        execute("update computed set a = c + 1");
        refresh();
        execute("select a from computed");
        assertThat((Integer) response.rows()[0][0], is(4));
    }

    @Test
    public void testUpdateReferencedByGeneratedColumnWithExpressionReferenceGeneratedColumn() throws Exception {
        execute("create table computed (" +
                " a int," +
                " b as (a + 1)" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into computed (a) values (1)");
        refresh();
        execute("update computed set a = b + 1");
        refresh();
        execute("select a from computed");
        assertThat((Integer) response.rows()[0][0], is(3));
    }

    @Test
    public void testFailingUpdateBulkOperation() throws Exception {
        execute("create table t (x string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (x) values ('1')");
        execute("refresh table t");

        // invalid regex causes failure in prepare phase, causing a failure row-count in each individual response
        Object[][] bulkArgs = new Object[][] {
            new Object[] { 1, "+123" },
            new Object[] { 2, "+123" },
        };
        SQLBulkResponse resp = execute("update t set x = ? where x ~* ?", bulkArgs);
        assertThat(resp.results().length, is(2));
        for (SQLBulkResponse.Result result : resp.results()) {
            assertThat(result.rowCount(), is(-2L));
        }
    }


    @Test
    public void testUpdateByQueryWithSubQuery() throws Exception {
        execute("create table t1 (x int)");
        execute("insert into t1 (x) values (1), (2)");
        execute("refresh table t1");
        execute("update t1 set x = (select 3) where x = (select 1)");
        assertThat(response.rowCount(), is(1L));

        execute("refresh table t1");
        execute("select x from t1 order by x asc");
        assertThat(printedTable(response.rows()), is("2\n" +
                                                     "3\n"));
    }

    @Test
    public void testUpdateByIdWithSubQuery() throws Exception {
        execute("create table t1 (id int primary key, name string)");
        execute("insert into t1 (id, name) values (1, 'Arthur'), (2, 'Trillian')");
        execute("refresh table t1");
        execute("update t1 set name = (select 'Slartibartfast') where id = (select 1)");
        assertThat(response.rowCount(), is(1L));

        execute("refresh table t1");
        execute("select id, name from t1 order by id asc");
        assertThat(printedTable(response.rows()), is("1| Slartibartfast\n" +
                                                     "2| Trillian\n"));
    }
}
