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

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.common.collections.MapBuilder;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.VersioningValidationException;
import io.crate.testing.Asserts;
import io.crate.testing.UseJdbc;

public class UpdateIntegrationTest extends IntegTestCase {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void testUpdate() throws Exception {
        execute("create table test (message string) clustered into 2 shards");
        execute("insert into test values('hello'),('again'),('hello'),('hello')");
        assertThat(response).hasRowCount(4);
        refresh();

        execute("update test set message='b' where message = 'hello'");

        assertThat(response).hasRowCount(3);
        refresh();

        execute("select message from test where message='b'");
        assertThat(response).hasRowCount(3);
        assertThat(response.rows()[0][0]).isEqualTo("b");
    }

    @Test
    public void testUpdateByPrimaryKeyUnknownDocument() {
        execute("create table test (id int primary key, message string)");
        execute("update test set message='b' where id = 1");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testUpdateNotNullColumn() {
        execute("create table test (id int primary key, message string not null)");
        execute("insert into test (id, message) values(1, 'Ford'),(2, 'Arthur')");
        assertThat(response).hasRowCount(2);
        refresh();

        Asserts.assertSQLError(() -> execute(
                        "update test set message=null where id=1"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(BAD_REQUEST, 4000)
                .hasMessageContaining("\"message\" must not be null");
    }

    @Test
    public void testUpdateWithNotNull1LevelNestedColumn() {
        execute("create table test (" +
                "stuff object(dynamic) AS (" +
                "  level1 string not null" +
                ") not null)");
        execute("insert into test (stuff) values('{\"level1\":\"value\"}')");
        assertThat(response).hasRowCount(1);
        refresh();

        Asserts.assertSQLError(() -> execute(
                        "update test set stuff['level1']=null"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(BAD_REQUEST, 4000)
                .hasMessageContaining("\"stuff['level1']\" must not be null");
    }

    @Test
    public void testUpdateWithNotNull2LevelsNestedColumn() {
        execute("create table test (" +
                "stuff object(dynamic) AS (" +
                "  level1 object(dynamic) AS (" +
                "    level2 string not null" +
                "  ) not null" +
                ") not null)");
        execute("insert into test (stuff) values('{\"level1\":{\"level2\":\"value\"}}')");
        assertThat(response).hasRowCount(1);
        refresh();

        Asserts.assertSQLError(() -> execute(
                        "update test set stuff['level1']['level2']=null"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(BAD_REQUEST, 4000)
                .hasMessageContaining("\"stuff['level1']['level2']\" must not be null");
    }

    @Test
    public void testUpdateNullDynamicColumn() {
        /*
         * Regression test
         * validating dynamically generated columns with NULL values led to NPE
         */
        execute("create table test (id int primary key) with (column_policy = 'dynamic')");
        execute("insert into test (id) values (1)");
        refresh();

        execute("update test set dynamic_col=null");
        refresh();
        assertThat(response).hasRowCount(1);
    }

    @Test
    public void testUpdateWithExpression() throws Exception {
        execute("create table test (id integer, other_id long, name string)");
        execute("insert into test (id, other_id, name) values(1, 10, 'Ford'),(2, 20, 'Arthur')");
        assertThat(response).hasRowCount(2);
        refresh();

        execute("update test set id=(id+10)*cast(other_id as integer)");

        assertThat(response).hasRowCount(2);
        refresh();

        execute("select id, other_id, name from test order by id");
        assertThat(response).hasRowCount(2);
        assertThat(response.rows()[0][0]).isEqualTo(110);
        assertThat(response.rows()[0][1]).isEqualTo(10L);
        assertThat(response.rows()[0][2]).isEqualTo("Ford");
        assertThat(response.rows()[1][0]).isEqualTo(240);
        assertThat(response.rows()[1][1]).isEqualTo(20L);
        assertThat(response.rows()[1][2]).isEqualTo("Arthur");
    }

    @Test
    public void testUpdateWithExpressionReferenceUpdated() throws Exception {
        // test that expression in update assignment always refer to existing values, not updated ones

        execute("create table test (dividend integer, divisor integer, quotient integer)");
        execute("insert into test (dividend, divisor, quotient) values(10, 2, 5)");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set dividend = 30, quotient = dividend/divisor");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("select quotient name from test");
        assertThat(response.rows()[0][0]).isEqualTo(5);
    }

    @Test
    public void testUpdateByPrimaryKeyWithExpression() throws Exception {
        execute("create table test (id integer primary key, other_id long)");
        execute("insert into test (id, other_id) values(1, 10),(2, 20)");
        assertThat(response).hasRowCount(2);
        refresh();

        execute("update test set other_id=(id+10)*id where id = 2");

        assertThat(response).hasRowCount(1);
        refresh();

        execute("select other_id from test order by id");
        assertThat(response).hasRowCount(2);
        assertThat(response.rows()[0][0]).isEqualTo(10L);
        assertThat(response.rows()[1][0]).isEqualTo(24L);
    }

    @Test
    public void testUpdateMultipleDocuments() throws Exception {
        execute("create table test (message string)");
        execute("insert into test values('hello'),('again'),('hello')");
        assertThat(response).hasRowCount(3);
        refresh();

        execute("update test set message='b' where message = 'hello'");

        assertThat(response).hasRowCount(2);
        refresh();

        execute("select message from test where message='b'");
        assertThat(response).hasRowCount(2);
        assertThat(response.rows()[0][0]).isEqualTo("b");

    }

    @Test
    public void testTwoColumnUpdate() throws Exception {
        execute("create table test (col1 string, col2 string)");
        execute("insert into test values('hello', 'hallo'), ('again', 'nochmal')");
        assertThat(response).hasRowCount(2);
        refresh();

        execute("update test set col1='b' where col1 = 'hello'");

        assertThat(response).hasRowCount(1);
        refresh();

        execute("select col1, col2 from test where col1='b'");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo("b");
        assertThat(response.rows()[0][1]).isEqualTo("hallo");

    }

    @Test
    public void testUpdateWithArgs() throws Exception {
        execute("create table test (" +
                "  coolness float, " +
                "  details array(object)" +
                ")");
        execute("insert into test values(1.1, ?),(2.2, ?)", new Object[]{new Object[0],
            new Object[]{
                new HashMap<String, Object>(),
                Map.of("hello", "world")}
        });
        assertThat(response).hasRowCount(2);
        refresh();

        execute("update test set coolness=3.3, details=? where coolness = ?", new Object[]{new Object[0], 2.2});

        assertThat(response).hasRowCount(1);
        refresh();

        execute("select coolness from test where coolness=3.3");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(3.3f);

    }

    @Test
    public void testUpdateNestedObjectWithoutDetailedSchema() throws Exception {
        execute("create table test (coolness object)");
        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", 2);
        Object[] args = new Object[]{map};

        execute("insert into test values (?)", args);
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set coolness['x'] = '3'");

        assertThat(response).hasRowCount(1);
        refresh();

        execute("select coolness['x'], coolness['y'] from test");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo("3");
        // integer values for unknown columns will be result in a long type for range safety
        assertThat(response.rows()[0][1]).isEqualTo(2L);
    }

    @Test
    public void testUpdateWithFunctionWhereArgumentIsInIntegerRangeInsteadOfLong() {
        execute(
            "create table t (" +
            "   ts timestamp with time zone," +
            "   day int" +
            ") with (number_of_replicas = 0)");
        execute("insert into t (ts, day) values (0, 1)");
        execute("refresh table t");

        execute("update t set day = extract(day from ts)");
        assertThat(response).hasRowCount(1L);
    }

    @Test
    public void testInsertIntoWithOnConflictKeyWithFunctionWhereArgumentIsInIntegerRangeInsteadOfLong() {
        execute(
            "create table t (" +
            "   id int primary key," +
            "   ts timestamp with time zone, day int" +
            ") with (number_of_replicas = 0)");
        execute("insert into t (id, ts, day) values (1, 0, 0)");
        execute("refresh table t");
        execute("insert into t (id, ts, day) (select id, ts, day from t) " +
                "on conflict (id) do update set day = extract(day from ts)");
        assertThat(response).hasRowCount(1L);
    }

    @Test
    public void testUpdateNestedNestedObject() throws Exception {
        execute("create table test (" +
                "coolness object as (x object as (y object as (z int), a string, b string))," +
                "a object as (x string, y int)," +
                "firstcol int, othercol int" +
                ") with (number_of_replicas=0)");
        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", 2);
        Object[] args = new Object[]{map};

        execute("insert into test (a) values (?)", args);
        refresh();

        execute("update test set coolness['x']['y']['z'] = 3");

        assertThat(response).hasRowCount(1);
        refresh();

        execute("select coolness['x'], a from test");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0].toString()).isEqualTo("{y={z=3}}");
        assertThat(response.rows()[0][1]).isEqualTo(map);

        execute("update test set firstcol = 1, coolness['x']['a'] = 'a', coolness['x']['b'] = 'b', othercol = 2");
        assertThat(response).hasRowCount(1);
        refresh();
        waitNoPendingTasksOnAll();

        execute("select coolness['x']['b'], coolness['x']['a'], coolness['x']['y']['z'], " +
                "firstcol, othercol from test");
        assertThat(response).hasRowCount(1);
        Object[] firstRow = response.rows()[0];
        assertThat(firstRow[0]).isEqualTo("b");
        assertThat(firstRow[1]).isEqualTo("a");
        assertThat(firstRow[2]).isEqualTo(3);
        assertThat(firstRow[3]).isEqualTo(1);
        assertThat(firstRow[4]).isEqualTo(2);
    }

    @Test
    public void testUpdateNestedObjectDeleteWithArgs() throws Exception {
        execute("create table test (a object as (x object as (y int, z int))) with (number_of_replicas=0)");
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("y", 2);
        nestedMap.put("z", 3);
        map.put("x", nestedMap);
        Object[] args = new Object[]{map};

        execute("insert into test (a) values (?)", args);
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set a['x']['z'] = ?", new Object[]{null});

        assertThat(response).hasRowCount(1);
        refresh();

        execute("select a['x']['y'], a['x']['z'] from test");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(2);
        assertThat(response.rows()[0][1]).isNull();
    }

    @Test
    public void testUpdateNestedObjectDeleteWithoutArgs() throws Exception {
        execute("create table test (a object as (x object as (y int, z int))) with (number_of_replicas=0)");
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("y", 2);
        nestedMap.put("z", 3);
        map.put("x", nestedMap);
        Object[] args = new Object[]{map};

        execute("insert into test (a) values (?)", args);
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set a['x']['z'] = null");

        assertThat(response).hasRowCount(1);
        refresh();

        execute("select a['x']['z'], a['x']['y'] from test");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isNull();
        assertThat(response.rows()[0][1]).isEqualTo(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdateNestedObjectWithDetailedSchema() throws Exception {
        execute("create table test (coolness object as (x string, y string))");
        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", "2");
        Object[] args = new Object[]{map};

        execute("insert into test values (?)", args);
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set coolness['x'] = '3'");

        assertThat(response).hasRowCount(1);
        refresh();

        execute("select coolness from test");
        assertThat(response).hasRowCount(1);
        assertThat((Map<String, Object>) response.rows()[0][0])
            .isEqualTo(Map.of("x", "3", "y", "2"));
    }

    /**
     * Disable JDBC/PSQL as object values are streamed via JSON on the PSQL wire protocol which is not type safe.
     */
    @UseJdbc(0)
    @Test
    public void testUpdateResetNestedObject() throws Exception {
        execute("create table test (coolness object)");
        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", 2);
        Object[] args = new Object[]{map};

        execute("insert into test values (?)", args);
        assertThat(response).hasRowCount(1);
        refresh();

        // update with different map
        Map<String, Object> new_map = new HashMap<>();
        new_map.put("z", 1L);

        execute("update test set coolness = ?", new Object[]{new_map});
        assertThat(response).hasRowCount(1);
        refresh();

        execute("select coolness from test");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(new_map);

        // update with empty map
        Map<String, Object> empty_map = new HashMap<>();

        execute("update test set coolness = ?", new Object[]{empty_map});
        assertThat(response).hasRowCount(1);
        refresh();

        execute("select coolness from test");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(empty_map);
    }

    @Test
    public void testUpdateResetNestedObjectUsingUpdateRequest() throws Exception {
        execute("create table test (id string, data object(ignored))");
        Map<String, Object> data = Map.of(
            "foo", "bar",
            "days", List.of(
                "Mon",
                "Tue",
                "Wen"));

        execute("insert into test (id, data) values (?, ?)", new Object[]{"1", data});
        refresh();

        execute("select data from test where id = ?", new Object[]{"1"});
        assertThat(response.rows()[0][0]).isEqualTo(data);

        Map<String, Object> new_data = Map.of(
            "days", List.of(
                "Mon",
                "Wen"));

        execute("update test set data = ? where id = ?", new Object[]{new_data, "1"});
        assertThat(response).hasRowCount(1);
        refresh();

        execute("select data from test where id = ?", new Object[]{"1"});
        assertThat(response.rows()[0][0]).isEqualTo(new_data);
    }

    /**
     * Disable JDBC/PSQL as object values are streamed via JSON on the PSQL wire protocol which is not type safe.
     */
    @UseJdbc(0)
    @Test
    public void testUpdateResetNestedNestedObject() throws Exception {
        execute("create table test (coolness object)");
        Map<String, Object> map = Map.of(
            "x", "1",
            "y", Map.of("z", 3));

        execute("insert into test values (?)", new Object[]{map});
        assertThat(response).hasRowCount(1);
        refresh();

        Map<String, Object> new_map = new HashMap<>();
        new_map.put("a", 1L);

        execute("update test set coolness['y'] = ?", new Object[]{new_map});
        assertThat(response).hasRowCount(1);
        refresh();

        execute("select coolness['y'], coolness['x'] from test");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(new_map);
        assertThat(response.rows()[0][1]).isEqualTo("1");
    }

    @Test
    public void testUpdateToUpdateRequestByPlanner() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('123', 'bar')");
        assertThat(response).hasRowCount(1);
        waitNoPendingTasksOnAll(); // wait for new columns to be available
        refresh();

        execute("update test set message='bar1' where pk_col='123'");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("select message from test where pk_col='123'");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo("bar1");
    }

    @Test
    public void testUpdateByIdWithMultiplePrimaryKeyAndClusteredBy() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) clustered by(author) with (number_of_replicas=0)");
        execute("insert into quotes (id, author, quote) values(?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day."});
        assertThat(response).hasRowCount(1L);

        execute("update quotes set quote=? where id=1 and author='Ford'",
            new Object[]{"Don't panic"});
        assertThat(response).hasRowCount(1L);
        refresh();
        execute("select quote from quotes where id=1 and author='Ford'");
        assertThat(response).hasRowCount(1L);
        assertThat((String) response.rows()[0][0]).isEqualTo("Don't panic");
    }

    @Test
    public void testUpdateByQueryWithMultiplePrimaryKeyAndClusteredBy() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) clustered by(author) with (number_of_replicas=0)");
        execute("insert into quotes (id, author, quote) values(?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day."});
        assertThat(response).hasRowCount(1L);
        refresh();

        execute("update quotes set quote=? where id=1",
            new Object[]{"Don't panic"});
        assertThat(response).hasRowCount(1L);
        refresh();

        execute("select quote from quotes where id=1 and author='Ford'");
        assertThat(response).hasRowCount(1L);
        assertThat((String) response.rows()[0][0]).isEqualTo("Don't panic");
    }

    @Test
    public void testUpdateVersionHandling() throws Exception {
        execute("create table test (id int primary key, c int) with (number_of_replicas=0, refresh_interval=0)");
        execute("insert into test (id, c) values (1, 1)");
        execute("refresh table test");
        execute("select _version, c from test");

        long version = (Long) response.rows()[0][0];
        assertThat(version).isEqualTo(1L);

        // with primary key optimization:

        execute("update test set c = 2 where id = 1 and _version = 1"); // this one works
        assertThat(response).hasRowCount(1L);
        execute("update test set c = 3 where id = 1 and _version = 1"); // this doesn't
        assertThat(response).hasRowCount(0L);

        execute("refresh table test");
        execute("select _version, c from test");
        assertThat(response.rows()[0][0]).isEqualTo(2L);
        assertThat(response.rows()[0][1]).isEqualTo(2);

    }

    @Test
    public void testMultiUpdateWithSequenceConflict() {
        execute("create table test (id int primary key, c int)");
        execute("insert into test (id, c) values (1, 1), (2, 1)");
        refresh();

        execute("select id, _seq_no, _primary_term from test order by id");

        // 2nd will result in conflict, but 1st one was successful and must be replicated
        long wrongSeqNoForSecondRow = (long) response.rows()[1][1] - 1;
        execute(
            "update test set c = 3 where (id = ? and _seq_no = ? and _primary_term = ?) or (id = ? and _seq_no = ? and _primary_term = ?)",
            new Object[]{response.rows()[0][0], response.rows()[0][1], response.rows()[0][2],
                response.rows()[1][0], wrongSeqNoForSecondRow, response.rows()[1][2]});
        assertThat(response).hasRowCount(1L);
    }

    @Test
    public void testMultiUpdateWithVersionAndConflict() throws Exception {
        execute("create table test (id int primary key, c int)");
        execute("insert into test (id, c) values (1, 1), (2, 1)");
        refresh();

        // update 2nd row in order to increase version
        execute("update test set c = 2 where id = 2");
        refresh();

        // now update both rows, 2nd will result in conflict, but 1st one was successful and must be replicated
        execute("update test set c = 3 where (id = 1 and _version = 1) or (id = 2 and _version = 1)");
        assertThat(response).hasRowCount(1L);

        refresh();
        execute("select _version from test order by id");
        assertThat(response.rows()[0][0]).isEqualTo(2L);
        assertThat(response.rows()[1][0]).isEqualTo(2L);
    }

    @Test
    public void testUpdateVersionOrOperator() throws Exception {
        execute("create table test (id int primary key, c int) with (number_of_replicas=0, refresh_interval=0)");
        execute("insert into test (id, c) values (1, 1)");
        execute("refresh table test");

        Asserts.assertSQLError(() -> execute(
                        "update test set c = 4 where _version = 2 or _version=1"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(BAD_REQUEST, 4000)
                .hasMessageContaining(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testUpdateVersionInOperator() throws Exception {
        execute("create table test (id int primary key, c int) with (number_of_replicas=0, refresh_interval=0)");
        ensureGreen();
        execute("insert into test (id, c) values (1, 1)");
        execute("refresh table test");

        Asserts.assertSQLError(() -> execute(
                        "update test set c = 4 where _version in (1,2)"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(BAD_REQUEST, 4000)
                .hasMessageContaining(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }


    @Test
    public void testUpdateRetryOnVersionConflict() throws Exception {
        // issue a bulk update request updating the same document to force a version conflict
        execute("create table test (a string, b int) with (number_of_replicas=0)");
        execute("insert into test (a, b) values ('foo', 1)");
        assertThat(response).hasRowCount(1L);
        refresh();

        long[] rowCounts = execute("update test set a = ? where b = ?",
            new Object[][]{
                new Object[]{"bar", 1},
                new Object[]{"baz", 1},
                new Object[]{"foobar", 1}});
        assertThat(rowCounts).isEqualTo(new long[] { 1L, 1L, 1L });
        refresh();

        // document was changed 4 times (including initial creation), so version must be 4
        execute("select _version from test where b = 1");
        assertThat(response.rows()[0][0]).isEqualTo(4L);
    }

    @Test
    public void testUpdateByIdPartitionColumnPartOfPrimaryKey() throws Exception {
        execute("create table party (" +
                "  id int primary key, " +
                "  type byte primary key, " +
                "  value string" +
                ") partitioned by (type) with (number_of_replicas=0)");
        execute("insert into party (id, type, value) values (?, ?, ?)", new Object[][]{
            {1, 2, "foo"},
            {2, 3, "bar"},
            {2, 4, "baz"}
        });
        execute("refresh table party");

        execute("update party set value='updated' where (id=1 and type=2) or (id=2 and type=4)");
        assertThat(response).hasRowCount(2L);

        execute("refresh table party");

        execute("select id, type, value from party order by id, value");
        assertThat(response).hasRows(
            "1| 2| updated",
            "2| 3| bar",
            "2| 4| updated");

    }

    @Test
    public void testBulkUpdateWithOnlyOneBulkArgIsProducingRowCountResult() throws Exception {
        execute("create table t (name string) with (number_of_replicas = 0)");
        // regression test, used to throw a ClassCastException because the JobLauncher created a
        // QueryResult instead of RowCountResult
        long[] rowCounts = execute("update t set name = 'Trillian' where name = ?", $$($("Arthur")));
        assertThat(rowCounts.length).isEqualTo(1);
    }

    @Test
    public void testBulkUpdateWithPKAndMultipleHits() throws Exception {
        execute("create table t (id integer primary key, name string) with (number_of_replicas = 0)");
        execute("insert into t values (?, ?)", $$($(1, "foo"), $(2, "bar"), $(3, "hoschi"), $(4, "crate")));
        refresh();

        long[] rowCounts = execute("update t set name = 'updated' where id = ? or id = ?", $$($(1, 2), $(3, 4)));
        assertThat(rowCounts).isEqualTo(new long[] { 2L, 2L });
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
        execute("insert into generated_column (id, ts, \"user\") values (?, ?, ?)", new Object[]{
            1, "2015-11-18T11:11:00", MapBuilder.newMapBuilder().put("name", "foo").map()});
        refresh();
        execute("update generated_column set ts = ?, \"user\" = ? where id = ?", new Object[]{
            "2015-11-19T17:06:00", MapBuilder.newMapBuilder().put("name", "zoo").map(), 1});
        refresh();
        execute("select day, name from generated_column");
        assertThat(response.rows()[0][0]).isEqualTo(1447891200000L);
        assertThat(response.rows()[0][1]).isEqualTo("zoobar");
    }

    @Test
    public void testGeneratedColumnWithoutRefsToOtherColumnsComputedOnUpdate() {
        execute("create table generated_column (" +
                " \"inserted\" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS current_timestamp(3), " +
                " \"message\" STRING" +
                ")");
        execute("insert into generated_column (message) values (?)", new Object[]{"str"});
        refresh();
        execute("select inserted from generated_column");
        long ts = (long) response.rows()[0][0];
        execute("update generated_column set message = ?", new Object[]{"test"});
        refresh();
        execute("select inserted from generated_column");
        assertThat(response.rows()[0][0]).isNotEqualTo(ts);
    }

    @Test
    public void testUpdateSetInvalidGeneratedColumnOnly() {
        execute("create table computed (" +
                " ts timestamp with time zone," +
                " gen_col as extract(year from ts)" +
                ") with (number_of_replicas=0)");
        execute("insert into computed (ts) values (1)");
        refresh();

        execute("update computed set gen_col=1745");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testUpdateNotNullSourceGeneratedColumn() {
        execute("create table generated_column (" +
                " id int primary key," +
                " ts timestamp with time zone," +
                " gen_col as extract(year from ts) not null" +
                ") with (number_of_replicas=0)");
        execute("insert into generated_column (id, ts) values (1, '2015-11-18T11:11:00')");
        assertThat(response).hasRowCount(1);
        refresh();

        Asserts.assertSQLError(() -> execute(
                        "update generated_column set ts=null where id=1"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
                .hasMessageContaining("\"gen_col\" must not be null");

    }

    @Test
    public void testUpdateNotNullTargetGeneratedColumn() {
        execute("create table generated_column (" +
                " id int primary key," +
                " ts timestamp with time zone," +
                " gen_col as extract(year from ts) not null" +
                ") with (number_of_replicas=0)");
        execute("insert into generated_column (id, ts) values (1, '2015-11-18T11:11:00')");
        assertThat(response).hasRowCount(1);
        refresh();

        Asserts.assertSQLError(() -> execute(
                        "update generated_column set gen_col=null where id=1"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(BAD_REQUEST, 4000)
                .hasMessageContaining("\"gen_col\" must not be null");

    }

    @Test
    public void testUpdateWithGeneratedColumnSomeReferencesUpdated() throws Exception {
        execute("create table computed (" +
                " firstname string," +
                " surname string," +
                " name as concat(surname, ', ', firstname)" +
                ") with (number_of_replicas=0)");
        execute("insert into computed (firstname, surname) values ('Douglas', 'Adams')");
        refresh();
        execute("update computed set firstname = 'Ford'");
        refresh();
        execute("select name from computed");
        assertThat(response.rows()[0][0]).isEqualTo("Adams, Ford");
    }

    @Test
    public void testUpdateExpressionReferenceGeneratedColumn() throws Exception {
        execute("create table computed (" +
                " a int," +
                " b int," +
                " c as (b + 1)" +
                ") with (number_of_replicas=0)");
        execute("insert into computed (a, b) values (1, 2)");
        refresh();
        execute("update computed set a = c + 1");
        refresh();
        execute("select a from computed");
        assertThat(response.rows()[0][0]).isEqualTo(4);
    }

    @Test
    public void testUpdateReferencedByGeneratedColumnWithExpressionReferenceGeneratedColumn() throws Exception {
        execute("create table computed (" +
                " a int," +
                " b as (a + 1)" +
                ") with (number_of_replicas=0)");
        execute("insert into computed (a) values (1)");
        refresh();
        execute("update computed set a = b + 1");
        refresh();
        execute("select a from computed");
        assertThat(response.rows()[0][0]).isEqualTo(3);
    }

    @Test
    public void testFailingUpdateBulkOperation() throws Exception {
        execute("create table t (x string) clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into t (x) values ('1')");
        execute("refresh table t");

        // invalid regex causes failure in prepare phase, causing a failure row-count in each individual response
        Object[][] bulkArgs = new Object[][] {
            new Object[] { 1, "+123" },
            new Object[] { 2, "+123" },
        };
        long[] rowCounts = execute("update t set x = ? where x ~* ?", bulkArgs);
        assertThat(rowCounts).isEqualTo(new long[] { -2L, -2L });
    }

    @Test
    public void testUpdateByQueryWithSubQuery() throws Exception {
        execute("create table t1 (x int)");
        execute("insert into t1 (x) values (1), (2)");
        execute("refresh table t1");
        execute("update t1 set x = (select 3) where x = (select 1)");
        assertThat(response).hasRowCount(1L);

        execute("refresh table t1");
        execute("select x from t1 order by x asc");
        assertThat(response).hasRows(
            "2",
            "3");
    }

    @Test
    public void testUpdateByIdWithSubQuery() throws Exception {
        execute("create table t1 (id int primary key, name string)");
        execute("insert into t1 (id, name) values (1, 'Arthur'), (2, 'Trillian')");
        execute("refresh table t1");
        execute("update t1 set name = (select 'Slartibartfast') where id = (select 1)");
        assertThat(response).hasRowCount(1L);

        execute("refresh table t1");
        execute("select id, name from t1 order by id asc");
        assertThat(response).hasRows(
            "1| Slartibartfast",
            "2| Trillian");
    }

    @Test
    public void test_update_by_id_returning_id() throws Exception {
        execute("create table test (id int primary key, message string) clustered into 2 shards");
        execute("insert into test values(1, 'msg');");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set message='msg' where id = 1 returning id");

        assertThat(response.cols()[0]).isEqualTo("id");
        assertThat(response).hasRows("1");
    }

    @Test
    public void test_update_by_id_returning_id_with_outputname() throws Exception {
        execute("create table test (id int primary key, message string) clustered into 2 shards");
        execute("insert into test values(1, 'msg');");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set message='msg' where id = 1 returning id as renamed");

        assertThat(response.cols()[0]).isEqualTo("renamed");
        assertThat(response).hasRows("1");
    }

    @Test
    public void test_update_by_id_with_subquery_returning_id() throws Exception {
        execute("create table test (id int primary key, message string) clustered into 2 shards");
        execute("insert into test values(1, 'msg');");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set message='updated' where id = (select 1) returning id");

        assertThat(response).hasRows("1");
    }

    @Test
    public void test_update_by_id_where_no_row_is_matching() throws Exception {
        execute("create table test (id int primary key, message string) clustered into 2 shards");
        execute("insert into test values(1, 'msg');");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set message='updated' where id = 99 returning id");

        assertThat(response.cols()[0]).isEqualTo("id");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void test_update_by_query_returning_single_field_with_outputputname() throws Exception {
        execute("create table test (id int primary key, message string) clustered into 2 shards");
        execute("insert into test values(1, 'msg');");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set message='updated' where message='msg' returning message as message_renamed");

        assertThat(response).hasRowCount(1L);
        assertThat(response.cols()[0]).isEqualTo("message_renamed");
        assertThat(response.rows()[0][0]).isEqualTo("updated");
    }


    @Test
    public void test_update_by_query_with_subquery_returning_multiple_fields() throws Exception {
        execute("create table test (id int primary key, message string) clustered into 2 shards");
        execute("insert into test values(1, 'msg');");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set message='updated' where message= (select 'msg') returning id, message");

        assertThat(response).hasRowCount(1L);
        assertThat(response.cols()[0]).isEqualTo("id");
        assertThat(response.cols()[1]).isEqualTo("message");
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isEqualTo("updated");

    }

    @Test
    public void test_update_by_query_returning_multiple_results() throws Exception {
        execute("create table test (id int primary key, x int, message string) clustered into 2 shards");
        execute("insert into test values(1, 1, 'msg') returning _seq_no;");
        long fstSeqNo = (long) response.rows()[0][0];
        execute("insert into test values(2, 1, 'msg') returning _seq_no;");
        long sndSeqNo = (long) response.rows()[0][0];
        assertThat(response).hasRowCount(1);
        refresh();

        execute("update test set message='updated' where message='msg' and x > 0 " +
                "returning id, _seq_no as seq, message as message_renamed");

        assertThat(response).hasRowCount(2L);
        assertThat(response.cols()[0]).isEqualTo("id");
        assertThat(response.cols()[1]).isEqualTo("seq");
        assertThat(response.cols()[2]).isEqualTo("message_renamed");

        int fstRowIndex = response.rows()[0][0].equals(1) ? 0 : 1;
        assertThat((long) response.rows()[fstRowIndex][1]).isGreaterThan(fstSeqNo);
        assertThat(response.rows()[fstRowIndex][2]).isEqualTo("updated");

        int sndRowIndex = fstRowIndex == 0 ? 1 : 0;

        assertThat((long) response.rows()[sndRowIndex][1]).isGreaterThan(sndSeqNo);
        assertThat(response.rows()[sndRowIndex][2]).isEqualTo("updated");
    }

    @Test
    public void test_update_sys_tables_returning_values_with_expressions_and_outputnames() throws Exception {
        execute("update sys.node_checks set acknowledged = true where id = 1 " +
                "returning id, UPPER(description) as description, acknowledged as ack");

        long numberOfNodes = this.clusterService().state().nodes().getSize();

        assertThat(response).hasRowCount(numberOfNodes);
        assertThat(response.cols()[0]).isEqualTo("id");
        assertThat(response.cols()[1]).isEqualTo("description");
        assertThat(response.cols()[2]).isEqualTo("ack");
        for (int i = 0; i < numberOfNodes; i++) {
            assertThat(response.rows()[i][0]).isEqualTo(1);
            assertThat(response.rows()[i][1]).isNotNull();
            assertThat(response.rows()[i][2]).isEqualTo(true);
        }
    }

    @Test
    public void test_update_preserves_the_top_level_order_implied_by_set_clause_while_dynamically_adding_columns() {
        execute("create table t (x int) partitioned by (x) with (column_policy='dynamic')");
        execute("insert into t values (1)");
        refresh();
        execute("update t set b=1, a=1, d=1, c=1");
        execute("select * from t");
        // the same order as provided by 'update t set b=1, a=1, d=1, c=1'
        assertThat(response).hasColumns("x", "b", "a", "d", "c");
    }

    @Test
    public void test_update_preserves_the_sub_column_order_implied_by_set_clause_while_dynamically_adding_columns() {
        execute("create table doc.t (id int primary key) with (column_policy='dynamic')");
        execute("insert into doc.t values (1)");
        refresh();
        execute("update doc.t set o = {c=1, a={d=1, b=1, c=1, a=1}, b=1}");
        execute("show create table doc.t");
        assertThat(printedTable(response.rows()))
            // the same order as provided by 'update t set o = {c=1, a={d=1, b=1, c=1, a=1}, b=1}'
            .contains(
                "CREATE TABLE IF NOT EXISTS \"doc\".\"t\" (\n" +
                "   \"id\" INTEGER NOT NULL,\n" +
                "   \"o\" OBJECT(DYNAMIC) AS (\n" +
                "      \"c\" BIGINT,\n" +
                "      \"a\" OBJECT(DYNAMIC) AS (\n" +
                "         \"d\" BIGINT,\n" +
                "         \"b\" BIGINT,\n" +
                "         \"c\" BIGINT,\n" +
                "         \"a\" BIGINT\n" +
                "      ),\n" +
                "      \"b\" BIGINT\n" +
                "   ),\n" +
                "   PRIMARY KEY (\"id\")\n" +
                ")"
            );
    }

    @Test
    public void test_update_array_by_elements() {
        execute("create table t (a int[], idx int, value int)");
        execute("insert into t values ([1,2,3], 2, 10)");
        refresh();
        // equivalent to 'update t set a[1] = 11, a[1] = 7, a[2] = a[1]'
        execute("update t set a[1] = value + 1, a[a[1]] = 7, a[a[1]+1] = a[1]");
        refresh();
        execute("select a from t");
        // the second assignment overrides the first
        // the third assignment should not be affected by the first two
        assertThat(printedTable(response.rows())).isEqualTo("[7, 1, 3]\n");
    }

    @UseJdbc(0)
    @Test
    public void test_update_object_array_by_elements_with_column_policies() {
        execute("create table t (a array(object(dynamic)))");
        execute("insert into t values ([{}])");
        refresh();
        execute("update t set a[1] = {c=1}");
        refresh();
        execute("select a['c'][1] from t");
        assertThat(printedTable(response.rows())).isEqualTo("1\n");

        execute("create table t2 (a array(object(strict)))");
        assertThatThrownBy(() -> execute("update t2 set a[1] = {c=1}"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column a['c'] unknown");
    }

    @Test
    public void test_update_on_table_with_big_refresh_interval_emptied_by_delete_return_0_rows() {
        execute("create table test (a int) with (refresh_interval = 10000)");
        execute("insert into test (a) values (1)");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("delete from test");
        assertThat(response).hasRowCount(1);

        // No refresh() here, collect doesn't now that table is already empty,
        // which leads to ShardUpsertRequest to be issued.
        // We ensure that no exception is thrown and UPDATE behaves similar to DELETE, returns 0 rows.
        execute("update test set a = 2");
        assertThat(response).hasRowCount(0L);

        // Another similar scenario of update, which could hit multiple rows.
        execute("update test set a = 2 where a = 1");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void test_update_continues_on_error() {
        execute("create table test (a int CHECK (a < 100))");

        execute("insert into test (a) values (1), (2), (3)");
        assertThat(response).hasRowCount(3);
        refresh();

        execute("update test set a = a + 98");
        assertThat(response).hasRowCount(1);
    }
}
