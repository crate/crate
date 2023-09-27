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
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.assertj.core.data.Offset;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.IntegTestCase;
import org.joda.time.Period;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.spatial4j.shape.Point;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import io.crate.analyze.validator.SemanticSortValidator;
import io.crate.common.collections.Lists2;
import io.crate.exceptions.SQLExceptions;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.testing.Asserts;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.ObjectType.Builder;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class TransportSQLActionTest extends IntegTestCase {

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSelectResultContainsColumnsInTheOrderOfTheSelectListInTheQuery() throws Exception {
        execute("create table test (id string primary key)");
        execute("insert into test (id) values ('id1')");
        execute("refresh table test");

        execute("select \"_id\" as b, \"_version\" as a from test");
        assertArrayEquals(new String[]{"b", "a"}, response.cols());
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testIndexNotFoundExceptionIsRaisedIfDeletedAfterPlan() throws Throwable {
        execute("create table t (name string)");
        ensureYellow();

        PlanForNode plan = plan("select * from t");
        execute("drop table t");

        assertThatThrownBy(() -> execute(plan).getResult())
            .isExactlyInstanceOf(IndexNotFoundException.class);
    }

    @Test
    public void testDeletePartitionAfterPlan() throws Throwable {
        execute("CREATE TABLE parted_table (id long, text string, day timestamp with time zone) " +
                "PARTITIONED BY (day)");
        execute("INSERT INTO parted_table (id, text, day) values(1, 'test', '2016-06-07')");
        ensureYellow();

        PlanForNode plan = plan("delete from parted_table where day='2016-06-07'");
        execute("delete from parted_table where day='2016-06-07'");
        try {
            execute(plan).getResult();
        } catch (Throwable t) {
            throw SQLExceptions.unwrap(t);
        }
    }

    @Test
    public void testSelectCountStar() throws Exception {
        execute("create table test (name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (name) values (?)", new Object[]{"Arthur"});
        execute("insert into test (name) values (?)", new Object[]{"Trillian"});
        refresh();
        execute("select count(*) from test");
        assertEquals(1, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
    }

    @Test
    public void testSelectZeroLimit() throws Exception {
        execute("create table test (name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (name) values (?)", new Object[]{"Arthur"});
        execute("insert into test (name) values (?)", new Object[]{"Trillian"});
        refresh();
        execute("select * from test limit 0");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testSelectZeroLimitOrderBy() throws Exception {
        execute("create table test (name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (name) values (?)", new Object[]{"Arthur"});
        execute("insert into test (name) values (?)", new Object[]{"Trillian"});
        refresh();
        execute("select * from test order by name limit 0");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testSelectCountStarWithWhereClause() throws Exception {
        execute("create table test (name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (name) values (?)", new Object[]{"Arthur"});
        execute("insert into test (name) values (?)", new Object[]{"Trillian"});
        refresh();
        execute("select count(*) from test where name = 'Trillian'");
        assertEquals(1, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testSelectStar() throws Exception {
        execute("create table test (\"firstName\" string, \"lastName\" string)");
        ensureYellow();
        execute("select * from test");
        assertArrayEquals(new String[]{"firstName", "lastName"}, response.cols());
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testSelectStarEmptyMapping() throws Exception {
        execute("select * from unnest()");
        assertArrayEquals(new String[]{}, response.cols());
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGroupByOnAnalyzedColumn() throws Exception {
        execute("create table test1 (col1 string index using fulltext)");
        execute("insert into test1 (col1) values ('abc def, ghi. jkl')");
        refresh();
        assertThat(execute("select count(col1) from test1 group by col1")).hasRows(
            "1"
        );
    }

    @Test
    public void testSelectStarWithOther() throws Exception {
        execute("create table test (id string primary key, first_name string, last_name string)");
        execute("insert into test (id, first_name, last_name) values ('id1', 'Youri', 'Zoon')");
        execute("refresh table test");

        execute("select \"_version\", *, \"_id\" from test");
        assertThat(response)
            .hasColumns("_version", "id", "first_name", "last_name", "_id")
            .hasRowCount(1)
            .hasRows(new Object[]{1L, "id1", "Youri", "Zoon", "id1"});
    }

    @Test
    @UseJdbc(0) // $1 style parameter substitution not supported
    public void testSelectWithParams() throws Exception {
        execute("create table test (id string primary key, first_name string, last_name string, age double) " +
                "with (number_of_replicas = 0)");

        execute("insert into test (id, first_name, last_name, age) values ('id1', 'Youri', 'Zoon', 38)");
        execute("refresh table test");

        Object[] args = new Object[]{"id1"};
        execute("select first_name, last_name from test where \"_id\" = $1", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);

        args = new Object[]{"Zoon"};
        execute("select first_name, last_name from test where last_name = $1", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);

        args = new Object[]{38, "Zoon"};
        execute("select first_name, last_name from test where age = $1 and last_name = $2", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);

        args = new Object[]{38, "Zoon"};
        execute("select first_name, last_name from test where age = ? and last_name = ?", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);
    }

    @Test
    public void testSelectStarWithOtherAndAlias() throws Exception {
        execute("create table test (first_name string, last_name string)");
        execute("insert into test (first_name, last_name) values ('Youri', 'Zoon')");
        execute("refresh table test");
        execute("select *, \"_version\", \"_version\" as v from test");
        assertArrayEquals(new String[]{"first_name", "last_name", "_version", "v"}, response.cols());
        assertEquals(1, response.rowCount());
        assertArrayEquals(new Object[]{"Youri", "Zoon", 1L, 1L}, response.rows()[0]);
    }

    @Test
    public void testFilterByEmptyString() throws Exception {
        execute("create table test (name string)");
        execute("insert into test (name) values (''), ('Ruben Lenten')");
        execute("refresh table test");

        execute("select name from test where name = ''");
        assertEquals(1, response.rowCount());
        assertEquals("", response.rows()[0][0]);

        execute("select name from test where name != ''");
        assertEquals(1, response.rowCount());
        assertEquals("Ruben Lenten", response.rows()[0][0]);

    }

    @Test
    public void testFilterByNull() throws Exception {
        execute("create table test (id int, name string, o object(ignored))");
        ensureYellow();

        execute("insert into test (id) values (1)");
        execute("insert into test (id, name) values (2, 'Ruben Lenten'), (3, '')");
        refresh();

        execute("select id from test where name is null");
        assertEquals(1, response.rowCount());
        assertEquals(1, response.rows()[0][0]);

        execute("select id from test where name is not null order by id");
        assertEquals(2, response.rowCount());
        assertEquals(2, response.rows()[0][0]);

        execute("select id from test where o['invalid'] is null");
        assertEquals(3, response.rowCount());

        execute("select name from test where name is not null and name != ''");
        assertEquals(1, response.rowCount());
        assertEquals("Ruben Lenten", response.rows()[0][0]);
    }

    @Test
    public void testFilterByBoolean() throws Exception {
        execute("create table test (sunshine boolean)");
        execute("insert into test values (?)", new Object[]{true});
        refresh();

        execute("select sunshine from test where sunshine = true");
        assertEquals(1, response.rowCount());
        assertEquals(true, response.rows()[0][0]);

        execute("update test set sunshine=false where sunshine = true");
        assertEquals(1, response.rowCount());
        refresh();

        execute("select sunshine from test where sunshine = ?", new Object[]{false});
        assertEquals(1, response.rowCount());
        assertEquals(false, response.rows()[0][0]);
    }


    /**
     * Queries are case sensitive by default, however column names without quotes are converted
     * to lowercase which is the same behaviour as in postgres
     * see also http://www.thenextage.com/wordpress/postgresql-case-sensitivity-part-1-the-ddl/
     *
     */
    @Test
    public void testColsAreCaseSensitive() throws Exception {
        execute("create table test (\"firstname\" string, \"firstName\" string) " +
                "with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into test (\"firstname\", \"firstName\") values ('LowerCase', 'CamelCase')");
        refresh();

        execute("select FIRSTNAME, \"firstname\", \"firstName\" from test");
        assertArrayEquals(new String[]{"firstname", "firstname", "firstName"}, response.cols());
        assertEquals(1, response.rowCount());
        assertEquals("LowerCase", response.rows()[0][0]);
        assertEquals("LowerCase", response.rows()[0][1]);
        assertEquals("CamelCase", response.rows()[0][2]);
    }


    @Test
    public void testIdSelectWithResult() throws Exception {
        execute("create table test (id string primary key)");
        execute("insert into test (id) values ('id1')");
        execute("refresh table test");

        execute("select \"_id\" from test");
        assertArrayEquals(new String[]{"_id"}, response.cols());
        assertEquals(1, response.rowCount());
        assertEquals(1, response.rows()[0].length);
        assertEquals("id1", response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithLimit() throws Exception {
        execute("create table test (id string primary key)");
        execute("insert into test (id) values ('id1'), ('id2')");
        execute("refresh table test");
        execute("select \"_id\" from test limit 1");
        assertEquals(1, response.rowCount());
    }


    @Test
    public void testSqlRequestWithLimitAndOffset() throws Exception {
        execute("create table test (id string primary key) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (id) values (?), (?), (?)", new Object[]{"id1", "id2", "id3"});
        refresh();
        execute("select \"id\" from test order by id limit 1 offset 1");
        assertEquals(1, response.rowCount());
        assertThat(response).hasRows(
            "id2"
        );
    }


    @UseRandomizedOptimizerRules(0)
    @Test
    public void testSqlRequestWithFilter() throws Exception {
        execute("create table test (id string primary key)");
        execute("insert into test (id) values ('id1'), ('id2')");
        execute("select _id from test where id = 'id1'");
        assertThat(response).hasRows(
            "id1"
        );
    }

    @Test
    public void testSqlRequestWithNotEqual() throws Exception {
        execute("create table test (id string primary key) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into test (id) values (?)", new Object[][]{
            new Object[]{"id1"},
            new Object[]{"id2"}
        });
        refresh();
        execute("select id from test where id != 'id1'");
        assertThat(response).hasRows("id2");
    }


    @Test
    public void testSqlRequestWithOneOrFilter() throws Exception {
        execute("create table test (id string) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into test (id) values ('id1'), ('id2'), ('id3')");
        refresh();
        execute("select id from test where id='id1' or id='id3'");
        assertEquals(2, response.rowCount());
        assertThat(response).hasRowsInAnyOrder(
            $("id1"),
            $("id3")
        );
    }

    @Test
    public void testSqlRequestWithOneMultipleOrFilter() throws Exception {
        execute("create table test (id string) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into test (id) values ('id1'), ('id2'), ('id3'), ('id4')");
        refresh();
        execute("select id from test where id='id1' or id='id2' or id='id4'");
        assertThat(response).hasRowsInAnyOrder(
            $("id1"),
            $("id2"),
            $("id4")
        );
    }

    @Test
    public void testSqlRequestWithDateFilter() {
        execute("create table test (date timestamp with time zone)");
        execute("insert into test (date) values ('2013-10-01'), ('2013-10-02')");
        execute("refresh table test");
        execute("select date from test where date = '2013-10-01'");
        assertThat(response).hasRows("1380585600000");
    }

    @Test
    public void testSqlRequestWithDateGtFilter() {
        execute("create table test (date timestamp with time zone)");
        execute("insert into test (date) values ('2013-10-01'), ('2013-10-02')");
        execute("refresh table test");
        execute(
            "select date from test where date > '2013-10-01'");
        assertThat(response).hasRows("1380672000000");
    }

    @Test
    public void testSqlRequestWithNumericGtFilter() throws Exception {
        execute("create table test (i long)");
        execute("insert into test (i) values (10), (20)");
        execute("refresh table test");
        execute(
            "select i from test where i > 10");
        assertThat(response).hasRows("20");
    }


    @Test
    public void testArraySupport() throws Exception {
        execute("create table t1 (id int primary key, strings array(string), integers array(integer)) with (number_of_replicas=0)");
        execute("insert into t1 (id, strings, integers) values (?, ?, ?)",
            new Object[]{
                1,
                new String[]{"foo", "bar"},
                new Integer[]{1, 2, 3}
            }
        );
        refresh();
        execute("select id, strings, integers from t1");
        assertThat(response).hasRows(
            "1| [foo, bar]| [1, 2, 3]"
        );
    }

    @Test
    public void testArrayInsideObject() throws Exception {
        execute("create table t1 (id int primary key, details object as (names array(string))) with (number_of_replicas=0)");
        Map<String, Object> details = new HashMap<>();
        details.put("names", new Object[]{"Arthur", "Trillian"});
        execute("insert into t1 (id, details) values (?, ?)", new Object[]{1, details});
        refresh();

        assertThat(execute("select details['names'] from t1")).hasRows(
            "[Arthur, Trillian]"
        );
    }

    @Test
    public void testArrayInsideObjectArrayOutputAndQueryBehaviour() throws Exception {
        execute("create table t1 (id int primary key, details array(object as (names array(string)))) with (number_of_replicas=0)");
        ensureYellow();

        Map<String, Object> detail1 = new HashMap<>();
        detail1.put("names", new Object[]{"Arthur", "Trillian"});

        Map<String, Object> detail2 = new HashMap<>();
        detail2.put("names", new Object[]{"Ford", "Slarti", "Ford"});

        List<Map<String, Object>> details = Arrays.asList(detail1, detail2);

        execute("insert into t1 (id, details) values (?, ?)", new Object[]{1, details});
        refresh();

        execute("select " +
                "details['names'], ['Arthur', 'Trillian'] = ANY (details['names']) " +
                "from t1 " +
                "where ['Ford', 'Slarti', 'Ford'] = ANY (details['names'])");
        assertThat(response).hasRows(
            "[[Arthur, Trillian], [Ford, Slarti, Ford]]| true"
        );
    }

    @Test
    public void testFullPathRequirement() throws Exception {
        // verifies that the "fullPath" setting in the es mapping is no longer required
        execute("create table t1 (id int primary key, details object as (id int, more_details object as (id int))) with (number_of_replicas=0)");
        ensureYellow();

        Map<String, Object> more_details = new HashMap<>();
        more_details.put("id", 2);

        Map<String, Object> details = new HashMap<>();
        details.put("id", 1);
        details.put("more_details", more_details);

        execute("insert into t1 (id, details) values (2, ?)", new Object[]{details});
        execute("refresh table t1");

        execute("select details from t1 where details['id'] = 2");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testArraySupportWithNullValues() throws Exception {
        execute("create table t1 (id int primary key, strings array(string)) with (number_of_replicas=0)");
        execute("insert into t1 (id, strings) values (?, ?)",
            new Object[]{
                1,
                new String[]{"foo", null, "bar"},
            }
        );
        refresh();

        execute("select id, strings from t1");
        assertThat(response).hasRows(
            "1| [foo, null, bar]"
        );
    }

    @Test
    public void testObjectArrayInsertAndSelect() throws Exception {
        execute("create table t1 (" +
                "  id int primary key, " +
                "  objects array(" +
                "   object as (" +
                "     name string, " +
                "     age int" +
                "   )" +
                "  )" +
                ") with (number_of_replicas=0)");
        Map<String, Object> obj1 = Map.of("name", "foo", "age", 1);
        Map<String, Object> obj2 = Map.of("name", "bar", "age", 2);

        Object[] args = new Object[]{1, new Object[]{obj1, obj2}};
        execute("insert into t1 (id, objects) values (?, ?)", args);
        refresh();

        execute("select objects from t1");
        assertThat(response).hasRows(
            "[{name=foo, age=1}, {name=bar, age=2}]"
        );
        execute("select objects['name'] from t1");
        assertThat(response).hasRows("[foo, bar]");

        execute("select objects['name'] from t1 where ? = ANY (objects['name'])", new Object[]{"foo"});
        assertThat(response).hasRows("[foo, bar]");
    }

    @Test
    public void testGetResponseWithObjectColumn() throws Exception {
        execute("create table test (" +
                "   id text primary key," +
                "   data object " +
                ")");

        Map<String, Object> data = new HashMap<>();
        data.put("foo", "bar");
        execute("insert into test (id, data) values (?, ?)", new Object[]{"1", data});
        refresh();

        execute("select data from test where id = ?", new Object[]{"1"});
        assertThat(response).hasRows($(data));
    }

    @Test
    public void testSelectToGetRequestByPlanner() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('124', 'bar1')");
        assertEquals(1, response.rowCount());
        refresh();
        waitNoPendingTasksOnAll(); // wait for mapping update as foo is being added

        execute("select pk_col, message from test where pk_col='124'");
        assertThat(response).hasRows("124| bar1");
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void testSelectToRoutedRequestByPlanner() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");

        execute("SELECT * FROM test WHERE pk_col='1' OR pk_col='2'");
        assertThat(response).hasRowCount(2);

        execute("SELECT * FROM test WHERE pk_col=? OR pk_col=?", new Object[]{"1", "2"});
        assertThat(response).hasRowCount(2);

        execute("SELECT * FROM test WHERE (pk_col=? OR pk_col=?) OR pk_col=?", new Object[]{"1", "2", "3"});
        assertThat(response).hasRowCount(3);
        assertThat(response).hasColumns("pk_col", "message");
    }

    @Test
    public void testSelectToRoutedRequestByPlannerMissingDocuments() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");
        refresh();

        execute("SELECT pk_col, message FROM test WHERE pk_col='4' OR pk_col='3'");
        assertThat(response).hasRows(
            "3| baz"
        );

        execute("SELECT pk_col, message FROM test WHERE pk_col='4' OR pk_col='99'");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testSelectToRoutedRequestByPlannerWhereIn() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");
        refresh();

        execute("SELECT * FROM test WHERE pk_col IN (?,?,?)", new Object[]{"1", "2", "3"});
        assertThat(response).hasRowCount(3);
    }

    @Test
    public void testUpdateToRoutedRequestByPlannerWhereOr() throws Exception {
        this.setup.createTestTableWithPrimaryKey();
        execute("insert into test (pk_col, message) values ('1', 'foo'), ('2', 'bar'), ('3', 'baz')");
        refresh();
        execute("update test set message='new' WHERE pk_col='1' or pk_col='2' or pk_col='4'");
        assertThat(response).hasRowCount(2);
        refresh();
        execute("SELECT distinct message FROM test");
        assertThat(response).hasRowCount(2);
    }

    @Test
    public void testSelectWithWhereLike() throws Exception {
        this.setup.groupBySetup();

        execute("select name from characters where name like '%ltz'");
        assertThat(response).hasRowCount(2);

        execute("select count(*) from characters where name like 'Jeltz'");
        assertThat(response).hasRows("1");

        execute("select count(*) from characters where race like '%o%'");
        assertThat(response).hasRows("3");

        Map<String, Object> emptyMap = new HashMap<>();
        Map<String, Object> details = new HashMap<>();
        details.put("age", 30);
        details.put("job", "soldier");
        execute("insert into characters (race, gender, name, details) values (?, ?, ?, ?)",
            new Object[]{"Vo*", "male", "Kwaltzz", emptyMap}
        );
        execute("insert into characters (race, gender, name, details) values (?, ?, ?, ?)",
            new Object[]{"Vo?", "male", "Kwaltzzz", emptyMap}
        );
        execute("insert into characters (race, gender, name, details) values (?, ?, ?, ?)",
            new Object[]{"Vo!", "male", "Kwaltzzzz", details}
        );
        execute("insert into characters (race, gender, name, details) values (?, ?, ?, ?)",
            new Object[]{"Vo%", "male", "Kwaltzzzz", details}
        );
        refresh();

        execute("select race from characters where race like 'Vo*'");
        assertEquals(1L, response.rowCount());
        assertEquals("Vo*", response.rows()[0][0]);

        execute("select race from characters where race like ?", new Object[]{"Vo?"});
        assertEquals(1L, response.rowCount());
        assertEquals("Vo?", response.rows()[0][0]);

        execute("select race from characters where race like 'Vo!'");
        assertEquals(1L, response.rowCount());
        assertEquals("Vo!", response.rows()[0][0]);

        execute("select race from characters where race like 'Vo\\%'");
        assertEquals(1L, response.rowCount());
        assertEquals("Vo%", response.rows()[0][0]);

        execute("select race from characters where race like 'Vo_'");
        assertEquals(4L, response.rowCount());

        execute("select race from characters where details['job'] like 'sol%'");
        assertEquals(2L, response.rowCount());
    }

    private void nonExistingColumnSetup() {
        execute("create table quotes (" +
                "id integer primary key, " +
                "quote string index off, " +
                "o object(ignored) " +
                ") clustered by (id) into 3 shards with (number_of_replicas = 0)");
        execute("insert into quotes (id, quote) values (1, '\"Nothing particularly exciting," +
                "\" it admitted, \"but they are alternatives.\"')");
        execute("insert into quotes (id, quote) values (2, '\"Have another drink," +
                "\" said Trillian. \"Enjoy yourself.\"')");
        refresh();
    }

    @Test
    public void selectNonExistingColumn() throws Exception {
        nonExistingColumnSetup();
        execute("select o['notExisting'] from quotes");
        assertEquals(2L, response.rowCount());
        assertEquals("o['notExisting']", response.cols()[0]);
        assertNull(response.rows()[0][0]);
        assertNull(response.rows()[1][0]);
    }

    @Test
    public void selectNonExistingAndExistingColumns() throws Exception {
        nonExistingColumnSetup();
        execute("select o['unknown'], id from quotes order by id asc");
        assertEquals(2L, response.rowCount());
        assertEquals("o['unknown']", response.cols()[0]);
        assertEquals("id", response.cols()[1]);
        assertNull(response.rows()[0][0]);
        assertEquals(1, response.rows()[0][1]);
        assertNull(response.rows()[1][0]);
        assertEquals(2, response.rows()[1][1]);
    }

    @Test
    public void selectWhereNonExistingColumn() throws Exception {
        nonExistingColumnSetup();
        execute("select * from quotes where o['something'] > 0");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereDynamicColumnIsNull() throws Exception {
        nonExistingColumnSetup();
        execute("select * from quotes where o['something'] IS NULL");
        assertEquals(2, response.rowCount());
    }

    @Test
    public void selectWhereNonExistingColumnWhereIn() throws Exception {
        nonExistingColumnSetup();
        execute("select * from quotes where o['something'] IN (1,2,3)");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereNonExistingColumnLike() throws Exception {
        nonExistingColumnSetup();
        execute("select * from quotes where o['something'] Like '%bla'");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereNonExistingColumnMatchFunction() throws Exception {
        nonExistingColumnSetup();

        Asserts.assertSQLError(() -> execute("select * from quotes where match(o['something'], 'bla')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Can only use MATCH on columns of type STRING or GEO_SHAPE, not on 'undefined'");
    }

    @Test
    public void testSelectCountDistinctZero() throws Exception {
        execute("create table test (col1 int) with (number_of_replicas=0)");
        ensureYellow();

        execute("select count(distinct col1) from test");

        assertEquals(1, response.rowCount());
        assertEquals(0L, response.rows()[0][0]);
    }

    @Test
    public void testRefresh() throws Exception {
        execute("create table test (id int primary key, name string) with (refresh_interval = 0)");
        ensureYellow();
        execute("insert into test (id, name) values (0, 'Trillian'), (1, 'Ford'), (2, 'Zaphod')");
        execute("select count(*) from test");
        assertThat((long) response.rows()[0][0]).isLessThanOrEqualTo(3L);

        execute("refresh table test");
        assertThat(response).hasRowCount(1L);

        execute("select count(*) from test");
        assertThat(response).hasRows("3");
    }

    @Test
    public void testInsertSelectWithClusteredBy() throws Exception {
        execute("create table quotes (id integer, quote string) clustered by(id) " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote) values(?, ?)",
            new Object[]{1, "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select \"_id\", id, quote from quotes where id=1");
        assertEquals(1L, response.rowCount());

        // Validate generated _id, must be: <generatedRandom>
        assertThat((String) response.rows()[0][0])
            .isNotNull()
            .hasSize(UUIDs.base64UUID().length());
    }

    @Test
    public void testInsertSelectWithAutoGeneratedId() throws Exception {
        execute("create table quotes (id integer, quote string)" +
                "with (number_of_replicas=0)");
        execute("insert into quotes (id, quote) values(?, ?)",
            new Object[]{1, "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select \"_id\", id, quote from quotes where id=1");
        assertEquals(1L, response.rowCount());

        // Validate generated _id, must be: <generatedRandom>
        assertThat((String) response.rows()[0][0])
            .isNotNull()
            .hasSize(UUIDs.base64UUID().length());
    }

    @Test
    public void testInsertSelectWithPrimaryKey() throws Exception {
        execute("create table quotes (id integer primary key, quote string)" +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote) values(?, ?)",
            new Object[]{1, "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select \"_id\", id, quote from quotes where id=1");
        assertEquals(1L, response.rowCount());

        // Validate generated _id.
        // Must equal to id because its the primary key
        String _id = (String) response.rows()[0][0];
        Integer id = (Integer) response.rows()[0][1];
        assertEquals(id.toString(), _id);
    }

    @Test
    public void testInsertSelectWithMultiplePrimaryKey() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) with (number_of_replicas=0)");
        execute("insert into quotes (id, author, quote) values(?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select \"_id\", id from quotes where id=1 and author='Ford'");
        assertEquals(1L, response.rowCount());
        assertThat(response).hasRows(
            "AgExBEZvcmQ=| 1"
        );
    }

    @Test
    public void testInsertSelectWithMultiplePrimaryKeyAndClusteredBy() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) clustered by(author) with (number_of_replicas=0)");
        execute("insert into quotes (id, author, quote) values(?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select \"_id\", id from quotes where id=1 and author='Ford'");
        assertEquals(1L, response.rowCount());
        assertThat(response).hasRows(
            "AgRGb3JkATE=| 1"
        );
    }

    @Test
    public void testInsertSelectWithMultiplePrimaryOnePkSame() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) clustered by(author) with (number_of_replicas=0)");
        execute("insert into quotes (id, author, quote) values (?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day.",
                1, "Douglas", "Don't panic"}
        );
        assertThat(response).hasRowCount(2);
        refresh();

        execute("select \"_id\", id from quotes where id=1 order by author");
        assertThat(response).hasRows(
            "AgdEb3VnbGFzATE=| 1",
            "AgRGb3JkATE=| 1"
        );
    }

    @Test
    public void testSelectWhereBoolean() {
        execute("create table a (v boolean)");
        ensureYellow();

        execute("insert into a values (true)");
        execute("insert into a values (true)");
        execute("insert into a values (true)");
        execute("insert into a values (false)");
        execute("insert into a values (false)");
        refresh();

        execute("select v from a where v");
        assertEquals(3L, response.rowCount());

        execute("select v from a where not v");
        assertEquals(2L, response.rowCount());

        execute("select v from a where v or not v");
        assertEquals(5L, response.rowCount());

        execute("select v from a where v and not v");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testSelectWhereBooleanPK() {
        execute("create table b (v boolean primary key) clustered by (v)");
        ensureYellow();

        execute("insert into b values (true)");
        execute("insert into b values (false)");
        refresh();

        execute("select v from b where v");
        assertEquals(1L, response.rowCount());

        execute("select v from b where not v");
        assertEquals(1L, response.rowCount());

        execute("select v from b where v or not v");
        assertEquals(2L, response.rowCount());

        execute("select v from b where v and not v");
        assertEquals(0L, response.rowCount());
    }


    @Test
    public void testBulkOperations() throws Exception {
        execute("create table test (id integer primary key, name string) with (number_of_replicas = 0)");
        ensureYellow();
        long[] rowCounts = execute("insert into test (id, name) values (?, ?), (?, ?)",
            new Object[][]{
                {1, "Earth", 2, "Saturn"},    // bulk row 1
                {3, "Moon", 4, "Mars"}        // bulk row 2
            });
        assertThat(rowCounts).hasSize(2);
        for (long rowCount : rowCounts) {
            assertThat(rowCount).isEqualTo(2);
        }
        refresh();

        rowCounts = execute("insert into test (id, name) values (?, ?), (?, ?)",
            new Object[][]{
                {1, "Earth", 2, "Saturn"},    // bulk row 1
                {3, "Moon", 4, "Mars"}        // bulk row 2
            });
        assertThat(rowCounts).hasSize(2);
        for (long rowCount : rowCounts) {
            assertThat(rowCount).isEqualTo(-2L);
        }

        execute("select name from test order by id asc");
        assertEquals("Earth\nSaturn\nMoon\nMars\n", printedTable(response.rows()));

        // test bulk update-by-id
        rowCounts = execute("update test set name = concat(name, '-updated') where id = ?", new Object[][]{
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
        });
        assertThat(rowCounts).hasSize(3);
        for (long rowCount : rowCounts) {
            assertThat(rowCount).isEqualTo(1L);
        }
        refresh();

        execute("select count(*) from test where name like '%-updated'");
        assertThat(response).hasRows("3");

        // test bulk of delete-by-id
        rowCounts = execute("delete from test where id = ?", new Object[][]{
            new Object[]{1},
            new Object[]{3}
        });
        assertThat(rowCounts).hasSize(2);
        for (long rowCount : rowCounts) {
            assertThat(rowCount).isEqualTo(1L);
        }
        refresh();

        execute("select count(*) from test");
        assertThat(response).hasRows("2");

        // test bulk of delete-by-query
        rowCounts = execute("delete from test where name = ?", new Object[][]{
            new Object[]{"Saturn-updated"},
            new Object[]{"Mars-updated"}
        });
        assertThat(rowCounts).hasSize(2);
        for (long rowCount : rowCounts) {
            assertThat(rowCount).isEqualTo(1);
        }
        refresh();

        execute("select count(*) from test");
        assertThat(response).hasRows("0");

    }

    @Test
    public void testSelectFormatFunction() throws Exception {
        execute("create table tbl (name text, kind text)");
        execute("insert into tbl (name, kind) values ('', 'Planet'), ('Aldebaran', 'Star System'), ('Algol', 'Star System')");
        execute("refresh table tbl");
        execute("select format('%s is a %s', name, kind) as sentence from tbl order by name");
        assertThat(response).hasColumns("sentence");
        assertThat(response).hasRows(
            " is a Planet",
            "Aldebaran is a Star System",
            "Algol is a Star System"
        );

    }

    @Test
    public void testAnyArray() throws Exception {
        this.setup.setUpArrayTables();

        execute("select count(*) from any_table where 'Berlin' = ANY (names)");
        assertThat(response).hasRows("2");

        execute("select id, names from any_table where 'Berlin' = ANY (names) order by id");
        assertThat(response).hasRows(
            "1| [Dornbirn, Berlin, St. Margrethen]",
            "3| [Hangelsberg, Berlin]"
        );

        execute("select id from any_table where 'Berlin' != ANY (names) order by id");
        assertThat(response).hasRows(
            "1",
            "2",
            "3"
        );

        execute("select count(id) from any_table where 0.0 < ANY (temps)");
        assertThat(response).hasRows("2");

        execute("select id, names from any_table where 0.0 < ANY (temps) order by id");
        assertThat(response).hasRows(
            "2| [Dornbirn, Dornbirn, Dornbirn]",
            "3| [Hangelsberg, Berlin]"
        );

        execute("select count(*) from any_table where 0.0 > ANY (temps)");
        assertThat(response).hasRows("2");

        execute("select id, names from any_table where 0.0 > ANY (temps) order by id");
        assertThat(response).hasRows(
            "2| [Dornbirn, Dornbirn, Dornbirn]",
            "3| [Hangelsberg, Berlin]"
        );

        execute("select id, names from any_table where 'Ber%' LIKE ANY (names) order by id");
        assertThat(response).hasRows(
            "1| [Dornbirn, Berlin, St. Margrethen]",
            "3| [Hangelsberg, Berlin]"
        );
    }

    @Test
    public void testNotAnyArray() throws Exception {
        this.setup.setUpArrayTables();

        execute("select id from any_table where NOT 'Hangelsberg' = ANY (names) order by id");
        assertThat(response).hasRows(
            "1",
            "2"
        );

        execute("select id from any_table where 'Hangelsberg' != ANY (names) order by id");
        assertThat(response).hasRows(
            "1",
            "2",
            "3"
        );
    }

    @Test
    public void testAnyLike() throws Exception {
        this.setup.setUpArrayTables();

        execute("select id from any_table where 'kuh%' LIKE ANY (tags) order by id");
        assertThat(response).hasRows(
            "3",
            "4"
        );

        execute("select id from any_table where 'kuh%' NOT LIKE ANY (tags) order by id");
        assertThat(response).hasRows(
            "1",
            "2",
            "3"
        );
    }

    @Test
    public void testInsertAndSelectIpType() throws Exception {
        execute("create table ip_table (fqdn string, addr ip) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into ip_table (fqdn, addr) values ('localhost', '127.0.0.1'), ('crate.io', '23.235.33.143')");
        execute("refresh table ip_table");

        execute("select addr from ip_table where addr = '23.235.33.143'");
        assertThat(response).hasRows("23.235.33.143");

        execute("select addr from ip_table where addr > '127.0.0.1'");
        assertThat(response).hasRowCount(0);
        execute("select addr from ip_table where addr > 2130706433"); // 2130706433 == 127.0.0.1
        assertThat(response).hasRowCount(0);

        execute("select addr from ip_table where addr < '127.0.0.1'");
        assertThat(response).hasRows("23.235.33.143");
        execute("select addr from ip_table where addr < 2130706433"); // 2130706433 == 127.0.0.1
        assertThat(response).hasRows("23.235.33.143");

        execute("select addr from ip_table where addr <= '127.0.0.1'");
        assertThat(response).hasRowCount(2L);

        execute("select addr from ip_table where addr >= '23.235.33.143'");
        assertThat(response).hasRowCount(2L);

        execute("select addr from ip_table where addr IS NULL");
        assertThat(response).hasRowCount(0L);

        execute("select addr from ip_table where addr IS NOT NULL");
        assertThat(response).hasRowCount(2L);

    }

    @Test
    public void testGroupByOnIpType() throws Exception {
        execute("create table t (i ip) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i) values ('192.168.1.2'), ('192.168.1.2'), ('192.168.1.3')");
        execute("refresh table t");
        execute("select i, count(*) from t group by 1 order by count(*) desc");

        assertThat(response).hasRows(
            "192.168.1.2| 2",
            "192.168.1.3| 1"
        );
    }

    @Test
    public void testInsertAndSelectGeoType() throws Exception {
        execute("create table geo_point_table (id int primary key, p geo_point) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into geo_point_table (id, p) values (?, ?)", new Object[]{1, new Double[]{47.22, 12.09}});
        execute("insert into geo_point_table (id, p) values (?, ?)", new Object[]{2, new Double[]{57.22, 7.12}});
        refresh();

        execute("select p from geo_point_table order by id desc");

        assertThat(response).hasRowCount(2L);
        Point p1 = (Point) response.rows()[0][0];
        assertThat(p1.getX()).isCloseTo(57.22, Offset.offset(0.01d));
        assertThat(p1.getY()).isCloseTo(7.12, Offset.offset(0.01d));

        Point p2 = (Point) response.rows()[1][0];
        assertThat(p2.getX()).isCloseTo(47.22, Offset.offset(0.01));
        assertThat(p2.getY()).isCloseTo(12.09, Offset.offset(0.01));
    }

    @Test
    public void testGeoTypeQueries() throws Exception {
        // setup
        execute("create table t (id int primary key, i int, p geo_point) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (id, i, p) values (1, 1, 'POINT (10 20)')");
        execute("insert into t (id, i, p) values (2, 1, 'POINT (11 21)')");
        refresh();

        // order by
        execute("select distance(p, 'POINT (11 21)') from t order by 1");
        assertThat(response).hasRowCount(2L);

        Double result1 = (Double) response.rows()[0][0];
        Double result2 = (Double) response.rows()[1][0];

        assertThat(result1).isEqualTo(0.0d);
        assertThat(result2).isEqualTo(152354.32308347954);

        String stmtOrderBy = "SELECT id " +
                             "FROM t " +
                             "ORDER BY distance(p, 'POINT(30.0 30.0)')";
        execute(stmtOrderBy);
        assertThat(response).hasRowCount(2L);
        String expectedOrderBy =
            "2\n" +
            "1\n";
        assertEquals(expectedOrderBy, printedTable(response.rows()));

        // aggregation (max())
        String stmtAggregate = "SELECT i, max(distance(p, 'POINT(30.0 30.0)')) " +
                               "FROM t " +
                               "GROUP BY i";
        execute(stmtAggregate);
        assertThat(response).hasRowCount(1L);
        String expectedAggregate = "1| 2296582.8899438097\n";
        assertEquals(expectedAggregate, printedTable(response.rows()));

        Point point;
        // queries
        execute("select p from t where distance(p, 'POINT (11 21)') > 0.0");
        assertThat(response).hasRowCount(1L);
        point = (Point) response.rows()[0][0];
        assertThat(point.getX()).isCloseTo(10.0d, Offset.offset(0.01));
        assertThat(point.getY()).isCloseTo(20.0d, Offset.offset(0.02));

        execute("select p from t where distance(p, 'POINT (11 21)') < 10.0");
        assertThat(response).hasRowCount(1L);
        point = (Point) response.rows()[0][0];
        assertThat(point.getX()).isCloseTo(11.0d, Offset.offset(0.01));
        assertThat(point.getY()).isCloseTo(21.0d, Offset.offset(0.01));

        execute("select p from t where distance(p, 'POINT (11 21)') < 10.0 or distance(p, 'POINT (11 21)') > 10.0");
        assertThat(response).hasRowCount(2L);

        execute("select p from t where distance(p, 'POINT (10 20)') >= 0.0 and distance(p, 'POINT (10 20)') <= 0.1");
        assertThat(response).hasRowCount(1L);
        point = (Point) response.rows()[0][0];
        assertThat(point.getX()).isCloseTo(10.0d, Offset.offset(0.01));
        assertThat(point.getY()).isCloseTo(20.0d, Offset.offset(0.01));

        execute("select p from t where distance(p, 'POINT (10 20)') = 0.0");
        assertThat(response).hasRowCount(1L);
        point = (Point) response.rows()[0][0];
        assertThat(point.getX()).isCloseTo(10.0d, Offset.offset(0.01));
        assertThat(point.getY()).isCloseTo(20.0d, Offset.offset(0.01));

        execute("select p from t where distance(p, 'POINT (10 20)') = 152354.3209044634");
        Point p = (Point) response.rows()[0][0];
        assertThat(p.getX()).isCloseTo(11.0, Offset.offset(0.01));
        assertThat(p.getY()).isCloseTo(21.0, Offset.offset(0.01));
    }

    @Test
    @UseJdbc(0) // test-layer converts Map to string, but for geo-shape strings should be WKT, not JSON.
    public void testWithinQuery() throws Exception {
        execute("create table t (id int primary key, p geo_point) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (id, p) values (1, 'POINT (10 10)')");
        refresh();

        execute("select within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))') from t");
        assertThat(response).hasRows("true");

        execute("select * from t where within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))')");
        assertThat(response).hasRowCount(1L);
        execute("select * from t where within(p, ?)", $(Map.of(
            "type", "Polygon",
            "coordinates", new double[][][]{
                {
                    {5.0, 5.0},
                    {30.0, 5.0},
                    {30.0, 30.0},
                    {5.0, 30.0},
                    {5.0, 5.0}
                }
            }
        )));
        assertThat(response).hasRowCount(1L);

        execute("select * from t where within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))') = false");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testTwoSubStrOnSameColumn() throws Exception {
        execute("select substr(name, 0, 4), substr(name, 4, 8) from sys.cluster");
        assertThat(response).hasRows("SUIT| TE-TEST_");
    }


    @Test
    public void testSelectArithMetricOperatorInOrderBy() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 3 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (2, 5, 90.5), (193384, 31234594433, 99.0), (10, 21, 99.0), (-1, 4, 99.0)");
        refresh();

        execute("select i, i%3 from t order by i%3, l");
        assertThat(response).hasRowCount(5L);
        assertThat(response).hasRows(
            "-1| -1",
            "1| 1",
            "10| 1",
            "193384| 1",
            "2| 2");
    }

    @Test
    public void testSelectFailingSearchScript() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5)");
        refresh();

        Asserts.assertSQLError(() -> execute("select log(d, l) from t where log(d, -1) >= 0"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("log(x, b): given arguments would result in: 'NaN'");
    }

    @Test
    public void testSelectGroupByFailingSearchScript() throws Exception {
        execute("create table t (i integer, l long, d double) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (0, 4, 100)");
        execute("refresh table t");

        Asserts.assertSQLError(() -> execute("select log(d, l) from t where log(d, -1) >= 0 group by log(d, l)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("log(x, b): given arguments would result in: 'NaN'");
    }

    @Test
    public void testNumericScriptOnAllTypes() {
        // this test validates that no exception is thrown
        execute(
            "create table t (" +
            "   b byte," +
            "   s short," +
            "   i integer," +
            "   l long," +
            "   f float," +
            "   d double," +
            "   t timestamp with time zone" +
            ") with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (b, s, i, l, f, d, t) values (1, 2, 3, 4, 5.7, 6.3, '2014-07-30')");
        refresh();

        String[] functionCalls = new String[]{
            "abs(%s)",
            "ceil(%s)",
            "floor(%s)",
            "ln(%s)",
            "log(%s)",
            "log(%s, 2)",
            "random()",
            "round(%s)",
            "sqrt(%s)"
        };

        for (String functionCall : functionCalls) {
            String byteCall = String.format(Locale.ENGLISH, functionCall, "b");
            execute(String.format(Locale.ENGLISH, "select %s, b from t where %s < 2", byteCall, byteCall));

            String shortCall = String.format(Locale.ENGLISH, functionCall, "s");
            execute(String.format(Locale.ENGLISH, "select %s, s from t where %s < 2", shortCall, shortCall));

            String intCall = String.format(Locale.ENGLISH, functionCall, "i");
            execute(String.format(Locale.ENGLISH, "select %s, i from t where %s < 2", intCall, intCall));

            String longCall = String.format(Locale.ENGLISH, functionCall, "l");
            execute(String.format(Locale.ENGLISH, "select %s, l from t where %s < 2", longCall, longCall));

            String floatCall = String.format(Locale.ENGLISH, functionCall, "f");
            execute(String.format(Locale.ENGLISH, "select %s, f from t where %s < 2", floatCall, floatCall));

            String doubleCall = String.format(Locale.ENGLISH, functionCall, "d");
            execute(String.format(Locale.ENGLISH, "select %s, d from t where %s < 2", doubleCall, doubleCall));
        }
    }

    @Test
    public void testWhereColumnEqColumnAndFunctionEqFunction() throws Exception {
        execute("create table tbl (name text)");
        execute("insert into tbl (name) values ('Arthur'), ('Trillian')");
        execute("refresh table tbl");

        execute("select name from tbl where name = name");
        assertThat(response).hasRowCount(2L);

        execute("select name from tbl where substr(name, 1, 1) = substr(name, 1, 1)");
        assertThat(response).hasRowCount(2L);
    }

    @Test
    public void testNewColumn() throws Exception {
        execute("create table t (name string) with (number_of_replicas=0, column_policy = 'dynamic')");
        execute("insert into t (name, score) values ('Ford', 1.2)");
    }

    @Test
    public void testESGetSourceColumns() throws Exception {
        this.setup.setUpLocations();
        ensureYellow();
        refresh();

        execute("select _id, _version from locations where id=2");
        assertNotNull(response.rows()[0][0]);
        assertNotNull(response.rows()[0][1]);

        execute("select _id, name from locations where id=2");
        assertNotNull(response.rows()[0][0]);
        assertNotNull(response.rows()[0][1]);

        execute("select _id, _doc from locations where id=2");
        assertNotNull(response.rows()[0][0]);
        assertNotNull(response.rows()[0][1]);

        execute("select _doc, id from locations where id in (2,3) order by id");
        Map<String, Object> _doc1 = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> _doc2 = (Map<String, Object>) response.rows()[1][0];
        assertEquals(_doc1.get("id"), "2");
        assertEquals(_doc2.get("id"), "3");

        execute("select name, kind from locations where id in (2,3) order by id");
        assertEquals(printedTable(response.rows()), "Outer Eastern Rim| Galaxy\n" +
                                                    "Galactic Sector QQ7 Active J Gamma| Galaxy\n");

        execute("select name, kind, _id from locations where id in (2,3) order by id");
        assertEquals(printedTable(response.rows()), "Outer Eastern Rim| Galaxy| 2\n" +
                                                    "Galactic Sector QQ7 Active J Gamma| Galaxy| 3\n");

        execute("select _raw, id from locations where id in (2,3) order by id");
        Map<String, Object> firstRaw = JsonXContent.JSON_XCONTENT.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            (String) response.rows()[0][0]).map();

        assertThat(response.rows()[0][1]).isEqualTo("2");
        assertThat(firstRaw).containsEntry("id", "2");
        assertThat(firstRaw).containsEntry("name", "Outer Eastern Rim");
        assertThat(firstRaw).containsEntry("date", 308534400000L);
        assertThat(firstRaw).containsEntry("kind", "Galaxy");

        Map<String, Object> secondRaw = JsonXContent.JSON_XCONTENT.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            (String) response.rows()[1][0]).map();
        assertThat(response.rows()[1][1]).isEqualTo("3");
        assertThat(secondRaw).containsEntry("id", "3");
        assertThat(secondRaw).containsEntry("name", "Galactic Sector QQ7 Active J Gamma");
        assertThat(secondRaw).containsEntry("date", 1367366400000L);
        assertThat(secondRaw).containsEntry("kind", "Galaxy");
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testUnknownTableJobGetsRemoved() throws Exception {
        String uniqueId = UUID.randomUUID().toString();
        String stmtStr = "select '" + uniqueId + "' from foobar";
        String stmtStrWhere = "select ''" + uniqueId + "'' from foobar";
        Asserts.assertSQLError(() -> execute(stmtStr))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'foobar' unknown");
        execute("select stmt from sys.jobs where stmt='" + stmtStrWhere + "'");
        assertEquals(response.rowCount(), 0L);
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void testInsertAndCopyHaveSameIdGeneration() throws Exception {
        execute("create table t (" +
                "id1 long primary key," +
                "id2 long primary key," +
                "ts timestamp with time zone primary key," +
                "n string) " +
                "clustered by (id2)");
        ensureYellow();

        execute("insert into t (id1, id2, ts, n) values (176406344, 1825712027, 1433635200000, 'foo')");
        try {
            execute("insert into t (id1, id2, ts, n) values (176406344, 1825712027, 1433635200000, 'bar')");
            fail("should fail because document exists already");
        } catch (Exception e) {
            // good
        }
        execute("refresh table t");

        File file = folder.newFolder();
        String uriTemplate = Paths.get(file.toURI()).toUri().toString();
        execute("copy t to directory ?", new Object[]{uriTemplate});
        execute("copy t from ? with (shared=true)", new Object[]{uriTemplate + "/*"});
        execute("refresh table t");

        execute("select _id, * from t");
        assertThat(response).hasRowCount(1L);
    }


    @Test
    public void testSelectFrom_Doc() throws Exception {
        execute("create table t (name string) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (name) values ('Marvin')");
        execute("refresh table t");

        execute("select _doc['name'] from t");
        assertThat(response).hasRows("Marvin");
    }

    @Test
    public void testSelectWithSingleBulkArgRaisesUnsupportedError() {
        Asserts.assertSQLError(() -> execute("select * from sys.cluster", new Object[1][0]))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Bulk operations for statements that return result sets is not supported");
    }

    @Test
    public void testSelectWithBulkArgsRaisesUnsupportedError() {
        Asserts.assertSQLError(() -> execute("select * from sys.cluster", new Object[][]{new Object[]{1}, new Object[]{2}}))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Bulk operations for statements that return result sets is not supported");
    }

    @Test
    public void testWeirdIdentifiersAndLiterals() throws Exception {
        execute("CREATE TABLE with_quote (\"\"\"\" string) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("INSERT INTO with_quote (\"\"\"\") VALUES ('''')");
        execute("REFRESH TABLE with_quote");
        execute("SELECT * FROM with_quote");
        assertThat(response)
            .hasColumns("\"")
            .hasRows("'");
    }

    @Test
    public void testWhereNotNull() throws Exception {
        execute("create table t (b boolean, i int) with (number_of_replicas=0)");
        execute("insert into t (b, i) values (true, 1), (false, 2), (null, null)");
        execute("refresh table t");

        execute("select b, not b, not (b > i::boolean) from t order by b");
        assertThat(response).hasRows(
            "false| true| true",
            "true| false| true",
            "NULL| NULL| NULL"
        );

        execute("select b, i from t where not b");
        assertThat(response).hasRowCount(1L);

        execute("select b, i from t where not b > i::boolean");
        assertThat(response).hasRowCount(2L);

        execute("SELECt b, i FROM t WHERE NOT (i = 1 AND b = TRUE)");
        assertThat(response).hasRowCount(1L);

        execute("SELECT b, i FROM t WHERE NOT (i IS NULL OR b IS NULL)");
        assertThat(response).hasRowCount(2L);

        execute("SELECT b FROM t WHERE NOT (coalesce(b, true))");
        assertThat(response).hasRowCount(1L);

        execute("SELECT b, i FROM t WHERE NOT (coalesce(b, false) = true AND i IS NULL)");
        assertThat(response).hasRowCount(2L);
    }

    @Test
    public void testScalarEvaluatesInErrorOnDocTable() throws Exception {
        execute("create table t1 (id int) with (number_of_replicas=0)");
        ensureYellow();
        // we need at least 1 row, otherwise the table is empty and no evaluation occurs
        execute("insert into t1 (id) values (1)");
        refresh();

        Asserts.assertSQLError(() -> execute("select 1/0 from t1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("/ by zero");
    }

    /**
     * If the WHERE CLAUSE results in a NO MATCH, no expression evaluation error will be thrown as nothing is evaluated.
     * This is different from e.g. postgres where evaluation always occurs.
     */
    @Test
    public void testScalarEvaluatesInErrorOnDocTableNoMatch() throws Exception {
        execute("create table t1 (id int) with (number_of_replicas=0)");
        ensureYellow();
        execute("select 1/0 from t1 where true = false");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testOrderByWorksOnSymbolWithLateNormalization() throws Exception {
        execute("create table t (x int) with (number_of_replicas = 0)");
        execute("insert into t (x) values (5), (10), (3)");
        execute("refresh table t");

        // cast is added to have a to_int(2) after the subquery is inserted
        // this causes a normalization on the map-side which used to remove the order by
        execute("select cast((select 2) as integer) * x from t order by 1");
        assertThat(response).hasRows(
            "6",
            "10",
            "20");
    }

    @Test
    public void testOrderOnAnalyzedColumn() {
        execute("create table t (text string index using fulltext) with (number_of_replicas = 0)");
        execute("insert into t (text) values ('Hello World'), ('The End')");
        execute("refresh table t");

        assertThat(execute("select text from t order by 1 desc")).hasRows(
            "The End",
            "Hello World"
        );
    }

    @Test
    public void test_values_as_top_level_relation() {
        execute("VALUES (1, 'a'), (2, 'b'), (3, (SELECT 'c'))");
        assertThat(response).hasRows(
            "1| a",
            "2| b",
            "3| c"
        );
    }


    @Test
    public void test_select_with_explicit_high_limit_and_query_that_does_not_match_anything_to_trigger_batch_size_optimization() {
        execute("create table tbl (x int) clustered into 1 shards");
        Object[][] values = new Object[1001][];
        for (int i = 0; i < values.length; i++) {
            values[i] = new Object[] { i };
        }
        execute("insert into tbl (x) values (?)", values);
        execute("refresh table tbl");

        // This tests a regression where we optimized the internal batchSize down to 0, which caused a failure as this is
        // not supported by the TopFieldCollector
        execute("select x from tbl where x > 5000 order by x limit 1500");
    }

    @Test
    public void test_select_from_virtual_table_with_window_function_and_column_pruning() throws Exception {
        // push down of columns used in WHERE on top of join results in column pruning
        // This triggered a bug in the prune implementation of the WindowAgg operator
        execute("create table doc.metrics (id int primary key, x int)");
        execute("SELECT"
            + "    * "
            + " FROM ("
            + "     SELECT"
            + "         n.nspname,"
            + "         c.relname,"
            + "         a.attname,"
            + "         a.atttypid,"
            + "         a.attnotnull"
            + "         OR (t.typtype = 'd'"
            + "             AND t.typnotnull) AS attnotnull,"
            + "         a.atttypmod,"
            + "         a.attlen,"
            + "         row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,"
            + "         pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,"
            + "         dsc.description,"
            + "         t.typbasetype,"
            + "         t.typtype"
            + "     FROM"
            + "         pg_catalog.pg_namespace n"
            + "         JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)"
            + "         JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid)"
            + "         JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)"
            + "         LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid = def.adrelid"
            + "                 AND a.attnum = def.adnum)"
            + "         LEFT JOIN pg_catalog.pg_description dsc ON (c.oid = dsc.objoid"
            + "                 AND a.attnum = dsc.objsubid)"
            + "         LEFT JOIN pg_catalog.pg_class dc ON (dc.oid = dsc.classoid"
            + "                 AND dc.relname = 'pg_class')"
            + "         LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace = dn.oid"
            + "                 AND dn.nspname = 'pg_catalog')"
            + "     WHERE"
            + "         c.relkind IN ('r', 'v', 'f', 'm')"
            + "         AND a.attnum > 0"
            + "         AND NOT a.attisdropped"
            + "         AND c.relname LIKE E'metrics') c"
            + " WHERE"
            + "     TRUE"
            + " ORDER BY"
            + "     nspname,"
            + "     c.relname,"
            + "     attnum");
        assertThat(response).hasRows(
            "doc| metrics| id| 23| true| -1| 4| 1| NULL| NULL| 0| b",
            "doc| metrics| x| 23| false| -1| 4| 2| NULL| NULL| 0| b"
        );
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_subscript_on_ignored_object_does_not_raise_missing_key_error() throws Exception {
        execute("create table tbl (obj object (ignored))");
        execute("insert into tbl (obj) values ({a = 10})");
        execute("refresh table tbl");

        execute("select * from tbl where obj['b'] = 10");
        assertThat(response).hasRows("");

        execute("select * from (select * from tbl) as t where obj['b'] = 10");
        assertThat(response).hasRows("");
    }


    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_primary_key_lookup_on_multiple_columns() throws Exception {
        execute(
            "CREATE TABLE doc.test (" +
            "   id TEXT, " +
            "   region_level_1 TEXT, " +
            "   marker_type TEXT, " +
            "   is_auto BOOLEAN," +
            "   PRIMARY KEY (id, region_level_1, marker_type, is_auto) " +
            ") clustered into 1 shards "
        );
        execute("insert into doc.test (id, region_level_1, marker_type, is_auto) values ('1', '2', '3', false)");
        execute("refresh table doc.test");
        execute("explain (costs false) select id from doc.test where id = '1' and region_level_1 = '2' and marker_type = '3' and is_auto = false;");
        assertThat(response).hasRows(
            "Get[doc.test | id | DocKeys{'1', '2', '3', false} | ((((id = '1') AND (region_level_1 = '2')) AND (marker_type = '3')) AND (is_auto = false))]"
        );
        execute("select id from doc.test where id = '1' and region_level_1 = '2' and marker_type = '3' and is_auto = false;");
        assertThat(response).hasRows(
            "1"
        );
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_primary_key_lookup_with_param_that_requires_cast_to_column_type() throws Exception {
        execute("create table tbl (ts timestamp with time zone primary key, path text primary key)");
        execute("INSERT INTO tbl (ts, path) VALUES ('2017-06-21T00:00:00.000000Z', 'c1')");

        execute("select _id, path from tbl where ts = '2017-06-21' and path = 'c1'");
        assertThat(response).hasRows(
            "Ag0xNDk4MDAzMjAwMDAwAmMx| c1"
        );
        execute("select _id, path from tbl where ts = ? and path = ?", new Object[] { "2017-06-21", "c1" });
        assertThat(response).hasRows(
            "Ag0xNDk4MDAzMjAwMDAwAmMx| c1"
        );
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_primary_key_lookups_returns_inserted_records() throws Exception {
        int numKeys = randomIntBetween(1, 3);
        Random random = RandomizedContext.current().getRandom();
        StringBuilder createTable = new StringBuilder("CREATE TABLE tbl (");
        ArrayList<DataType<?>> types = new ArrayList<>();
        List<DataType<?>> typeCandidates = DataTypes.PRIMITIVE_TYPES.stream()
            .filter(x -> x.storageSupport() != null)
            .collect(Collectors.toList());
        for (int i = 0; i < numKeys; i++) {
            var type = RandomPicks.randomFrom(random, typeCandidates);
            types.add(type);
            createTable.append("col");
            createTable.append(i);
            createTable.append(' ');
            createTable.append(type.getName());
            createTable.append(" PRIMARY KEY");
            if (i + 1 < numKeys) {
                createTable.append(", ");
            }
        }
        createTable.append(")");
        execute(createTable.toString());

        String insert = "INSERT INTO tbl VALUES ("
            + String.join(", ", Lists2.map(types, ignored -> "?"))
            + ")";
        Object[] args = new Object[types.size()];
        for (int i = 0; i < types.size(); i++) {
            var type = types.get(i);
            args[i] = DataTypeTesting.getDataGenerator(type).get();
        }
        execute(insert, args);

        StringBuilder selectInlineValues = new StringBuilder("SELECT 1 FROM tbl WHERE ");
        StringBuilder selectParams = new StringBuilder("SELECT 1 FROM tbl WHERE ");
        for (int i = 0; i < args.length; i++) {
            var arg = args[i];
            selectInlineValues.append("col");
            selectParams.append("col");

            selectInlineValues.append(i);
            selectParams.append(i);

            selectInlineValues.append(" = ");
            if (arg instanceof String) {
                selectInlineValues.append("'");
                selectInlineValues.append(arg);
                selectInlineValues.append("'");
            } else {
                selectInlineValues.append(arg);
            }
            selectParams.append(" = ?");

            if (i + 1 < args.length) {
                selectInlineValues.append(" AND ");
                selectParams.append(" AND ");
            }
        }

        execute(selectParams.toString(), args);
        assertThat(response)
            .as(selectParams.toString() + " with values " + Arrays.toString(args) + " must return a record")
            .hasRowCount(1L);

        execute(selectInlineValues.toString());
        assertThat(response)
            .as(selectInlineValues.toString() + " must return a record")
            .hasRowCount(1L);
    }


    @Test
    public void test_query_then_fetch_result_order_of_all_columns_matches() throws Exception {
        int numItems = 40;
        Object[][] args = new Object[numItems][];
        for (int i = 0; i < numItems; i++) {
            args[i] = new Object[] { Integer.toString(i), i };
        }
        execute("create table tbl (x text, y int) clustered into 2 shards");
        execute("insert into tbl (x, y) values (?, ?)", args);
        execute("refresh table tbl");
        Object[][] rows = execute("select x, y from tbl order by y desc").rows();
        assertThat(rows[0]).containsExactly(Integer.toString(numItems - 1), numItems - 1);
        assertThat(rows[1]).containsExactly(Integer.toString(numItems - 2), numItems - 2);
    }


    @Test
    @UseJdbc(0)
    @UseRandomizedSchema(random = false)
    public void test_bit_string_can_be_inserted_and_queried() throws Exception {
        execute("create table tbl (id int primary key, xs bit(4))");
        execute("insert into tbl (id, xs) values (1, B'0000'), (2, B'0001'), (3, B'0011'), (4, B'0111'), (5, B'1111'), (6, B'1001')");
        assertThat(response).hasRowCount(6L);
        execute("refresh table tbl");

        execute("SELECT _doc['xs'], xs, _raw, xs::bit(3) FROM tbl WHERE xs = B'1001'");
        assertThat(response).hasRows(
            "B'1001'| B'1001'| {\"id\":6,\"xs\":\"CQ==\"}| B'100'"
        );
        // use LIMIT 1 to hit a different execution path that should load `xs` differently
        execute("SELECT _doc['xs'], xs, _raw, xs::bit(3) FROM tbl WHERE xs = B'1001' LIMIT 1");
        assertThat(response).hasRows(
            "B'1001'| B'1001'| {\"id\":6,\"xs\":\"CQ==\"}| B'100'"
        );

        // primary key lookup uses different execution path to decode the value
        execute("select xs from tbl where id = 6");
        assertThat(response).hasRows(
            "B'1001'"
        );

        var properties = new Properties();
        properties.put("user", "crate");
        properties.put("password", "");
        ArrayList<String> results = new ArrayList<>();
        try (var conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            Statement stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery("select xs from doc.tbl order by xs");
            while (result.next()) {
                String string = result.getString(1);
                results.add(string);
            }
        }
        assertThat(results).containsExactly(
            "0000",
            "0001",
            "0011",
            "0111",
            "1001",
            "1111"
        );
    }

    /**
     * Adds innerTypes to object type definitions
     **/
    private static DataType<?> extendedType(DataType<?> type, Object value) {
        if (type.id() == ObjectType.ID) {
            var entryIt = ((Map<?, ?>) value).entrySet().iterator();
            Builder builder = ObjectType.builder();
            while (entryIt.hasNext()) {
                var entry = entryIt.next();
                String innerName = (String) entry.getKey();
                Object innerValue = entry.getValue();
                DataType<?> innerType = DataTypes.guessType(innerValue);
                if (innerType.id() == ObjectType.ID && innerValue instanceof Map<?, ?> m && m.containsKey("coordinates")) {
                    innerType = DataTypes.GEO_SHAPE;
                }
                builder.setInnerType(innerName, extendedType(innerType, innerValue));
            }
            return builder.build();
        } else {
            return type;
        }
    }

    @Test
    @UseJdbc(0)
    public void test_types_with_storage_can_be_inserted_and_queried() {
        for (var type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (type.equals(DataTypes.GEO_POINT)) {
                // source and doc-value values don't match exactly
                continue;
            }
            Supplier<?> dataGenerator = DataTypeTesting.getDataGenerator(type);
            Object val1 = dataGenerator.get();
            var extendedType = extendedType(type, val1);
            String typeDefinition = SqlFormatter.formatSql(extendedType.toColumnType(ColumnPolicy.STRICT, null));
            execute("create table tbl (id int primary key, x " + typeDefinition + ")");
            execute("insert into tbl (id, x) values (?, ?)", new Object[] { 1, val1 });
            execute("refresh table tbl");

            var resp1 = execute("select _doc['x'], x, _raw FROM tbl where x = ?", new Object[] { val1 });
            Object x = resp1.rows()[0][1];
            boolean hasPrecisionChange = false;
            if (val1 instanceof Map<?, ?> map) {
                Object x1 = map.get("x");
                hasPrecisionChange = x1 instanceof Map<?, ?> innerMap && innerMap.containsKey("coordinates");
            }
            if (!hasPrecisionChange) {
                assertThat(((DataType) type).compare(x, val1))
                    .as("inserted value must match selected value for type " + type + ": val=" + val1 + " response=" + x)
                    .isEqualTo(0);
            }

            var resp2 = execute("select _doc['x'], x, _raw FROM tbl where id = ?", new Object[] { 1 });
            assertThat(resp2.rows()[0])
                .as("primary key lookup output must match regular select output for type " + type)
                .contains(resp1.rows()[0]);

            if (SemanticSortValidator.SUPPORTED_TYPES.contains(type.id())) {
                // should use doc-values/query-without-fetch execution path due to order + limit
                var resp3 = execute("select _doc['x'], x, _raw FROM tbl order by x limit 1");
                assertThat(resp3.rows()[0])
                    .as("output of query with order and limit must match output of query without" + type)
                    .contains(resp1.rows()[0]);
            }

            execute("drop table tbl");
        }
    }

    @Test
    public void test_can_insert_and_select_bit_string_arrays() throws Exception {
        execute("create table tbl (xs array(bit(3)))");
        execute("insert into tbl (xs) values ([B'010', B'110'])");
        execute("refresh table tbl");
        execute("select xs from tbl");
        assertThat(response).hasRows(
            "[B'010', B'110']"
        );
    }

    @Test
    @UseJdbc(0)
    @UseRandomizedSchema(random = false)
    public void test_character_can_be_inserted_and_queried() throws Exception {
        execute("create table tbl (c character(4))");
        execute("insert into tbl (c) values ('four'), ('two')");
        assertThat(response).hasRowCount(2L);
        execute("refresh table tbl");

        execute("SELECT _doc['c'], c, _raw, c::char(1) FROM tbl WHERE c = 'two'");
        assertThat(response).hasRows(
                "two | two | {\"c\":\"two \"}| t"
        );
        // use LIMIT 1 to hit a different execution path that should load `c` differently
        execute("SELECT _doc['c'], c, _raw, c::char(1) FROM tbl WHERE c = 'four' LIMIT 1");
        assertThat(response).hasRows(
            "four| four| {\"c\":\"four\"}| f"
        );

        var properties = new Properties();
        properties.put("user", "crate");
        properties.put("password", "");
        ArrayList<String> results = new ArrayList<>();
        try (var conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            Statement stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery("select c from doc.tbl order by c");
            while (result.next()) {
                String string = result.getString(1);
                results.add(string);
            }
        }
        assertThat(results).containsExactly(
            "four",
            "two "
        );
    }

    @Test
    public void test_can_insert_and_select_character_arrays() throws Exception {
        execute("create table tbl (c array(char(4)))");
        execute("insert into tbl (c) values (['four', 'two'])");
        execute("refresh table tbl");
        execute("select c from tbl");
        assertThat(response).hasRows(
            "[four, two ]"
        );
    }

    @Test
    public void test_can_select_and_order_by_doc_expression() throws Exception {
        execute("create table tbl (x int)");
        execute("insert into tbl (x) values (1), (2), (3)");
        execute("refresh table tbl");
        execute("select _doc['x'] from tbl order by 1");
        assertThat(response).hasRows(
            "1",
            "2",
            "3"
        );

    }

    /**
     * Test a regression resulting in unusable object type column/table
     */
    @Test
    public void test_can_use_object_with_inner_column_containing_spaces_and_quotes() {
        execute("CREATE TABLE t1 (o object as (\"my first \"\" field\" text))");
        execute("INSERT INTO t1 (o) VALUES ({\"my first \"\" field\" = 'foo'})");
        refresh();
        execute("SELECT o FROM t1");
        assertThat(response).hasRows("{my first \" field=foo}");
        execute("SELECT o['my first \" field'] FROM t1");
        assertThat(response).hasRows("foo");
    }

    /**
     * https://github.com/crate/crate/issues/12081
     */
    @Test
    public void test_subcolumn_can_contain_brackets_in_inserts() {
        execute("create table doc.obj (obj OBJECT);");
        execute("insert into doc.obj (obj) VALUES ('{\"(\": 1.0}');");
        execute("insert into doc.obj (obj) VALUES ('{\"(\": 1.0}');");
        refresh();
        execute("SELECT count(*) FROM doc.obj");
        assertThat(response).hasRows("2");
        execute("SELECT obj FROM doc.obj limit 1");
        assertThat(response).hasRows("{(=1.0}");
    }


    @Test
    public void test_double_quotes_in_column_names_and_insert_values() {
        execute("create table doc.test (\"\"\"\" text);");
        execute("insert into doc.test VALUES ('\"\"')");
        refresh();
        execute("SELECT \"\"\"\" FROM doc.test");
        assertThat(response).hasRows("\"\"");
    }

    @Test
    public void test_select_with_order_by_can_have_limit_zero() {
        execute("select 1 order by 1 limit 0");
        assertThat(response).hasRowCount(0L);

        execute("select 1 order by 1 offset 10 limit 0");
        assertThat(response).hasRowCount(0L);
    }

    /**
     * https://github.com/crate/crate/issues/12756
     */
    @Test
    public void test_is_null_on_object_with_dynamically_added_column_does_not_raise_npe() {
        execute("create table tbl (xo object)");

        // This triggers creation/caching of the ShardCollectorProvider
        execute("select * from tbl where xo is null");

        execute("insert into tbl (xo) values ({foo='bar'})");
        execute("refresh table tbl");

        // This tries to utilize `foo` within the query,
        // this used to throw a NPE because the `DocTableInfo` instance used in the
        // ShardCollectorProvider got stale and didn't have the new column
        execute("select * from tbl where xo is null");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void test_generated_non_deterministic_value_is_consistent_on_primary_and_replica() throws Exception {
        execute("""
            create table tbl (x int, created generated always as round((random() + 1) * 100))
            clustered into 1 shards
            with (number_of_replicas = 1)
            """
        );
        ensureGreen();
        execute("insert into tbl (x) values (1)");
        execute("refresh table tbl");
        execute("select created from tbl");
        long created = (long) response.rows()[0][0];

        // some iterations to ensure it hits both primary and replica
        for (int i = 0; i < 30; i++) {
            assertThat(execute("select created from tbl").rows()[0][0]).isEqualTo(created);
        }
    }

    @Test
    @UseJdbc(0) // For consistent output format
    public void test_can_sort_on_interval() {
        execute(
            """
            select x from unnest([
                INTERVAL '6 years 3 mons' YEAR TO MONTH,
                INTERVAL '4 days 3 hours' DAY TO HOUR,
                INTERVAL '6 years 4 mons' YEAR TO MONTH,
                INTERVAL '4 days 4 hours' DAY TO HOUR
            ]) t (x) order by 1 desc
            """
        );
        assertThat(response).hasRows(
            "P6Y4M",
            "P6Y3M",
            "P4DT4H",
            "P4DT3H"
        );
        execute("select current_timestamp - process['probe_timestamp'] from sys.nodes order by 1");
        List<Period> sorted = Arrays.stream(response.rows())
            .map(row -> (Period) row[0])
            .sorted(DataTypes.INTERVAL)
            .toList();
        List<Period> resultOrder = Arrays.stream(response.rows())
            .map(row -> (Period) row[0])
            .toList();
        assertThat(sorted).containsExactlyElementsOf(resultOrder);
    }
}
