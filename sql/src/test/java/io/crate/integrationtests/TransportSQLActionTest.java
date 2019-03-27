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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SQLActionException;
import io.crate.exceptions.SQLExceptions;
import io.crate.testing.SQLBulkResponse;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedSchema;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class TransportSQLActionTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private <T> List<T> getCol(Object[][] result, int idx) {
        ArrayList<T> res = new ArrayList<>(result.length);
        for (Object[] row : result) {
            res.add((T) row[idx]);
        }
        return res;
    }

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
        expectedException.expect(IndexNotFoundException.class);

        execute("create table t (name string)");
        ensureYellow();

        PlanForNode plan = plan("select * from t");
        execute("drop table t");
        try {
            execute(plan).getResult();
        } catch (Throwable t) {
            throw SQLExceptions.unwrap(t);
        }
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
        prepareCreate(getFqn("test")).execute().actionGet();
        ensureYellow();
        execute("select * from test");
        assertArrayEquals(new String[]{}, response.cols());
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGroupByOnAnalyzedColumn() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot GROUP BY 'col1': grouping on analyzed/fulltext columns is not possible");

        execute("create table test1 (col1 string index using fulltext)");
        ensureYellow();
        execute("insert into test1 (col1) values ('abc def, ghi. jkl')");
        refresh();
        execute("select count(col1) from test1 group by col1");
    }

    @Test
    public void testSelectStarWithOther() throws Exception {
        execute("create table test (id string primary key, first_name string, last_name string)");
        execute("insert into test (id, first_name, last_name) values ('id1', 'Youri', 'Zoon')");
        execute("refresh table test");

        execute("select \"_version\", *, \"_id\" from test");
        assertArrayEquals(new String[]{"_version", "id", "first_name", "last_name", "_id"}, response.cols());
        assertEquals(1, response.rowCount());
        assertArrayEquals(new Object[]{1L, "id1", "Youri", "Zoon", "id1"}, response.rows()[0]);
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
        prepareCreate(getFqn("test"))
            .addMapping("default", "sunshine", "type=boolean")
            .execute().actionGet();
        ensureYellow();

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
        assertThat((String) response.rows()[0][0], is("id2"));
    }


    @Test
    public void testSqlRequestWithFilter() throws Exception {
        execute("create table test (id string primary key)");
        execute("insert into test (id) values ('id1'), ('id2')");
        execute("select _id from test where id = 'id1'");
        assertEquals(1, response.rowCount());
        assertEquals("id1", response.rows()[0][0]);
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
        assertEquals(1, response.rowCount());
        assertEquals("id2", response.rows()[0][0]);
    }


    @Test
    public void testSqlRequestWithOneOrFilter() throws Exception {
        execute("create table test (id string) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into test (id) values ('id1'), ('id2'), ('id3')");
        refresh();
        execute("select id from test where id='id1' or id='id3'");
        assertEquals(2, response.rowCount());
        assertThat(this.<String>getCol(response.rows(), 0), containsInAnyOrder("id1", "id3"));
    }

    @Test
    public void testSqlRequestWithOneMultipleOrFilter() throws Exception {
        execute("create table test (id string) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into test (id) values ('id1'), ('id2'), ('id3'), ('id4')");
        refresh();
        execute("select id from test where id='id1' or id='id2' or id='id4'");
        assertEquals(3, response.rowCount());
        List<String> col1 = this.getCol(response.rows(), 0);
        assertThat(col1, containsInAnyOrder("id1", "id2", "id4"));
    }

    @Test
    public void testSqlRequestWithDateFilter() {
        execute("create table test (date timestamp with time zone)");
        execute("insert into test (date) values ('2013-10-01'), ('2013-10-02')");
        execute("refresh table test");
        execute("select date from test where date = '2013-10-01'");
        assertEquals(1, response.rowCount());
        assertEquals(1380585600000L, response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithDateGtFilter() {
        execute("create table test (date timestamp with time zone)");
        execute("insert into test (date) values ('2013-10-01'), ('2013-10-02')");
        execute("refresh table test");
        execute(
            "select date from test where date > '2013-10-01'");
        assertEquals(1, response.rowCount());
        assertEquals(1380672000000L, response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithNumericGtFilter() throws Exception {
        execute("create table test (i long)");
        execute("insert into test (i) values (10), (20)");
        execute("refresh table test");
        execute(
            "select i from test where i > 10");
        assertEquals(1, response.rowCount());
        assertEquals(20L, response.rows()[0][0]);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testArraySupport() throws Exception {
        execute("create table t1 (id int primary key, strings array(string), integers array(integer)) with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into t1 (id, strings, integers) values (?, ?, ?)",
            new Object[]{
                1,
                new String[]{"foo", "bar"},
                new Integer[]{1, 2, 3}
            }
        );
        refresh();

        execute("select id, strings, integers from t1");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat(((String) ((Object[]) response.rows()[0][1])[0]), is("foo"));
        assertThat(((String) ((Object[]) response.rows()[0][1])[1]), is("bar"));
        assertThat(((Integer) ((Object[]) response.rows()[0][2])[0]), is(1));
        assertThat(((Integer) ((Object[]) response.rows()[0][2])[1]), is(2));
        assertThat(((Integer) ((Object[]) response.rows()[0][2])[2]), is(3));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testArrayInsideObject() throws Exception {
        execute("create table t1 (id int primary key, details object as (names array(string))) with (number_of_replicas=0)");
        ensureYellow();

        Map<String, Object> details = new HashMap<>();
        details.put("names", new Object[]{"Arthur", "Trillian"});
        execute("insert into t1 (id, details) values (?, ?)", new Object[]{1, details});
        refresh();

        execute("select details['names'] from t1");
        assertThat(response.rowCount(), is(1L));
        assertThat(((String) ((Object[]) response.rows()[0][0])[0]), is("Arthur"));
        assertThat(((String) ((Object[]) response.rows()[0][0])[1]), is("Trillian"));
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
        assertThat(
            printedTable(response.rows()),
            is("[[Arthur, Trillian], [Ford, Slarti, Ford]]| true\n")
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
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testArraySupportWithNullValues() throws Exception {
        execute("create table t1 (id int primary key, strings array(string)) with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into t1 (id, strings) values (?, ?)",
            new Object[]{
                1,
                new String[]{"foo", null, "bar"},
            }
        );
        refresh();

        execute("select id, strings from t1");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat(((String) ((Object[]) response.rows()[0][1])[0]), is("foo"));
        assertThat(((Object[]) response.rows()[0][1])[1], nullValue());
        assertThat(((String) ((Object[]) response.rows()[0][1])[2]), is("bar"));
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
        ensureYellow();

        Map<String, Object> obj1 = new MapBuilder<String, Object>().put("name", "foo").put("age", 1).map();
        Map<String, Object> obj2 = new MapBuilder<String, Object>().put("name", "bar").put("age", 2).map();

        Object[] args = new Object[]{1, new Object[]{obj1, obj2}};
        execute("insert into t1 (id, objects) values (?, ?)", args);
        refresh();

        execute("select objects from t1");
        assertThat(response.rowCount(), is(1L));

        Object[] objResults = ((Object[]) response.rows()[0][0]);
        Map<String, Object> obj1Result = ((Map) objResults[0]);
        assertThat((String) obj1Result.get("name"), is("foo"));
        assertThat((Integer) obj1Result.get("age"), is(1));

        Map<String, Object> obj2Result = ((Map) objResults[1]);
        assertThat((String) obj2Result.get("name"), is("bar"));
        assertThat((Integer) obj2Result.get("age"), is(2));

        execute("select objects['name'] from t1");
        assertThat(response.rowCount(), is(1L));

        String[] names = Arrays.copyOf(((Object[]) response.rows()[0][0]), 2, String[].class);
        assertThat(names[0], is("foo"));
        assertThat(names[1], is("bar"));

        execute("select objects['name'] from t1 where ? = ANY (objects['name'])", new Object[]{"foo"});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testGetResponseWithObjectColumn() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("default")
            .startObject("_meta").field("primary_keys", "id").endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("data")
            .field("type", "object")
            .field("dynamic", false)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        prepareCreate(getFqn("test"))
            .addMapping("default", mapping)
            .execute().actionGet();
        ensureYellow();

        Map<String, Object> data = new HashMap<>();
        data.put("foo", "bar");
        execute("insert into test (id, data) values (?, ?)", new Object[]{"1", data});
        refresh();

        execute("select data from test where id = ?", new Object[]{"1"});
        assertEquals(data, response.rows()[0][0]);
    }

    @Test
    public void testSelectToGetRequestByPlanner() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('124', 'bar1')");
        assertEquals(1, response.rowCount());
        refresh();
        waitNoPendingTasksOnAll(); // wait for mapping update as foo is being added

        execute("select pk_col, message from test where pk_col='124'");
        assertEquals(1, response.rowCount());
        assertEquals("124", response.rows()[0][0]);
    }

    @Test
    public void testSelectToRoutedRequestByPlanner() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");
        refresh();

        execute("SELECT * FROM test WHERE pk_col='1' OR pk_col='2'");
        assertEquals(2, response.rowCount());

        execute("SELECT * FROM test WHERE pk_col=? OR pk_col=?", new Object[]{"1", "2"});
        assertEquals(2, response.rowCount());

        awaitBusy(() -> {
            execute("SELECT * FROM test WHERE (pk_col=? OR pk_col=?) OR pk_col=?", new Object[]{"1", "2", "3"});
            return response.rowCount() == 3
                   && Joiner.on(',').join(Arrays.asList(response.cols())).equals("message,pk_col");
        }, 10, TimeUnit.SECONDS);
    }

    @Test
    public void testSelectToRoutedRequestByPlannerMissingDocuments() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");
        refresh();

        execute("SELECT pk_col, message FROM test WHERE pk_col='4' OR pk_col='3'");
        assertEquals(1, response.rowCount());
        assertThat(Arrays.asList(response.rows()[0]), hasItems(new Object[]{"3", "baz"}));

        execute("SELECT pk_col, message FROM test WHERE pk_col='4' OR pk_col='99'");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testSelectToRoutedRequestByPlannerWhereIn() throws Exception {
        this.setup.createTestTableWithPrimaryKey();

        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");
        refresh();

        execute("SELECT * FROM test WHERE pk_col IN (?,?,?)", new Object[]{"1", "2", "3"});
        assertEquals(3, response.rowCount());
    }

    @Test
    public void testUpdateToRoutedRequestByPlannerWhereOr() throws Exception {
        this.setup.createTestTableWithPrimaryKey();
        execute("insert into test (pk_col, message) values ('1', 'foo'), ('2', 'bar'), ('3', 'baz')");
        refresh();
        execute("update test set message='new' WHERE pk_col='1' or pk_col='2' or pk_col='4'");
        assertThat(response.rowCount(), is(2L));
        refresh();
        execute("SELECT distinct message FROM test");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testSelectWithWhereLike() throws Exception {
        this.setup.groupBySetup();

        execute("select name from characters where name like '%ltz'");
        assertEquals(2L, response.rowCount());

        execute("select count(*) from characters where name like 'Jeltz'");
        assertEquals(1L, response.rows()[0][0]);

        execute("select count(*) from characters where race like '%o%'");
        assertEquals(3L, response.rows()[0][0]);

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
                "o object(ignored), " +
                "index quote_fulltext using fulltext(quote) with (analyzer='snowball')" +
                ") clustered by (id) into 3 shards with (number_of_replicas = 0)");
        ensureYellow();
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

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Can only use MATCH on columns of type STRING or GEO_SHAPE, not on 'undefined'");

        execute("select * from quotes where match(o['something'], 'bla')");
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
        assertThat((long) response.rows()[0][0], lessThanOrEqualTo(3L));

        execute("refresh table test");
        assertThat(response.rowCount(), is(1L));

        execute("select count(*) from test");
        assertThat((Long) response.rows()[0][0], is(3L));
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
        assertNotNull(response.rows()[0][0]);
        assertThat(((String) response.rows()[0][0]).length(), greaterThan(0));
    }

    @Test
    public void testInsertSelectWithAutoGeneratedId() throws Exception {
        execute("create table quotes (id integer, quote string)" +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote) values(?, ?)",
            new Object[]{1, "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select \"_id\", id, quote from quotes where id=1");
        assertEquals(1L, response.rowCount());

        // Validate generated _id, must be: <generatedRandom>
        assertNotNull(response.rows()[0][0]);
        assertThat(((String) response.rows()[0][0]).length(), greaterThan(0));
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
        ensureYellow();
        execute("insert into quotes (id, author, quote) values(?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select \"_id\", id from quotes where id=1 and author='Ford'");
        assertEquals(1L, response.rowCount());
        assertThat((String) response.rows()[0][0], is("AgExBEZvcmQ="));
        assertThat((Integer) response.rows()[0][1], is(1));
    }

    @Test
    public void testInsertSelectWithMultiplePrimaryKeyAndClusteredBy() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) clustered by(author) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, author, quote) values(?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day."});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select \"_id\", id from quotes where id=1 and author='Ford'");
        assertEquals(1L, response.rowCount());
        assertThat((String) response.rows()[0][0], is("AgRGb3JkATE="));
        assertThat((Integer) response.rows()[0][1], is(1));
    }

    @Test
    public void testInsertSelectWithMultiplePrimaryOnePkSame() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) clustered by(author) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, author, quote) values (?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Ford", "I'd far rather be happy than right any day.",
                1, "Douglas", "Don't panic"}
        );
        assertEquals(2L, response.rowCount());
        refresh();

        execute("select \"_id\", id from quotes where id=1 order by author");
        assertEquals(2L, response.rowCount());
        assertThat((String) response.rows()[0][0], is("AgdEb3VnbGFzATE="));
        assertThat((Integer) response.rows()[0][1], is(1));
        assertThat((String) response.rows()[1][0], is("AgRGb3JkATE="));
        assertThat((Integer) response.rows()[1][1], is(1));
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
        SQLBulkResponse bulkResp = execute("insert into test (id, name) values (?, ?), (?, ?)",
            new Object[][]{
                {1, "Earth", 2, "Saturn"},    // bulk row 1
                {3, "Moon", 4, "Mars"}        // bulk row 2
            });
        assertThat(bulkResp.results().length, is(2));
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(2L));
        }
        refresh();

        bulkResp = execute("insert into test (id, name) values (?, ?), (?, ?)",
            new Object[][]{
                {1, "Earth", 2, "Saturn"},    // bulk row 1
                {3, "Moon", 4, "Mars"}        // bulk row 2
            });
        assertThat(bulkResp.results().length, is(2));
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(-2L));
        }

        execute("select name from test order by id asc");
        assertEquals("Earth\nSaturn\nMoon\nMars\n", printedTable(response.rows()));

        // test bulk update-by-id
        bulkResp = execute("update test set name = concat(name, '-updated') where id = ?", new Object[][]{
            new Object[]{2},
            new Object[]{3},
            new Object[]{4},
        });
        assertThat(bulkResp.results().length, is(3));
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(1L));
        }
        refresh();

        execute("select count(*) from test where name like '%-updated'");
        assertThat((Long) response.rows()[0][0], is(3L));

        // test bulk of delete-by-id
        bulkResp = execute("delete from test where id = ?", new Object[][]{
            new Object[]{1},
            new Object[]{3}
        });
        assertThat(bulkResp.results().length, is(2));
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(1L));
        }
        refresh();

        execute("select count(*) from test");
        assertThat((Long) response.rows()[0][0], is(2L));

        // test bulk of delete-by-query
        bulkResp = execute("delete from test where name = ?", new Object[][]{
            new Object[]{"Saturn-updated"},
            new Object[]{"Mars-updated"}
        });
        assertThat(bulkResp.results().length, is(2));
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(1L));
        }
        refresh();

        execute("select count(*) from test");
        assertThat((Long) response.rows()[0][0], is(0L));

    }

    @Test
    public void testSelectFormatFunction() throws Exception {
        this.setup.setUpLocations();
        ensureYellow();
        refresh();

        execute("select format('%s is a %s', name, kind) as sentence from locations order by name");
        assertThat(response.rowCount(), is(13L));
        assertArrayEquals(response.cols(), new String[]{"sentence"});
        assertThat(response.rows()[0].length, is(1));
        assertThat((String) response.rows()[0][0], is(" is a Planet"));
        assertThat((String) response.rows()[1][0], is("Aldebaran is a Star System"));
        assertThat((String) response.rows()[2][0], is("Algol is a Star System"));
        // ...

    }

    @Test
    public void testAnyArray() throws Exception {
        this.setup.setUpArrayTables();

        execute("select count(*) from any_table where 'Berlin' = ANY (names)");
        assertThat((Long) response.rows()[0][0], is(2L));

        execute("select id, names from any_table where 'Berlin' = ANY (names) order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(3));

        execute("select id from any_table where 'Berlin' != ANY (names) order by id");
        assertThat(response.rowCount(), is(3L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(2));
        assertThat((Integer) response.rows()[2][0], is(3));

        execute("select count(id) from any_table where 0.0 < ANY (temps)");
        assertThat((Long) response.rows()[0][0], is(2L));

        execute("select id, names from any_table where 0.0 < ANY (temps) order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(2));
        assertThat((Integer) response.rows()[1][0], is(3));

        execute("select count(*) from any_table where 0.0 > ANY (temps)");
        assertThat((Long) response.rows()[0][0], is(2L));

        execute("select id, names from any_table where 0.0 > ANY (temps) order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(2));
        assertThat((Integer) response.rows()[1][0], is(3));

        execute("select id, names from any_table where 'Ber%' LIKE ANY (names) order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(3));

    }

    @Test
    public void testNotAnyArray() throws Exception {
        this.setup.setUpArrayTables();

        execute("select id from any_table where NOT 'Hangelsberg' = ANY (names) order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(2));

        execute("select id from any_table where 'Hangelsberg' != ANY (names) order by id");
        assertThat(response.rowCount(), is(3L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(2));
        assertThat((Integer) response.rows()[2][0], is(3));
    }

    @Test
    public void testAnyLike() throws Exception {
        this.setup.setUpArrayTables();

        execute("select id from any_table where 'kuh%' LIKE ANY (tags) order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(3));
        assertThat((Integer) response.rows()[1][0], is(4));

        execute("select id from any_table where 'kuh%' NOT LIKE ANY (tags) order by id");
        assertThat(response.rowCount(), is(3L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(2));
        assertThat((Integer) response.rows()[2][0], is(3));

    }

    @Test
    public void testInsertAndSelectIpType() throws Exception {
        execute("create table ip_table (fqdn string, addr ip) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into ip_table (fqdn, addr) values ('localhost', '127.0.0.1'), ('crate.io', '23.235.33.143')");
        execute("refresh table ip_table");

        execute("select addr from ip_table where addr = '23.235.33.143'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("23.235.33.143"));

        execute("select addr from ip_table where addr > '127.0.0.1'");
        assertThat(response.rowCount(), is(0L));
        execute("select addr from ip_table where addr > 2130706433"); // 2130706433 == 127.0.0.1
        assertThat(response.rowCount(), is(0L));

        execute("select addr from ip_table where addr < '127.0.0.1'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("23.235.33.143"));
        execute("select addr from ip_table where addr < 2130706433"); // 2130706433 == 127.0.0.1
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("23.235.33.143"));

        execute("select addr from ip_table where addr <= '127.0.0.1'");
        assertThat(response.rowCount(), is(2L));

        execute("select addr from ip_table where addr >= '23.235.33.143'");
        assertThat(response.rowCount(), is(2L));

        execute("select addr from ip_table where addr IS NULL");
        assertThat(response.rowCount(), is(0L));

        execute("select addr from ip_table where addr IS NOT NULL");
        assertThat(response.rowCount(), is(2L));

    }

    @Test
    public void testGroupByOnIpType() throws Exception {
        execute("create table t (i ip) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i) values ('192.168.1.2'), ('192.168.1.2'), ('192.168.1.3')");
        execute("refresh table t");
        execute("select i, count(*) from t group by 1 order by count(*) desc");

        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("192.168.1.2"));
        assertThat((Long) response.rows()[0][1], is(2L));
        assertThat((String) response.rows()[1][0], is("192.168.1.3"));
        assertThat((Long) response.rows()[1][1], is(1L));
    }

    @Test
    public void testInsertAndSelectGeoType() throws Exception {
        execute("create table geo_point_table (id int primary key, p geo_point) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into geo_point_table (id, p) values (?, ?)", new Object[]{1, new Double[]{47.22, 12.09}});
        execute("insert into geo_point_table (id, p) values (?, ?)", new Object[]{2, new Double[]{57.22, 7.12}});
        refresh();

        execute("select p from geo_point_table order by id desc");

        assertThat(response.rowCount(), is(2L));
        assertThat(((Object[]) response.rows()[0][0]), arrayContaining(new Object[]{57.22, 7.12}));
        assertThat(((Object[]) response.rows()[1][0]), arrayContaining(new Object[]{47.22, 12.09}));
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
        assertThat(response.rowCount(), is(2L));

        Double result1 = (Double) response.rows()[0][0];
        Double result2 = (Double) response.rows()[1][0];

        assertThat(result1, is(0.0d));
        assertThat(result2, is(152354.32308347954));

        String stmtOrderBy = "SELECT id " +
                             "FROM t " +
                             "ORDER BY distance(p, 'POINT(30.0 30.0)')";
        execute(stmtOrderBy);
        assertThat(response.rowCount(), is(2L));
        String expectedOrderBy =
            "2\n" +
            "1\n";
        assertEquals(expectedOrderBy, printedTable(response.rows()));

        // aggregation (max())
        String stmtAggregate = "SELECT i, max(distance(p, 'POINT(30.0 30.0)')) " +
                               "FROM t " +
                               "GROUP BY i";
        execute(stmtAggregate);
        assertThat(response.rowCount(), is(1L));
        String expectedAggregate = "1| 2296582.8899438097\n";
        assertEquals(expectedAggregate, printedTable(response.rows()));

        Double[] row;
        // queries
        execute("select p from t where distance(p, 'POINT (11 21)') > 0.0");
        assertThat(response.rowCount(), is(1L));
        row = Arrays.copyOf((Object[]) response.rows()[0][0], 2, Double[].class);
        assertThat(row[0], is(10.0d));
        assertThat(row[1], is(20.0d));

        execute("select p from t where distance(p, 'POINT (11 21)') < 10.0");
        assertThat(response.rowCount(), is(1L));
        row = Arrays.copyOf((Object[]) response.rows()[0][0], 2, Double[].class);
        assertThat(row[0], is(11.0d));
        assertThat(row[1], is(21.0d));

        execute("select p from t where distance(p, 'POINT (11 21)') < 10.0 or distance(p, 'POINT (11 21)') > 10.0");
        assertThat(response.rowCount(), is(2L));

        execute("select p from t where distance(p, 'POINT (10 20)') >= 0.0 and distance(p, 'POINT (10 20)') <= 0.1");
        assertThat(response.rowCount(), is(1L));
        row = Arrays.copyOf((Object[]) response.rows()[0][0], 2, Double[].class);
        assertThat(row[0], is(10.0d));
        assertThat(row[1], is(20.0d));

        execute("select p from t where distance(p, 'POINT (10 20)') = 0.0");
        assertThat(response.rowCount(), is(1L));
        row = Arrays.copyOf((Object[]) response.rows()[0][0], 2, Double[].class);
        assertThat(row[0], is(10.0d));
        assertThat(row[1], is(20.0d));

        execute("select p from t where distance(p, 'POINT (10 20)') = 152354.3209044634");
        assertThat(printedTable(response.rows()),
            is("[11.0, 21.0]\n"));
    }

    @Test
    public void testWithinQuery() throws Exception {
        execute("create table t (id int primary key, p geo_point) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (id, p) values (1, 'POINT (10 10)')");
        refresh();

        execute("select within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))') from t");
        assertThat((Boolean) response.rows()[0][0], is(true));

        execute("select * from t where within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))')");
        assertThat(response.rowCount(), is(1L));
        execute("select * from t where within(p, ?)", $(ImmutableMap.of(
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
        assertThat(response.rowCount(), is(1L));

        execute("select * from t where within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))') = false");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testTwoSubStrOnSameColumn() throws Exception {
        execute("select substr(name, 0, 4), substr(name, 4, 8) from sys.cluster");
        assertThat(printedTable(response.rows()), is("SUIT| TE-CHILD\n"));
    }


    @Test
    public void testSelectArithMetricOperatorInOrderBy() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 3 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (2, 5, 90.5), (193384, 31234594433, 99.0), (10, 21, 99.0), (-1, 4, 99.0)");
        refresh();

        execute("select i, i%3 from t order by i%3, l");
        assertThat(response.rowCount(), is(5L));
        assertThat(printedTable(response.rows()), is(
            "-1| -1\n" +
            "1| 1\n" +
            "10| 1\n" +
            "193384| 1\n" +
            "2| 2\n"));
    }

    @Test
    public void testSelectFailingSearchScript() throws Exception {
        expectedException.expectMessage("log(x, b): given arguments would result in: 'NaN'");

        execute("create table t (i integer, l long, d double) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5)");
        refresh();

        execute("select log(d, l) from t where log(d, -1) >= 0");
    }

    @Test
    public void testSelectGroupByFailingSearchScript() throws Exception {
        expectedException.expectMessage("log(x, b): given arguments would result in: 'NaN'");

        execute("create table t (i integer, l long, d double) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (0, 4, 100)");
        execute("refresh table t");

        execute("select log(d, l) from t where log(d, -1) >= 0 group by log(d, l)");
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
        this.setup.setUpLocations();
        ensureYellow();
        refresh();

        execute("select name from locations where name = name");
        assertThat(response.rowCount(), is(13L));

        execute("select name from locations where substr(name, 1, 1) = substr(name, 1, 1)");
        assertThat(response.rowCount(), is(13L));
    }

    @Test
    public void testNewColumn() throws Exception {
        execute("create table t (name string) with (number_of_replicas=0, column_policy = 'dynamic')");
        ensureYellow();
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
        Map<String, Object> firstRaw = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            (String) response.rows()[0][0]).map();

        assertThat(response.rows()[0][1], is("2"));
        assertThat(firstRaw.get("id"), is("2"));
        assertThat(firstRaw.get("name"), is("Outer Eastern Rim"));
        assertThat(firstRaw.get("date"), is(308534400000L));
        assertThat(firstRaw.get("kind"), is("Galaxy"));

        Map<String, Object> secondRaw = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            (String) response.rows()[1][0]).map();
        assertThat(response.rows()[1][1], is("3"));
        assertThat(secondRaw.get("id"), is("3"));
        assertThat(secondRaw.get("name"), is("Galactic Sector QQ7 Active J Gamma"));
        assertThat(secondRaw.get("date"), is(1367366400000L));
        assertThat(secondRaw.get("kind"), is("Galaxy"));
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testUnknownTableJobGetsRemoved() throws Exception {
        String uniqueId = UUID.randomUUID().toString();
        String stmtStr = "select '" + uniqueId + "' from foobar";
        String stmtStrWhere = "select ''" + uniqueId + "'' from foobar";
        try {
            execute(stmtStr);
        } catch (SQLActionException e) {
            assertThat(e.getMessage(), containsString("Relation 'foobar' unknown"));
            execute("select stmt from sys.jobs where stmt='" + stmtStrWhere + "'");
            assertEquals(response.rowCount(), 0L);
        }
    }

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
        assertThat(response.rowCount(), is(1L));
    }


    @Test
    public void testSelectFrom_Doc() throws Exception {
        execute("create table t (name string) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (name) values ('Marvin')");
        execute("refresh table t");

        execute("select _doc['name'] from t");
        assertThat(((String) response.rows()[0][0]), is("Marvin"));
    }

    @Test
    public void testSelectWithSingleBulkArgRaisesUnsupportedError() {
        expectedException.expectMessage("Bulk operations for statements that return result sets is not supported");
        execute("select * from sys.cluster", new Object[0][]);
    }

    @Test
    public void testSelectWithBulkArgsRaisesUnsupportedError() {
        expectedException.expectMessage("Bulk operations for statements that return result sets is not supported");
        execute("select * from sys.cluster", new Object[][]{new Object[]{1}, new Object[]{2}});
    }

    @Test
    public void testWeirdIdentifiersAndLiterals() throws Exception {
        execute("CREATE TABLE with_quote (\"\"\"\" string) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("INSERT INTO with_quote (\"\"\"\") VALUES ('''')");
        execute("REFRESH TABLE with_quote");
        execute("SELECT * FROM with_quote");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("\"")));
        assertThat((String) response.rows()[0][0], is("'"));
    }

    @Test
    public void testWhereNotNull() throws Exception {
        execute("create table t (b boolean, i int) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (b, i) values (true, 1), (false, 2), (null, null)");
        execute("refresh table t");

        execute("select b, not b, not (b > i::boolean) from t order by b");
        Object[][] rows = response.rows();
        assertThat((Boolean) rows[0][0], is(false));
        assertThat((Boolean) rows[0][1], is(true));
        assertThat((Boolean) rows[0][2], is(true));
        assertThat((Boolean) rows[1][0], is(true));
        assertThat((Boolean) rows[1][1], is(false));
        assertThat((Boolean) rows[1][2], is(true));
        assertThat(rows[2][0], nullValue());
        assertThat(rows[2][1], nullValue());
        assertThat(rows[2][2], nullValue());

        execute("select b, i from t where not b");
        assertThat(response.rowCount(), is(1L));

        execute("select b, i from t where not b > i::boolean");
        assertThat(response.rowCount(), is(2L));

        execute("SELECt b, i FROM t WHERE NOT (i = 1 AND b = TRUE)");
        assertThat(response.rowCount(), is(1L));

        execute("SELECT b, i FROM t WHERE NOT (i IS NULL OR b IS NULL)");
        assertThat(response.rowCount(), is(2L));

        execute("SELECT b FROM t WHERE NOT (coalesce(b, true))");
        assertThat(response.rowCount(), is(1L));

        execute("SELECT b, i FROM t WHERE NOT (coalesce(b, false) = true AND i IS NULL)");
        assertThat(response.rowCount(), is(2L));

    }

    @Test
    public void testScalarEvaluatesInErrorOnDocTable() throws Exception {
        execute("create table t1 (id int) with (number_of_replicas=0)");
        ensureYellow();
        // we need at least 1 row, otherwise the table is empty and no evaluation occurs
        execute("insert into t1 (id) values (1)");
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(" / by zero");
        execute("select 1/0 from t1");
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
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testOrderByWorksOnSymbolWithLateNormalization() throws Exception {
        execute("create table t (x int) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (x) values (5), (10), (3)");
        execute("refresh table t");

        // cast is added to have a to_int(2) after the subquery is inserted
        // this causes a normalization on the map-side which used to remove the order by
        execute("select cast((select 2) as integer) * x from t order by 1");
        assertThat(printedTable(response.rows()),
            is("6\n" +
               "10\n" +
               "20\n"));
    }

    @Test
    public void testOrderOnAnalyzedColumn() {
        execute("create table t (text string index using fulltext) with (number_of_replicas = 0)");
        execute("insert into t (text) values ('Hello World'), ('The End')");
        execute("refresh table t");

        assertThat(
            printedTable(execute("select text from t order by 1 desc").rows()),
            is("The End\n" +
               "Hello World\n")
        );
    }
}
