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

package org.cratedb.integrationtests;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.cratedb.Constants;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.sql.*;
import org.cratedb.sql.types.TimeStampSQLType;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 2)
@Seed("991C1014D4833E6C:1CDD059F87E0920F")
public class TransportSQLActionTest extends SQLTransportIntegrationTest {


    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    protected SQLResponse response;
    private Setup setup = new Setup(this);

    private String copyFilePath = getClass().getResource("/essetup/data/copy").getPath();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * override execute to store response in property for easier access
     */
    @Override
    public SQLResponse execute(String stmt, Object[] args) {
        response = super.execute(stmt, args);
        return response;
    }

    private <T> List<T> getCol(Object[][] result, int idx){
        ArrayList<T> res = new ArrayList<>(result.length);
        for (Object[] row : result) {
            res.add((T) row[idx]);
        }
        return res;
    }

    @Test
    public void testSelectKeepsOrder() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" as b, \"_version\" as a from test");
        assertArrayEquals(new String[]{"b", "a"}, response.cols());
        assertEquals(1, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
    }

    @Test
    public void testSelectCountStar() throws Exception {
        execute("create table test (\"type\" string) replicas 0");
        ensureGreen();
        execute("insert into test (name) values (?)", new Object[]{"Arthur"});
        execute("insert into test (name) values (?)", new Object[]{"Trillian"});
        refresh();
        execute("select count(*) from test");
        assertEquals(1, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
    }

    @Test
    public void testSelectCountStarWithWhereClause() throws Exception {
        execute("create table test (\"type\" string) replicas 0");
        ensureGreen();
        execute("insert into test (name) values (?)", new Object[]{"Arthur"});
        execute("insert into test (name) values (?)", new Object[]{"Trillian"});
        refresh();
        execute("select count(*) from test where name = 'Trillian'");
        assertEquals(1, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
    }

    /*
    @Test
    public void testGroupByOnSysNodes() throws Exception {
        execute("select count(*), name from sys.nodes group by name");
        assertThat(response.rowCount(), is(2L));

        execute("select count(*), hostname from sys.nodes group by hostname");
        assertThat(response.rowCount(), is(1L));
    }
    */

    @Test
    public void testSysCluster() throws Exception {
        execute("select id from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat(((String)response.rows()[0][0]).length(), is(36)); // looks like a uuid
    }

    @Test
    public void testSelectStar() throws Exception {
        execute("create table test (\"firstName\" string, \"lastName\" string)");
        waitForRelocation(ClusterHealthStatus.GREEN);
        execute("select * from test");
        assertArrayEquals(new String[]{"firstName", "lastName"}, response.cols());
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testSelectStarEmptyMapping() throws Exception {
        prepareCreate("test").execute().actionGet();
        ensureGreen();
        execute("select * from test");
        assertArrayEquals(new String[]{}, response.cols());
        assertEquals(0, response.rowCount());
    }

    // TODO: when analyzers are in the tableinfo, re-enable this test
//    @Test(expected = GroupByOnArrayUnsupportedException.class)
//    public void testGroupByOnAnalyzedColumn() throws Exception {
//        execute("create table test1 (col1 string index using fulltext)");
//        ensureGreen();
//
//        execute("select count(*) from test1 group by col1");
//    }


    @Test
    public void testSelectStarWithOther() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string",
                        "lastName", "type=string")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"firstName\":\"Youri\",\"lastName\":\"Zoon\"}")
                .execute().actionGet();
        execute("select \"_version\", *, \"_id\" from test");
        assertArrayEquals(new String[]{"_version", "firstName", "lastName", "_id"},
                response.cols());
        assertEquals(1, response.rowCount());
        assertArrayEquals(new Object[]{1L, "Youri", "Zoon", "id1"}, response.rows()[0]);
    }

    @Test
    public void testSelectWithParams() throws Exception {
        execute("create table test (first_name string, last_name string, age double)");
        client().prepareIndex("test", "default", "id1").setRefresh(true)
            .setSource("{\"first_name\":\"Youri\",\"last_name\":\"Zoon\", \"age\": 38}")
            .execute().actionGet();

        Object[] args = new Object[] {"id1"};
        execute("select first_name, last_name from test where \"_id\" = $1", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);

        args = new Object[] {"Zoon"};
        execute("select first_name, last_name from test where last_name = $1", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);

        args = new Object[] {38, "Zoon"};
        execute("select first_name, last_name from test where age = $1 and last_name = $2", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);

        args = new Object[] {38, "Zoon"};
        execute("select first_name, last_name from test where age = ? and last_name = ?", args);
        assertArrayEquals(new Object[]{"Youri", "Zoon"}, response.rows()[0]);
    }

    @Test
    public void testSelectStarWithOtherAndAlias() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string",
                        "lastName", "type=string")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"firstName\":\"Youri\",\"lastName\":\"Zoon\"}")
                .execute().actionGet();
        execute("select *, \"_version\", \"_version\" as v from test");
        assertArrayEquals(new String[]{"firstName", "lastName", "_version", "v"},
                response.cols());
        assertEquals(1, response.rowCount());
        assertArrayEquals(new Object[]{"Youri", "Zoon", 1L, 1L}, response.rows()[0]);
    }


    @Test
    public void testSelectNestedColumns() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "message", "type=string",
                        "person", "type=object")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"message\":\"I'm addicted to kite\", " +
                        "\"person\": { \"name\": \"Youri\", \"addresses\": [ { \"city\": " +
                        "\"Dirksland\", \"country\": \"NL\" } ] }}")
                .execute().actionGet();

        execute("select message, person['name'], person['addresses']['city'] from test " +
                "where person['name'] = 'Youri'");

        assertArrayEquals(new String[]{"message", "person['name']", "person['addresses']['city']"},
                response.cols());
        assertEquals(1, response.rowCount());
        assertArrayEquals(new Object[]{"I'm addicted to kite", "Youri",
                new ArrayList<String>(){{add("Dirksland");}}},
                response.rows()[0]);
    }

    @Test
    public void testFilterByEmptyString() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "name", "type=string,index=not_analyzed")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"name\":\"\"}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2").setRefresh(true)
                .setSource("{\"name\":\"Ruben Lenten\"}")
                .execute().actionGet();

        execute("select name from test where name = ''");
        assertEquals(1, response.rowCount());
        assertEquals("", response.rows()[0][0]);

        execute("select name from test where name != ''");
        assertEquals(1, response.rowCount());
        assertEquals("Ruben Lenten", response.rows()[0][0]);

    }

    @Test
    public void testFilterByNull() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "name", "type=string,index=not_analyzed")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2").setRefresh(true)
                .setSource("{\"name\":\"Ruben Lenten\"}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id3").setRefresh(true)
                .setSource("{\"name\":\"\"}")
                .execute().actionGet();

        execute("select \"_id\" from test where name is null");
        assertEquals(1, response.rowCount());
        assertEquals("id1", response.rows()[0][0]);

        execute("select \"_id\" from test where name is not null order by \"_uid\"");
        assertEquals(2, response.rowCount());
        assertEquals("id2", response.rows()[0][0]);

        // missing field is null returns no match, since we cannot filter by it
        execute("select \"_id\" from test where invalid is null");
        assertEquals(0, response.rowCount());

        execute("select name from test where name is not null and name!=''");
        assertEquals(1, response.rowCount());
        assertEquals("Ruben Lenten", response.rows()[0][0]);

    }

    @Test
    public void testFilterByBoolean() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "sunshine", "type=boolean,index=not_analyzed")
                .execute().actionGet();

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
     * @throws Exception
     */
    @Test
    public void testColsAreCaseSensitive() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string,store=true,index=not_analyzed",
                        "firstname", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();

        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"firstname\":\"LowerCase\",\"firstName\":\"CamelCase\"}")
                .execute().actionGet();

        execute(
                "select FIRSTNAME, \"firstname\", \"firstName\" from test");
        assertArrayEquals(new String[]{"firstname", "firstname", "firstName"}, response.cols());
        assertEquals(1, response.rowCount());
        assertEquals("LowerCase", response.rows()[0][0]);
        assertEquals("LowerCase", response.rows()[0][1]);
        assertEquals("CamelCase", response.rows()[0][2]);
    }


    @Test
    public void testIdSelectWithResult() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test");
        assertArrayEquals(new String[]{"_id"}, response.cols());
        assertEquals(1, response.rowCount());
        assertEquals(1, response.rows()[0].length);
        assertEquals("id1", response.rows()[0][0]);
    }

    @Test
    public void testDelete() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        refresh();
        execute("delete from test");
        assertEquals(-1, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        execute("select \"_id\" from test");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testDeleteWithWhere() throws Exception {
        createIndex("test");
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
    public void testSelectSource() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{\"a\":1}")
                .execute().actionGet();
        refresh();
        execute("select \"_source\" from test");
        assertArrayEquals(new String[]{"_source"}, response.cols());
        assertEquals(1, response.rowCount());
        assertEquals(1, response.rows()[0].length);
        assertEquals(1, (long) ((Map<String, Integer>) response.rows()[0][0]).get("a"));
    }

    @Test
    public void testSelectObject() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1")
                .setSource("{\"a\":{\"nested\":2}}")
                .execute().actionGet();
        refresh();

        execute("select a from test");
        assertArrayEquals(new String[]{"a"}, response.cols());
        assertEquals(1, response.rowCount());
        assertEquals(1, response.rows()[0].length);
        assertEquals(2, (long) ((Map<String, Integer>) response.rows()[0][0]).get("nested"));
    }


    @Test
    public void testSqlRequestWithLimit() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test limit 1");
        assertEquals(1, response.rowCount());
    }


    @Test
    public void testSqlRequestWithLimitAndOffset() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id3").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test limit 1 offset 1");
        assertEquals(1, response.rowCount());
    }


    @Test
    public void testSqlRequestWithFilter() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test where \"_id\"='id1'");
        assertEquals(1, response.rowCount());
        assertEquals("id1", response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithNotEqual() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test where \"_id\"!='id1'");
        assertEquals(1, response.rowCount());
        assertEquals("id2", response.rows()[0][0]);
    }


    @Test
    public void testSqlRequestWithOneOrFilter() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id3").setSource("{}").execute().actionGet();
        refresh();
        execute(
                "select \"_id\" from test where \"_id\"='id1' or \"_id\"='id3'");
        assertEquals(2, response.rowCount());
        assertThat(this.<String>getCol(response.rows(), 0), containsInAnyOrder("id1", "id3"));
    }

    @Test
    public void testSqlRequestWithOneMultipleOrFilter() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id3").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id4").setSource("{}").execute().actionGet();
        refresh();
        execute(
                "select \"_id\" from test where \"_id\"='id1' or \"_id\"='id2' or \"_id\"='id4'");
        assertEquals(3, response.rowCount());
        System.out.println(Arrays.toString(response.rows()[0]));
        System.out.println(Arrays.toString(response.rows()[1]));
        System.out.println(Arrays.toString(response.rows()[2]));

        List<String> col1 = this.getCol(response.rows(), 0);
        assertThat(col1, containsInAnyOrder("id1", "id2", "id4"));
    }

    @Test
    public void testSqlRequestWithDateFilter() throws Exception {
        prepareCreate("test")
                .addMapping("default", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("default")
                            .startObject("properties")
                                .startObject("date")
                                    .field("type", "date")
                                .endObject()
                            .endObject()
                        .endObject().endObject())
                .execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "default", "id1")
                .setSource("{\"date\": " +
                        new TimeStampSQLType().mappedValue("2013-10-01") + "}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2")
                .setSource("{\"date\": " +
                        new TimeStampSQLType().mappedValue("2013-10-02") + "}")
                .execute().actionGet();
        refresh();
        execute(
                "select date from test where date = '2013-10-01'");
        assertEquals(1, response.rowCount());
        assertEquals(1380585600000L, response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithDateGtFilter() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "date", "type=date")
                .execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "default", "id1")
                .setSource("{\"date\": " +
                        new TimeStampSQLType().mappedValue("2013-10-01") + "}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2")
                .setSource("{\"date\":" +
                        new TimeStampSQLType().mappedValue("2013-10-02") + "}")
                .execute().actionGet();
        refresh();
        execute(
                "select date from test where date > '2013-10-01'");
        assertEquals(1, response.rowCount());
        assertEquals(1380672000000L, response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithNumericGtFilter() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "i", "type=long")
                .execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "default", "id1")
                .setSource("{\"i\":10}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2")
                .setSource("{\"i\":20}")
                .execute().actionGet();
        refresh();
        execute(
                "select i from test where i > 10");
        assertEquals(1, response.rowCount());
        assertEquals(20, response.rows()[0][0]);
    }


    @Test
    public void testInsertWithColumnNames() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string,store=true,index=not_analyzed",
                        "lastName", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();
        execute("insert into test (\"firstName\", \"lastName\") values('Youri', 'Zoon')");
        assertEquals(1, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select * from test where \"firstName\" = 'Youri'");

        assertEquals(1, response.rowCount());
        assertEquals("Youri", response.rows()[0][0]);
        assertEquals("Zoon", response.rows()[0][1]);
    }

    @Test
    public void testInsertWithoutColumnNames() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string,store=true,index=not_analyzed",
                        "lastName", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();
        execute("insert into test values('Youri', 'Zoon')");
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test where \"firstName\" = 'Youri'");

        assertEquals(1, response.rowCount());
        assertEquals("Youri", response.rows()[0][0]);
        assertEquals("Zoon", response.rows()[0][1]);
    }

    @Test
    public void testInsertAllCoreDatatypes() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "boolean", "type=boolean",
                        "datetime", "type=date",
                        "double", "type=double",
                        "float", "type=float",
                        "integer", "type=integer",
                        "long", "type=long",
                        "short", "type=short",
                        "string", "type=string,index=not_analyzed")
                .execute().actionGet();

        execute("insert into test values(true, '2013-09-10T21:51:43', 1.79769313486231570e+308, 3.402, 2147483647, 9223372036854775807, 32767, 'Youri')");
        execute("insert into test values(?, ?, ?, ?, ?, ?, ?, ?)",
            new Object[] { true, "2013-09-10T21:51:43", 1.79769313486231570e+308, 3.402, 2147483647, 9223372036854775807L, 32767, "Youri" });
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test");

        assertEquals(2, response.rowCount());
        assertEquals(true, response.rows()[0][0]);
        assertEquals(1378849903000L, response.rows()[0][1]);
        assertEquals(1.79769313486231570e+308, response.rows()[0][2]);
        assertEquals(3.402, response.rows()[0][3]);
        assertEquals(2147483647, response.rows()[0][4]);
        assertEquals(9223372036854775807L, response.rows()[0][5]);
        assertEquals(32767, response.rows()[0][6]);
        assertEquals("Youri", response.rows()[0][7]);

        assertEquals(true, response.rows()[1][0]);
        assertEquals(1378849903000L, response.rows()[1][1]);
        assertEquals(1.79769313486231570e+308, response.rows()[1][2]);
        assertEquals(3.402, response.rows()[1][3]);
        assertEquals(2147483647, response.rows()[1][4]);
        assertEquals(9223372036854775807L, response.rows()[1][5]);
        assertEquals(32767, response.rows()[1][6]);
        assertEquals("Youri", response.rows()[1][7]);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testArraySupport() throws Exception {
        execute("create table t1 (id int primary key, strings array(string), integers array(integer)) replicas 0");
        ensureGreen();

        execute("insert into t1 (id, strings, integers) values (?, ?, ?)",
                new Object[] {
                        1,
                        new String[] {"foo", "bar"},
                        new Integer[] {1, 2, 3}
                }
        );
        refresh();

        execute("select id, strings, integers from t1");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer)response.rows()[0][0], is(1));
        assertThat(((List<String>)response.rows()[0][1]).get(0), is("foo"));
        assertThat(((List<String>)response.rows()[0][1]).get(1), is("bar"));
        assertThat(((List<Integer>)response.rows()[0][2]).get(0), is(1));
        assertThat(((List<Integer>)response.rows()[0][2]).get(1), is(2));
        assertThat(((List<Integer>)response.rows()[0][2]).get(2), is(3));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testArrayInsideObject() throws Exception {
        execute("create table t1 (id int primary key, details object as (names array(string))) replicas 0");
        ensureGreen();

        Map<String, Object> details = new HashMap<>();
        details.put("names", new Object[]{"Arthur", "Trillian"});
        execute("insert into t1 (id, details) values (?, ?)", new Object[] { 1,  details});
        refresh();

        execute("select details['names'] from t1");
        assertThat(response.rowCount(), is(1L));
        assertThat(((List<String>)response.rows()[0][0]).get(0), is("Arthur"));
        assertThat(((List<String>)response.rows()[0][0]).get(1), is("Trillian"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testArraySupportWithNullValues() throws Exception {
        execute("create table t1 (id int primary key, strings array(string)) replicas 0");
        ensureGreen();

        execute("insert into t1 (id, strings) values (?, ?)",
                new Object[] {
                        1,
                        new String[] {"foo", null, "bar"},
                }
        );
        refresh();

        execute("select id, strings, integers from t1");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer)response.rows()[0][0], is(1));
        assertThat(((List<String>)response.rows()[0][1]).get(0), is("foo"));
        assertThat(((List<String>)response.rows()[0][1]).get(1), is((String)null));
        assertThat(((List<String>)response.rows()[0][1]).get(2), is("bar"));
    }

    @Test
    public void testObjectArrayInsertAndSelect() throws Exception {
        execute("create table t1 (id int primary key, objects array(object as (name string, age int))) replicas 0");
        ensureGreen();

        ImmutableMap<String, ? extends Serializable> obj1 = ImmutableMap.of("name", "foo", "age", 1);
        ImmutableMap<String, ? extends Serializable> obj2 = ImmutableMap.of("name", "bar", "age", 2);

        Object[] args = new Object[] { 1, new Object[] {obj1, obj2 }};
        execute("insert into t1 (id, objects) values (?, ?)", args);
        refresh();

        execute("select objects from t1");
        assertThat(response.rowCount(), is(1L));

        List<Map<String, Object>> objResults = (List<Map<String, Object>>) response.rows()[0][0];
        Map<String, Object> obj1Result = objResults.get(0);
        assertThat((String) obj1Result.get("name"), is("foo"));
        assertThat((Integer) obj1Result.get("age"), is(1));

        Map<String, Object> obj2Result = objResults.get(1);
        assertThat((String) obj2Result.get("name"), is("bar"));
        assertThat((Integer) obj2Result.get("age"), is(2));

        execute("select objects['name'] from t1");
        assertThat(response.rowCount(), is(1L));

        List<String> names = (List<String>)response.rows()[0][0];
        assertThat(names.get(0), is("foo"));
        assertThat(names.get(1), is("bar"));

        execute("select objects['name'] from t1 where objects['name'] = ?", new Object[]{"foo"});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsertCoreTypesAsArray() throws Exception {
         execute("create table test (" +
                    "\"boolean\" array(boolean), " +
                    "\"datetime\" array(timestamp), " +
                    "\"double\" array(double), " +
                    "\"float\" array(float), " +
                    "\"integer\" array(integer), " +
                    "\"long\" array(long), " +
                    "\"short\" array(short), " +
                    "\"string\" array(string) " +
                ") replicas 0"
        );
        ensureGreen();

        execute("insert into test values(?, ?, ?, ?, ?, ?, ?, ?)",
            new Object[] {
                new Boolean[] {true, false},
                new String[] {"2013-09-10T21:51:43", "2013-11-10T21:51:43"},
                new Double[] {1.79769313486231570e+308, 1.69769313486231570e+308},
                new Float[] {3.402f, 3.403f, null },
                new Integer[] {2147483647, 234583},
                new Long[] { 9223372036854775807L, 4L },
                new Short[] {32767, 2 },
                new String[] {"Youri", "Juri"}
            }
        );
        refresh();

        execute("select * from test");
        assertEquals(true, ((List<Boolean>)response.rows()[0][0]).get(0));
        assertEquals(false, ((List<Boolean>)response.rows()[0][0]).get(1));

        assertThat( ((List<Long>)response.rows()[0][1]).get(0), is(1378849903000L));
        assertThat( ((List<Long>)response.rows()[0][1]).get(1), is(1384120303000L));

        assertThat( ((List<Double>)response.rows()[0][2]).get(0), is(1.79769313486231570e+308));
        assertThat( ((List<Double>)response.rows()[0][2]).get(1), is(1.69769313486231570e+308));

        assertThat( ((List<Double>)response.rows()[0][3]).get(0), is(3.402));
        assertThat( ((List<Double>)response.rows()[0][3]).get(1), is(3.403));
        assertThat( ((List<Double>)response.rows()[0][3]).get(2), nullValue());

        assertThat( ((List<Integer>)response.rows()[0][4]).get(0), is(2147483647));
        assertThat( ((List<Integer>)response.rows()[0][4]).get(1), is(234583));

        assertThat( ((List<Long>)response.rows()[0][5]).get(0), is(9223372036854775807L));
        assertThat( ((List<Integer>)response.rows()[0][5]).get(1), is(4));

        assertThat( ((List<Integer>)response.rows()[0][6]).get(0), is(32767));
        assertThat( ((List<Integer>)response.rows()[0][6]).get(1), is(2));

        assertThat( ((List<String>)response.rows()[0][7]).get(0), is("Youri"));
        assertThat( ((List<String>)response.rows()[0][7]).get(1), is("Juri"));
    }

    @Test
    public void testInsertMultipleRows() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "age", "type=integer",
                        "name", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();

        execute("insert into test values(32, 'Youri'), (42, 'Ruben')");
        assertEquals(2, response.rowCount());
        refresh();

        execute("select * from test order by \"name\"");

        assertEquals(2, response.rowCount());
        assertArrayEquals(new Object[]{42, "Ruben"}, response.rows()[0]);
        assertArrayEquals(new Object[]{32, "Youri"}, response.rows()[1]);
    }

    @Test
    public void testInsertWithParams() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "age", "type=integer",
                        "name", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();

        Object[] args = new Object[] {32, "Youri"};
        execute("insert into test values(?, ?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test where name = 'Youri'");

        assertEquals(1, response.rowCount());
        assertEquals(32, response.rows()[0][0]);
        assertEquals("Youri", response.rows()[0][1]);
    }

    @Test
    public void testInsertMultipleRowsWithParams() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "age", "type=integer",
                        "name", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();

        Object[] args = new Object[] {32, "Youri", 42, "Ruben"};
        execute("insert into test values(?, ?), (?, ?)", args);
        assertEquals(2, response.rowCount());
        refresh();

        execute("select * from test order by \"name\"");

        assertEquals(2, response.rowCount());
        assertArrayEquals(new Object[]{42, "Ruben"}, response.rows()[0]);
        assertArrayEquals(new Object[]{32, "Youri"}, response.rows()[1]);
    }

    @Test
    public void testInsertObject() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "message", "type=string,store=true,index=not_analyzed",
                        "person", "type=object,store=true")
                .execute().actionGet();

        Map<String, String> person = new HashMap<String, String>();
        person.put("first_name", "Youri");
        person.put("last_name", "Zoon");
        Object[] args = new Object[] {"I'm addicted to kite", person};

        execute("insert into test values(?, ?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test");

        assertEquals(1, response.rowCount());
        assertArrayEquals(args, response.rows()[0]);
    }

    @Test
    public void testUpdate() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "message", "type=string,index=not_analyzed")
                .execute().actionGet();

        execute("insert into test values('hello'),('again')");
        assertEquals(2, response.rowCount());
        refresh();

        execute("update test set message='b' where message = 'hello'");

        assertEquals(1, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select message from test where message='b'");
        assertEquals(1, response.rowCount());
        assertEquals("b", response.rows()[0][0]);

    }

    @Test
    public void testUpdateMultipleDocuments() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "message", "type=string,index=not_analyzed")
                .execute().actionGet();

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
        prepareCreate("test")
            .addMapping("default",
                    "col1", "type=string,index=not_analyzed",
                    "col2", "type=string,index=not_analyzed")
            .execute().actionGet();

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
        prepareCreate("test")
                .addMapping("default",
                        "coolness", "type=float,index=not_analyzed")
                .execute().actionGet();

        execute("insert into test values(1.1),(2.2)");
        assertEquals(2, response.rowCount());
        refresh();

        execute("update test set coolness=3.3 where coolness = ?", new Object[]{2.2});

        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness from test where coolness=3.3");
        assertEquals(1, response.rowCount());
        assertEquals(3.3, response.rows()[0][0]);

    }

    @Test
    public void testUpdateNestedObjectWithoutDetailedSchema() throws Exception {
        execute("create table test (coolness object)");
        ensureGreen();

        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", 2);
        Object[] args = new Object[] { map };

        execute("insert into test values (?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("update test set coolness['x'] = '3'");

        assertEquals(1, response.rowCount());
        refresh();

        execute("select coolness['x'], coolness['y'] from test");
        assertEquals(1, response.rowCount());
        assertEquals("3", response.rows()[0][0]);
        assertEquals(2, response.rows()[0][1]);
    }

    @Test
    public void testUpdateNestedNestedObject() throws Exception {
        Settings settings = settingsBuilder()
            .put("mapper.dynamic", true)
            .put("number_of_replicas", 0)
            .build();
        prepareCreate("test")
            .setSettings(settings)
            .execute().actionGet();
        ensureGreen();

        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", 2);
        Object[] args = new Object[] { map };

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
        Settings settings = settingsBuilder()
            .put("mapper.dynamic", true)
            .put("number_of_replicas", 0)
            .build();
        prepareCreate("test")
            .setSettings(settings)
            .execute().actionGet();
        ensureGreen();

        Map<String, Object> map = newHashMap();
        Map<String, Object> nestedMap = newHashMap();
        nestedMap.put("y", 2);
        nestedMap.put("z", 3);
        map.put("x", nestedMap);
        Object[] args = new Object[] { map };

        execute("insert into test (a) values (?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("update test set a['x']['z'] = ?", new Object[] { null });

        assertEquals(1, response.rowCount());
        refresh();

        execute("select a['x']['y'], a['x']['z'] from test");
        assertEquals(1, response.rowCount());
        assertEquals(2, response.rows()[0][0]);
        assertNull(response.rows()[0][1]);
    }

    @Test
    public void testUpdateNestedObjectDeleteWithoutArgs() throws Exception {
        Settings settings = settingsBuilder()
            .put("mapper.dynamic", true)
            .put("number_of_replicas", 0)
            .build();
        prepareCreate("test")
            .setSettings(settings)
            .execute().actionGet();
        ensureGreen();

        Map<String, Object> map = newHashMap();
        Map<String, Object> nestedMap = newHashMap();
        nestedMap.put("y", 2);
        nestedMap.put("z", 3);
        map.put("x", nestedMap);
        Object[] args = new Object[] { map };

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

    @Test(expected = SQLParseException.class)
    public void testUpdateWithNestedObjectArrayIdxAccess() throws Exception {
        execute("create table test (coolness array(float)) replicas 0");
        ensureGreen();
        execute("insert into test values (?)", new Object[]{new Object[]{2.2, 2.3, 2.4}});
        assertEquals(1, response.rowCount());
        refresh();

        execute("update test set coolness[0] = 3.3");
    }

    @Test
    public void testUpdateNestedObjectWithDetailedSchema() throws Exception {
        execute("create table test (coolness object as (x string, y string))");
        ensureGreen();
        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", "2");
        Object[] args = new Object[] { map };

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
        prepareCreate("test")
                .addMapping("default",
                        "coolness", "type=object,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();

        Map<String, Object> map = new HashMap<>();
        map.put("x", "1");
        map.put("y", 2);
        Object[] args = new Object[] { map };

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
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("default")
                    .startObject("_meta").field("primary_keys", "id").endObject()
                    .startObject("properties")
                        .startObject("id")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("data")
                            .field("type", "object")
                            .field("index", "not_analyzed")
                            .field("dynamic", false)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject();

        prepareCreate("test")
                .addMapping("default", mapping)
                .execute().actionGet();
        ensureGreen();

        Map<String, Object> data = new HashMap<String, Object>(){{
            put("foo", "bar");
            put("days", new ArrayList<String>(){{
                add("Mon");
                add("Tue");
                add("Wen");
            }});
        }};
        execute("insert into test (id, data) values (?, ?)", new Object[] { "1", data});
        refresh();

        execute("select data from test where id = ?", new Object[]{"1"});
        assertEquals(data, response.rows()[0][0]);

        Map<String, Object> new_data = new HashMap<String, Object>(){{
            put("days", new ArrayList<String>(){{
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
    public void testGetResponseWithObjectColumn() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("default")
                .startObject("_meta").field("primary_keys", "id").endObject()
                .startObject("properties")
                    .startObject("id")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                    .endObject()
                .startObject("data")
                    .field("type", "object")
                    .field("index", "not_analyzed")
                    .field("dynamic", false)
                .endObject()
            .endObject()
            .endObject()
            .endObject();

        prepareCreate("test")
            .addMapping("default", mapping)
            .execute().actionGet();
        ensureGreen();

        Map<String, Object> data = new HashMap<>();
        data.put("foo", "bar");
        execute("insert into test (id, data) values (?, ?)", new Object[]{"1", data});
        refresh();

        execute("select data from test where id = ?", new Object[]{"1"});
        assertEquals(data, response.rows()[0][0]);
    }

    @Test
    public void testUpdateResetNestedNestedObject() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "coolness", "type=object,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();

        Map<String, Object> map = new HashMap<String, Object>(){{
            put("x", "1");
            put("y", new HashMap<String, Object>(){{
                put("z", 3);
            }});
        }};

        execute("insert into test values (?)", new Object[]{map});
        assertEquals(1, response.rowCount());
        refresh();

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
    public void testInsertWithPrimaryKey() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[] { "1",
            "A towel is about the most massively useful thing an interstellar hitch hiker can have."};
        execute("insert into test (pk_col, message) values (?, ?)", args);
        refresh();

        GetResponse response = client().prepareGet("test", "default", "1").execute().actionGet();
        assertTrue(response.getSourceAsMap().containsKey("message"));
    }

    @Test
    public void testInsertWithPrimaryKeyMultiValues() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[] {
            "1", "All the doors in this spaceship have a cheerful and sunny disposition.",
            "2", "I always thought something was fundamentally wrong with the universe"
        };
        execute("insert into test (pk_col, message) values (?, ?), (?, ?)", args);
        refresh();

        GetResponse response = client().prepareGet("test", "default", "1").execute().actionGet();
        assertTrue(response.getSourceAsMap().containsKey("message"));
    }

    @Test (expected = DuplicateKeyException.class)
    public void testInsertWithUniqueConstraintViolation() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[] {
            "1", "All the doors in this spaceship have a cheerful and sunny disposition.",
        };
        execute("insert into test (pk_col, message) values (?, ?)", args);

        args = new Object[] {
            "1", "I always thought something was fundamentally wrong with the universe"
        };

        execute("insert into test (pk_col, message) values (?, ?)", args);
    }

    private void createTestIndexWithPkMapping() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject("default")
                    .startObject("_meta").field("primary_keys", "pk_col").endObject()
                    .startObject("properties")
                    .startObject("pk_col").field("type", "string").field("store",
                            "true").field("index", "not_analyzed").endObject()
                    .startObject("message").field("type", "string").field("store",
                            "true").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();

        prepareCreate("test")
            .addMapping("default", mapping)
                .execute().actionGet();
        ensureGreen();
    }

    @Test (expected = UnsupportedFeatureException.class)
    public void testMultiplePrimaryKeyColumns() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject("default")
                    .startObject("_meta").array("primary_keys", "pk_col1", "pk_col2").endObject()
                    .startObject("properties")
                    .startObject("pk_col1").field("type", "string").field("store",
                            "false").field("index", "not_analyzed").endObject()
                    .startObject("pk_col2").field("type", "string").field("store",
                            "false").field("index", "not_analyzed").endObject()
                    .startObject("message").field("type", "string").field("store",
                            "false").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();

        prepareCreate("test")
            .addMapping("default", mapping)
            .execute().actionGet();

        Object[] args = new Object[] {
            "Life, loathe it or ignore it, you can't like it."
        };
        execute("insert into test (pk_col1, pk_col2, message) values ('1', '2', ?)", args);
    }

    @Test (expected = CrateException.class)
    public void testInsertWithPKMissingOnInsert() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[] {
            "In the beginning the Universe was created.\n" +
                "This has made a lot of people very angry and been widely regarded as a bad move."
        };
        execute("insert into test (message) values (?)", args);
    }

    private void createTestIndexWithSomeIdPkMapping() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("default")
                .startObject("_meta").field("primary_keys", "some_id").endObject()
                .startObject("properties")
                .startObject("some_id").field("type", "string").field("store",
                        "true").field("index", "not_analyzed").endObject()
                .startObject("foo").field("type", "string").field("store",
                        "true").field("index", "not_analyzed").endObject()
                .endObject()
                .endObject()
                .endObject();

        prepareCreate("test")
                .addMapping("default", mapping)
                .execute().actionGet();
        ensureGreen();
    }

    @Test
    public void testSelectToGetRequestByPlanner() throws Exception {
        createTestIndexWithSomeIdPkMapping();

        execute("insert into test (some_id, foo) values ('124', 'bar1')");
        assertEquals(1, response.rowCount());
        refresh();

        execute("select some_id, foo from test where some_id='124'");
        assertEquals(1, response.rowCount());
        assertEquals("124", response.rows()[0][0]);
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
    }


    @Test
    public void testDeleteToDeleteRequestByPlanner() throws Exception {
        createTestIndexWithSomeIdPkMapping();

        execute("insert into test (some_id, foo) values ('123', 'bar')");
        assertEquals(1, response.rowCount());
        refresh();

        execute("delete from test where some_id='123'");
        assertEquals(1, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select * from test where some_id='123'");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testUpdateToUpdateRequestByPlanner() throws Exception {
        createTestIndexWithSomeIdPkMapping();

        execute("insert into test (some_id, foo) values ('123', 'bar')");
        assertEquals(1, response.rowCount());
        refresh();

        execute("update test set foo='bar1' where some_id='123'");
        assertEquals(1, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select foo from test where some_id='123'");
        assertEquals(1, response.rowCount());
        assertEquals("bar1", response.rows()[0][0]);
    }

    @Test
    public void testSelectToRoutedRequestByPlanner() throws Exception {
        createTestIndexWithSomeIdPkMapping();

        execute("insert into test (some_id, foo) values ('1', 'foo')");
        execute("insert into test (some_id, foo) values ('2', 'bar')");
        execute("insert into test (some_id, foo) values ('3', 'baz')");
        refresh();

        execute("SELECT * FROM test WHERE some_id='1' OR some_id='2'");
        assertEquals(2, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));

        execute("SELECT * FROM test WHERE some_id=? OR some_id=?", new Object[]{"1", "2"});
        assertEquals(2, response.rowCount());

        execute("SELECT * FROM test WHERE (some_id=? OR some_id=?) OR some_id=?", new Object[]{"1", "2", "3"});
        assertEquals(3, response.rowCount());
        assertThat(Arrays.asList(response.cols()), hasItems("some_id", "foo"));
    }

    @Test
    public void testSelectToRoutedRequestByPlannerMissingDocuments() throws Exception {
        createTestIndexWithSomeIdPkMapping();

        execute("insert into test (some_id, foo) values ('1', 'foo')");
        execute("insert into test (some_id, foo) values ('2', 'bar')");
        execute("insert into test (some_id, foo) values ('3', 'baz')");
        refresh();

        execute("SELECT some_id, foo FROM test WHERE some_id='4' OR some_id='3'");
        assertEquals(1, response.rowCount());
        assertThat(Arrays.asList(response.rows()[0]), hasItems(new Object[]{"3", "baz"}));
        assertThat(response.duration(), greaterThanOrEqualTo(0L));

        execute("SELECT some_id, foo FROM test WHERE some_id='4' OR some_id='99'");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testSelectToRoutedRequestByPlannerWhereIn() throws Exception {
        createTestIndexWithSomeIdPkMapping();

        execute("insert into test (some_id, foo) values ('1', 'foo')");
        execute("insert into test (some_id, foo) values ('2', 'bar')");
        execute("insert into test (some_id, foo) values ('3', 'baz')");
        refresh();

        execute("SELECT * FROM test WHERE some_id IN (?,?,?)", new Object[]{"1", "2", "3"});
        assertEquals(3, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
    }

    @Test
    public void testDeleteToRoutedRequestByPlannerWhereIn() throws Exception {
        createTestIndexWithSomeIdPkMapping();

        execute("insert into test (some_id, foo) values ('1', 'foo')");
        execute("insert into test (some_id, foo) values ('2', 'bar')");
        execute("insert into test (some_id, foo) values ('3', 'baz')");
        refresh();

        execute("DELETE FROM test WHERE some_Id IN (?, ?, ?)", new Object[]{"1", "2", "4"});
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("SELECT some_id FROM test");
        assertThat(response.rowCount(), is(1L));
        assertEquals(response.rows()[0][0], "3");

    }

    @Test
    public void testCountWithGroupByNullArgs() throws Exception {
        this.setup.groupBySetup();

        execute("select count(*), race from characters group by race", null);
        assertEquals(3, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
    }

    @Test
    public void testGroupByAndOrderByAlias() throws Exception {
        this.setup.groupBySetup();

        execute("select characters.race as test_race from characters group by characters.race order by characters.race");
        assertEquals(3, response.rowCount());

        execute("select characters.race as test_race from characters group by characters.race order by test_race");
        assertEquals(3, response.rowCount());
    }


    @Test
    public void testCountWithGroupByWithWhereClause() throws Exception {
        this.setup.groupBySetup();

        execute("select count(*), race from characters where race = 'Human' group by race");
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndLimit() throws Exception {
        this.setup.groupBySetup();

        execute("select count(*), race from characters group by race order by count(*) asc limit ?",
                new Object[] { 2 });

        assertEquals(2, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
        assertEquals("Android", response.rows()[0][1]);
        assertEquals(2L, response.rows()[1][0]);
        assertEquals("Vogon", response.rows()[1][1]);
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndSecondColumnAndLimit() throws Exception {
        this.setup.groupBySetup();

        execute("select count(*), gender, race from characters group by race, gender order by count(*) desc, race, gender asc limit 2");

        assertEquals(2L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals("female", response.rows()[0][1]);
        assertEquals("Human", response.rows()[0][2]);
        assertEquals(2L, response.rows()[1][0]);
        assertEquals("male", response.rows()[1][1]);
        assertEquals("Human", response.rows()[1][2]);
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndSecondColumnAndLimitAndOffset() throws Exception {
        this.setup.groupBySetup();

        execute("select count(*), gender, race from characters group by race, gender order by count(*) desc, race asc limit 2 offset 2");

        assertEquals(2, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals("male", response.rows()[0][1]);
        assertEquals("Vogon", response.rows()[0][2]);
        assertEquals(1L, response.rows()[1][0]);
        assertEquals("male", response.rows()[1][1]);
        assertEquals("Android", response.rows()[1][2]);
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndSecondColumnAndLimitAndTooLargeOffset() throws Exception {
        this.setup.groupBySetup();

        execute("select count(*), gender, race from characters group by race, gender order by count(*) desc, race asc limit 2 offset 20");

        assertEquals(0, response.rows().length);
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testCountWithGroupByOrderOnAggDescFuncAndLimit() throws Exception {
        this.setup.groupBySetup();

        execute("select count(*), race from characters group by race order by count(*) desc limit 2");

        assertEquals(2, response.rowCount());
        assertEquals(4L, response.rows()[0][0]);
        assertEquals("Human", response.rows()[0][1]);
        assertEquals(2L, response.rows()[1][0]);
        assertEquals("Vogon", response.rows()[1][1]);
    }

    @Test
    public void testCountWithGroupByOrderOnKeyAscAndLimit() throws Exception {
        this.setup.groupBySetup();


        execute("select count(*), race from characters group by race order by race asc limit 2");

        assertEquals(2, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
        assertEquals("Android", response.rows()[0][1]);
        assertEquals(4L, response.rows()[1][0]);
        assertEquals("Human", response.rows()[1][1]);
    }

    @Test
    public void testCountWithGroupByOrderOnKeyDescAndLimit() throws Exception {
        this.setup.groupBySetup();

        execute("select count(*), race from characters group by race order by race desc limit 2");

        assertEquals(2L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals("Vogon", response.rows()[0][1]);
        assertEquals(4L, response.rows()[1][0]);
        assertEquals("Human", response.rows()[1][1]);
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
            new Object[] {"Vo*", "male", "Kwaltzz", emptyMap }
        );
        execute("insert into characters (race, gender, name, details) values (?, ?, ?, ?)",
            new Object[] {"Vo?", "male", "Kwaltzzz", emptyMap }
        );
        execute("insert into characters (race, gender, name, details) values (?, ?, ?, ?)",
            new Object[] {"Vo!", "male", "Kwaltzzzz", details}
        );
        execute("insert into characters (race, gender, name, details) values (?, ?, ?, ?)",
            new Object[] {"Vo%", "male", "Kwaltzzzz", details}
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

//        execute("select count(*) from characters where age like 32");
//        assertEquals(1L, response.rows()[0][0]);
//
//        execute("select race from characters where details['age'] like 30");
//        assertEquals(2L, response.rowCount());

        execute("select race from characters where details['job'] like 'sol%'");
        assertEquals(2L, response.rowCount());
    }

    @Test
    public void testCreateTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        String expectedMapping = "{\"default\":{" +
                "\"_meta\":{\"primary_keys\":[\"col1\"]," +
                "\"columns\":{\"col1\":{},\"col2\":{}},\"indices\":{}}," +
                "\"_all\":{\"enabled\":false}," +
                "\"properties\":{" +
                    "\"col1\":{\"type\":\"integer\",\"doc_values\":true}," +
                    "\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":true}" +
                "}}}";

        String expectedSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.version.created\":\"1000199\"" +
                "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);

        // test index usage
        execute("insert into test (col1, col2) values (1, 'foo')");
        assertEquals(1, response.rowCount());
        refresh();
        execute("SELECT * FROM test");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testSqlAlchemyGeneratedCountWithStar() throws  Exception {
        // generated using sqlalchemy
        // session.query(func.count('*')).filter(Test.name == 'foo').scalar()

        execute("create table test (col1 integer primary key, col2 string) replicas 0");
        ensureGreen();
        execute("insert into test values (?, ?)", new Object[] { 1, "foo" });
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        refresh();

        execute(
            "SELECT count(?) AS count_1 FROM test WHERE test.col2 = ?",
            new Object[] { "*", "foo" }
        );
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testSqlAlchemyGeneratedCountWithPrimaryKeyCol() throws Exception {
        // generated using sqlalchemy
        // session.query(Test.col1).filter(Test.col2 == 'foo').scalar()

        execute("create table test (col1 integer primary key, col2 string) replicas 0");
        ensureGreen();
        execute("insert into test values (?, ?)", new Object[] { 1, "foo" });
        execute("insert into test values (?, ?)", new Object[] { 2, "bar" });
        refresh();

        execute(
            "SELECT count(test.col1) AS count_1 FROM test WHERE test.col2 = ?",
            new Object[] { "foo" }
        );
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testSqlAlchemyGroupByWithCountStar() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count('*'), Test.col2).group_by(Test.col2).order_by(desc(func.count('*'))).all()

        execute("create table test (col1 integer primary key, col2 string) replicas 0");
        ensureGreen();
        execute("insert into test values (?, ?)", new Object[] { 1, "foo" });
        execute("insert into test values (?, ?)", new Object[] { 2, "bar" });
        execute("insert into test values (?, ?)", new Object[]{3, "foo"});
        refresh();

        execute(
            "SELECT count(?) AS count_1, test.col2 AS test_col2 FROM test " +
                "GROUP BY test.col2 order by count_1 desc",
            new Object[] { "*" }
        );

        assertEquals(2L, response.rows()[0][0]);
    }

    @Test
    public void testSqlAlchemyGroupByWithPrimaryKeyCol() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count(Test.col1), Test.col2).group_by(Test.col2).order_by(desc(func.count(Test.col1))).all()


        execute("create table test (col1 integer primary key, col2 string) replicas 0");
        ensureGreen();
        execute("insert into test values (?, ?)", new Object[] { 1, "foo" });
        execute("insert into test values (?, ?)", new Object[] { 2, "bar" });
        execute("insert into test values (?, ?)", new Object[] { 3, "foo" });
        refresh();

        execute(
            "SELECT count(test.col1) AS count_1, test.col2 AS test_col2 FROM test " +
                "GROUP BY test.col2 order by count_1 desc"
        );

        assertEquals(2L, response.rows()[0][0]);
    }

    @Test(expected = TableAlreadyExistsException.class)
    public void testCreateTableAlreadyExistsException() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        execute("create table test (col1 integer primary key, col2 string)");
    }

    @Test
    public void testCreateTableWithReplicasAndShards() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) replicas 2" +
                "clustered by (col1) into 10 shards");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        String expectedMapping = "{\"default\":{" +
                "\"_meta\":{\"primary_keys\":\"col1\"}," +
                "\"_all\":{\"enabled\":false}," +
                "\"properties\":{" +
                "\"col1\":{\"type\":\"integer\"}," +
                "\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\"}" +
                "}}}";

        String expectedSettings = "{\"test\":{" +
                "\"settings\":{" +
                    "\"index.number_of_replicas\":\"2\"," +
                    "\"index.number_of_shards\":\"10\"," +
                    "\"index.version.created\":\"1000199\"" +
                "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);
    }

    @Test
    public void testGroupByNestedObject() throws Exception {
        this.setup.groupBySetup();

        execute("select count(*), details['job'] from characters group by details['job'] order by count(*), details['job']");
        assertEquals(3, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
        assertEquals("Mathematician", response.rows()[0][1]);
        assertEquals(1L, response.rows()[1][0]);
        assertEquals("Sandwitch Maker", response.rows()[1][1]);
        assertEquals(5L, response.rows()[2][0]);
        assertNull(null, response.rows()[2][1]);
    }

    @Test
    public void testGroupByEmpty() throws Exception {
        execute("create table test (col1 string)");
        waitForRelocation(ClusterHealthStatus.GREEN);

        execute("select count(*), col1 from test group by col1");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGroupByMultiValueField() throws Exception {
        expectedException.expect(GroupByOnArrayUnsupportedException.class);
        this.setup.groupBySetup();
        // inserting multiple values not supported anymore
        client().prepareIndex("characters", Constants.DEFAULT_MAPPING_TYPE).setSource(new HashMap<String, Object>(){{
            put("race", new String[] {"Android"});
            put("gender", new String[]{"male", "robot"});
            put("name", "Marvin2");
        }}).execute().actionGet();
        client().prepareIndex("characters", Constants.DEFAULT_MAPPING_TYPE).setSource(new HashMap<String, Object>(){{
            put("race", new String[] {"Android"});
            put("gender", new String[]{"male", "robot"});
            put("name", "Marvin3");
        }}).execute().actionGet();
        refresh();
        execute("select gender from characters group by gender");
    }

    @Test
    public void testDropTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        execute("drop table test");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        assertFalse(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());
    }

    @Test (expected = TableUnknownException.class)
    public void testDropUnknownTable() throws Exception {
        execute("drop table test");
    }

    @Test
    public void selectMultiGetRequestWithColumnAlias() throws IOException {
        createTestIndexWithSomeIdPkMapping();
        execute("insert into test (some_id, foo) values ('1', 'foo')");
        execute("insert into test (some_id, foo) values ('2', 'bar')");
        execute("insert into test (some_id, foo) values ('3', 'baz')");
        refresh();
        execute("SELECT some_id as id, foo from test where some_id IN (?,?)", new Object[]{'1', '2'});
        assertThat(response.rowCount(), is(2L));
        assertThat(response.cols(), arrayContainingInAnyOrder("id", "foo"));
        assertThat(new String[]{(String) response.rows()[0][0], (String) response.rows()[1][0]}, arrayContainingInAnyOrder("1", "2"));
    }

    @Test (expected = TableUnknownException.class)
    public void selectMultiGetRequestFromNonExistentTable() throws IOException {
        execute("SELECT * FROM \"non_existent\" WHERE \"_id\" in (?,?)", new Object[]{"1", "2"});
    }

    @Test
    public void testDeleteWhereVersion() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) replicas 0");
        ensureGreen();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertEquals(1L, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
        Long version = (Long)response.rows()[0][0];

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
    public void testSelectMatch() throws Exception {
        execute("create table quotes (quote string)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        execute("insert into quotes values (?)", new Object[]{"don't panic"});
        refresh();

        execute("select quote from quotes where match(quote, ?)", new Object[]{"don't panic"});
        assertEquals(1L, response.rowCount());
        assertEquals("don't panic", response.rows()[0][0]);
    }

    @Test
    public void testSelectNotMatch() throws Exception {
        execute("create table quotes (quote string)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        execute("insert into quotes values (?), (?)", new Object[]{"don't panic", "hello"});
        refresh();

        execute("select quote from quotes where not match(quote, ?)",
                new Object[]{"don't panic"});
        assertEquals(1L, response.rowCount());
        assertEquals("hello", response.rows()[0][0]);
    }

    @Test
    public void testSelectOrderByScore() throws Exception {
        execute("create table quotes (quote string index off," +
                "index quote_ft using fulltext(quote))");
        ensureGreen();
        execute("insert into quotes values (?)",
                new Object[]{"Would it save you a lot of time if I just gave up and went mad now?"}
        );
        execute("insert into quotes values (?)",
                new Object[]{"Time is an illusion. Lunchtime doubly so"}
        );
        refresh();

        execute("select * from quotes");
        execute("select quote, \"_score\" from quotes where match(quote_ft, ?) " +
                "order by \"_score\" desc",
                new Object[]{"time", "time"});
        assertEquals(2L, response.rowCount());
        assertEquals("Time is an illusion. Lunchtime doubly so", response.rows()[0][0]);
    }

    @Test
    public void testCreateTableWithInlineDefaultIndex() throws Exception {
        execute("create table quotes (quote string index using plain)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes values (?)", new Object[]{quote});
        refresh();

        // matching does not work on plain indexes
        execute("select quote from quotes where match(quote, 'time')");
        assertEquals(0, response.rowCount());

        // filtering on the actual value does work
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);
    }

    @Test
    public void testCreateTableWithInlineIndex() throws Exception {
        execute("create table quotes (quote string index using fulltext)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes values (?)", new Object[]{quote});
        refresh();

        execute("select quote from quotes where match(quote, 'time')");
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);

        // filtering on the actual value does not work anymore because its now indexed using the
        // standard analyzer
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testCreateTableWithIndexOff() throws Exception {
        execute("create table quotes (id int, quote string index off)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes (id, quote) values (?, ?)", new Object[]{1, quote});
        refresh();

        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(0, response.rowCount());

        execute("select quote from quotes where id = 1");
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);
    }

    @Test
    public void testCreateTableWithIndex() throws Exception {
        execute("create table quotes (quote string, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='english'))");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes values (?)", new Object[]{quote});
        refresh();

        execute("select quote from quotes where match(quote_fulltext, 'time')");
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);

        // filtering on the actual value does still work
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testCreateTableWithCompositeIndex() throws Exception {
        execute("create table novels (title string, description string, " +
                "index title_desc_fulltext using fulltext(title, description) " +
                "with(analyzer='english'))");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("novels"))
                .actionGet().isExists());

        String title = "So Long, and Thanks for All the Fish";
        String description = "Many were increasingly of the opinion that they'd all made a big " +
                "mistake in coming down from the trees in the first place. And some said that " +
                "even the trees had been a bad move, and that no one should ever have left " +
                "the oceans.";
        execute("insert into novels (title, description) values(?, ?)",
                new Object[]{title, description});
        refresh();

        // match token existing at field `title`
        execute("select title, description from novels where match(title_desc_fulltext, 'fish')");
        assertEquals(1L, response.rowCount());
        assertEquals(title, response.rows()[0][0]);
        assertEquals(description, response.rows()[0][1]);

        // match token existing at field `description`
        execute("select title, description from novels where match(title_desc_fulltext, 'oceans')");
        assertEquals(1L, response.rowCount());
        assertEquals(title, response.rows()[0][0]);
        assertEquals(description, response.rows()[0][1]);

        // filtering on the actual values does still work
        execute("select title from novels where title = ?", new Object[]{title});
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testSelectScoreMatchAll() throws Exception {
        execute("create table quotes (quote string)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        execute("insert into quotes values (?), (?)",
                new Object[]{"Would it save you a lot of time if I just gave up and went mad now?",
                        "Time is an illusion. Lunchtime doubly so"}
        );
        refresh();

        execute("select quote, \"_score\" from quotes");
        assertEquals(2L, response.rowCount());
        assertEquals(1.0f, response.rows()[0][1]);
        assertEquals(1.0f, response.rows()[1][1]);
    }

    @Test
    public void testSelectWhereScore() throws Exception {
        execute("create table quotes (quote string, " +
                "index quote_ft using fulltext(quote))");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        execute("insert into quotes values (?), (?)",
                new Object[]{"Would it save you a lot of time if I just gave up and went mad now?",
                        "Time is an illusion. Lunchtime doubly so. Take your time."}
        );
        refresh();

        execute("select quote, \"_score\" from quotes where match(quote_ft, 'time') " +
                "and \"_score\" > 0.98");
        assertEquals(1L, response.rowCount());
        assertEquals(1, ((Float)response.rows()[0][1]).compareTo(0.98f));
    }

    @Test
    public void testSelectMatchAnd() throws Exception {
        execute("create table quotes (id int, quote string, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='english'))");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        execute("insert into quotes (id, quote) values (?, ?), (?, ?)",
                new Object[]{
                        1, "Would it save you a lot of time if I just gave up and went mad now?",
                        2, "Time is an illusion. Lunchtime doubly so"}
        );
        refresh();

        execute("select quote from quotes where match(quote_fulltext, 'time') and id = 1");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testSelectFromInformationSchema() throws Exception {
        execute("create table quotes (" +
                "id integer primary key, " +
                "quote string index off, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='snowball')" +
                ") clustered by (id) into 3 shards replicas 10");
        refresh();

        execute("select table_name, number_of_shards, number_of_replicas, clustered_by from " +
                "information_schema.tables " +
                "where table_name='quotes'");
        assertEquals(1L, response.rowCount());
        assertEquals("quotes", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals(10, response.rows()[0][2]);
        assertEquals("id", response.rows()[0][3]);
        assertThat(response.duration(), greaterThanOrEqualTo(0L));

        execute("select * from information_schema.columns where table_name='quotes'");
        assertEquals(2L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));


        execute("select * from information_schema.table_constraints where schema_name='doc'");
        assertEquals(1L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));

//        // TODO: more information_schema tables
//        execute("select * from information_schema.indices");
//        assertEquals(2L, response.rowCount());
//        assertEquals("id", response.rows()[0][1]);
//        assertEquals("quote_fulltext", response.rows()[1][1]);
//        assertThat(response.duration(), greaterThanOrEqualTo(0L));
//
//        execute("select * from information_schema.routines");
//        assertEquals(103L, response.rowCount());
//        assertThat(response.duration(), greaterThanOrEqualTo(0L));
    }

    @Test ( expected = CrateException.class)
    public void testSelectSysColumnsFromInformationSchema() throws Exception {
        execute("select sys.nodes.id, table_name, number_of_replicas from information_schema.tables");
    }

    private void nonExistingColumnSetup() {
        execute("create table quotes (" +
                "id integer primary key, " +
                "quote string index off, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='snowball')" +
                ") clustered by (id) into 3 shards");
        refresh();
        execute("insert into quotes (id, quote) values (1, '\"Nothing particularly exciting," +
                "\" it admitted, \"but they are alternatives.\"')");
        execute("insert into quotes (id, quote) values (2, '\"Have another drink," +
                "\" said Trillian. \"Enjoy yourself.\"')");
        refresh();
    }

    @Test
    public void selectNonExistingColumn() throws Exception {
        nonExistingColumnSetup();
        execute("select notExisting from quotes");
        assertEquals(2L, response.rowCount());
        assertEquals("notexisting", response.cols()[0]);
        assertNull(response.rows()[0][0]);
        assertNull(response.rows()[1][0]);
    }

    @Test
    public void selectNonExistingAndExistingColumns() throws Exception {
        nonExistingColumnSetup();
        execute("select \"unknown\", id from quotes order by id asc");
        assertEquals(2L, response.rowCount());
        assertEquals("unknown", response.cols()[0]);
        assertEquals("id", response.cols()[1]);
        assertNull(response.rows()[0][0]);
        assertEquals(1, response.rows()[0][1]);
        assertNull(response.rows()[1][0]);
        assertEquals(2, response.rows()[1][1]);
    }

    @Test
    public void selectWhereNonExistingColumn() throws Exception {
        nonExistingColumnSetup();
        execute("select * from quotes where something > 0");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereDynamicColumnIsNull() throws Exception {
        nonExistingColumnSetup();
        // dynamic fields are not indexed, so we just don't know if it matches
        execute("select * from quotes where something IS NULL");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void selectWhereNonExistingColumnWhereIn() throws Exception {
        nonExistingColumnSetup();
        execute("select * from quotes where something IN(1,2,3)");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereNonExistingColumnLike() throws Exception {
        nonExistingColumnSetup();
        execute("select * from quotes where something Like '%bla'");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereNonExistingColumnMatchFunction() throws Exception {
        nonExistingColumnSetup();
        execute("select * from quotes where match(something, 'bla')");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectOrderByNonExistingColumn() throws Exception {
        nonExistingColumnSetup();
        execute("SELECT * from quotes");
        SQLResponse responseWithoutOrder = response;
        execute("SELECT * from quotes order by something");
        assertEquals(responseWithoutOrder.rowCount(), response.rowCount());
        for (int i=0;i<response.rowCount();i++) {
            assertArrayEquals(responseWithoutOrder.rows()[i], response.rows()[i]);
        }
    }

    @Test
    public void testCopyFromFile() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext)");
        refresh();

        String filePath = Joiner.on(File.separator).join(copyFilePath, "test_copy_from.json");
        execute("copy quotes from ?", new Object[]{filePath});
        // 2 nodes on same machine resulting in double affected rows
        assertEquals(6L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testCopyFromDirectory() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext)");

        execute("copy quotes from ?", new Object[]{copyFilePath});
        // 2 nodes on same machine resulting in double affected rows
        assertEquals(6L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testCopyFromFilePattern() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext)");

        String filePath = Joiner.on(File.separator).join(copyFilePath, "(\\D).json");
        execute("copy quotes from ?", new Object[]{filePath});
        // 2 nodes on same machine resulting in double affected rows
        assertEquals(6L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testSelectTableAlias() throws Exception {
        execute("create table quotes_en (id int primary key, quote string) replicas 0");
        execute("create table quotes_de (id int primary key, quote string) replicas 0");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();

        execute("insert into quotes_en values (?,?)", new Object[]{1, "Don't panic"});
        assertEquals(1, response.rowCount());
        execute("insert into quotes_de values (?,?)", new Object[]{2, "Keine Panik"});
        assertEquals(1, response.rowCount());
        refresh();

        execute("select quote from quotes where id = ?", new Object[]{1});
        assertEquals(1, response.rowCount());
        execute("select quote from quotes where id = ?", new Object[]{2});
        assertEquals(1, response.rowCount());
    }

    @Test (expected = TableAliasSchemaException.class)
    public void testSelectTableAliasSchemaExceptionColumnDefinition() throws Exception {
        execute("create table quotes_en (id int primary key, quote string, author string)");
        execute("create table quotes_de (id int primary key, quote2 string)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test (expected = TableAliasSchemaException.class)
    public void testSelectTableAliasSchemaExceptionColumnDataType() throws Exception {
        execute("create table quotes_en (id int primary key, quote int) replicas 0");
        execute("create table quotes_de (id int primary key, quote string) replicas 0");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test (expected = TableAliasSchemaException.class)
    public void testSelectTableAliasSchemaExceptionPrimaryKeyRoutingColumn() throws Exception {
        execute("create table quotes_en (id int primary key, quote string)");
        execute("create table quotes_de (id int, quote string)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test (expected = TableAliasSchemaException.class)
    public void testSelectTableAliasSchemaExceptionIndices() throws Exception {
        execute("create table quotes_en (id int primary key, quote string)");
        execute("create table quotes_de (id int primary key, quote2 string index using fulltext)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test
    public void testCountWithGroupByTableAlias() throws Exception {
        execute("create table characters_guide (race string, gender string, name string)");
        execute("insert into characters_guide (race, gender, name) values ('Human', 'male', 'Arthur Dent')");
        execute("insert into characters_guide (race, gender, name) values ('Android', 'male', 'Marving')");
        execute("insert into characters_guide (race, gender, name) values ('Vogon', 'male', 'Jeltz')");
        execute("insert into characters_guide (race, gender, name) values ('Vogon', 'male', 'Kwaltz')");
        refresh();

        execute("create table characters_life (race string, gender string, name string)");
        execute("insert into characters_life (race, gender, name) values ('Rabbit', 'male', 'Agrajag')");
        execute("insert into characters_life (race, gender, name) values ('Human', 'male', 'Ford Perfect')");
        execute("insert into characters_life (race, gender, name) values ('Human', 'female', 'Trillian')");
        refresh();

        client().admin().indices().prepareAliases().addAlias("characters_guide", "characters")
                .addAlias("characters_life", "characters").execute().actionGet();
        ensureGreen();

        execute("select count(*) from characters");
        assertEquals(7L, response.rows()[0][0]);

        execute("select count(*), race from characters group by race order by count(*) desc " +
                "limit 2");
        assertEquals(2, response.rowCount());
        assertEquals("Human", response.rows()[0][1]);
        assertEquals("Vogon", response.rows()[1][1]);
    }

    private String tableAliasSetup() throws Exception {
        String tableName = "mytable";
        String tableAlias = "mytablealias";
        execute(String.format("create table %s (id integer primary key, " +
                "content string)",
                tableName));
        refresh();
        client().admin().indices().prepareAliases().addAlias(tableName,
                tableAlias).execute().actionGet();
        refresh();
        Thread.sleep(20);
        return tableAlias;
    }

    @Test
    public void testCreateTableWithExistingTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();

        expectedException.expect(TableAlreadyExistsException.class);
        expectedException.expectMessage("The table 'mytablealias' already exists.");

        execute(String.format("create table %s (content string index off)", tableAlias));
    }

    @Test
    public void testDropTableWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Table alias not allowed in DROP TABLE statement.");
        execute(String.format("drop table %s", tableAlias));
    }

    @Test
    public void testCopyFromWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("aliases are read only");

        execute(String.format("copy %s from '/tmp/file.json'", tableAlias));

    }

    @Test
    public void testInsertWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("aliases are read only");

        execute(
                String.format("insert into %s (id, content) values (?, ?)", tableAlias),
                new Object[]{1, "bla"}
                );
    }

    @Test
    public void testUpdateWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("aliases are read only");

        execute(
                String.format("update %s set id=?, content=?", tableAlias),
                new Object[]{1, "bla"}
        );
    }

    @Test
    public void testDeleteWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("aliases are read only");

        execute(
                String.format("delete from %s where id=?", tableAlias),
                new Object[]{1}
        );
    }


    /* GLOBAL AGGREGATE */

    @Test
    public void testGlobalAggregateSimple() throws Exception {
        this.setup.groupBySetup();

        execute("select max(age) from characters");

        assertEquals(1, response.rowCount());
        assertEquals("max(age)", response.cols()[0]);
        assertEquals(112, response.rows()[0][0]);

        execute("select min(name) from characters");

        assertEquals(1, response.rowCount());
        assertEquals("min(name)", response.cols()[0]);
        assertEquals("Anjie", response.rows()[0][0]);

        execute("select avg(age) as median_age from characters");
        assertEquals(1, response.rowCount());
        assertEquals("median_age", response.cols()[0]);
        assertEquals(55.25d, response.rows()[0][0]);

        execute("select sum(age) as sum_age from characters");
        assertEquals(1, response.rowCount());
        assertEquals("sum_age", response.cols()[0]);
        assertEquals(221.0d, response.rows()[0][0]);
    }

    @Test
    public void testGlobalAggregateWithoutNulls() throws Exception {
        this.setup.groupBySetup();

        execute("select sum(age) from characters");

        SQLResponse first_response = this.response;

        execute("select sum(age) from characters where age is not null");

        assertEquals(
                first_response.rowCount(),
                this.response.rowCount()
        );
        assertEquals(
                first_response.rows()[0][0],
                this.response.rows()[0][0]
        );
    }

    @Test
    public void testGlobalAggregateNullRowWithoutMatchingRows() throws Exception {
        this.setup.groupBySetup();
        execute("select sum(age), avg(age) from characters where characters.age > 112");
        assertEquals(1, response.rowCount());
        assertNull(response.rows()[0][0]);
        assertNull(response.rows()[0][1]);

        execute("select sum(age) from characters limit 0");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGlobalAggregateMany() throws Exception {
        this.setup.groupBySetup();
        execute("select sum(age), min(age), max(age), avg(age) from characters");
        assertEquals(1, response.rowCount());
        assertEquals(221.0d, response.rows()[0][0]);
        assertEquals(32, response.rows()[0][1]);
        assertEquals(112, response.rows()[0][2]);
        assertEquals(55.25d, response.rows()[0][3]);
    }


    @Test
    public void testSelectGlobalExpressionGroupBy() throws Exception {
        this.setup.groupBySetup();
        execute("select count(distinct race), sys.cluster.name from characters group by sys.cluster.name");
        assertEquals(1, response.rowCount());
        for (int i=0; i<response.rowCount();i++) {
            assertEquals(3L, response.rows()[i][0]);
            assertEquals(cluster().clusterName(), response.rows()[i][1]);
        }
    }

    @Test
    public void testSelectGlobalExpressionGroupByWith2GroupByKeys() throws Exception {
        this.setup.groupBySetup();
        execute("select count(name), sys.cluster.name from characters group by race, sys.cluster.name order by count(name) desc");
        assertEquals(3, response.rowCount());
        assertEquals(4L, response.rows()[0][0]);
        assertEquals(cluster().clusterName(), response.rows()[0][1]);
        assertEquals(response.rows()[0][1], response.rows()[1][1]);
        assertEquals(response.rows()[0][1], response.rows()[2][1]);
    }

    @Test
    public void testSelectGlobalExpressionGlobalAggregate() throws Exception {
        this.setup.groupBySetup();
        execute("select count(distinct race), sys.cluster.name from characters");
        assertEquals(1, response.rowCount());
        assertArrayEquals(new String[]{"count(DISTINCT race)", "sys.cluster.name"}, response.cols());
        assertEquals(3L, response.rows()[0][0]);
        assertEquals(cluster().clusterName(), response.rows()[0][1]);
    }

    @Test
    public void testSelectNonExistentGlobalExpression() throws Exception {
        this.setup.groupBySetup();
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("suess.cluster");
        execute("select count(race), suess.cluster.name from characters");
    }

    @Test
    public void testSelectOrderByGlobalExpression() throws Exception {
        this.setup.groupBySetup();
        execute("select count(*), sys.cluster.name from characters group by sys.cluster.name order by sys.cluster.name");
        assertEquals(1, response.rowCount());
        assertEquals(7L, response.rows()[0][0]);

        assertEquals(cluster().clusterName(), response.rows()[0][1]);
    }

    @Test
    public void testSelectAggregateOnGlobalExpression() throws Exception {
        this.setup.groupBySetup();
        execute("select count(sys.cluster.name) from characters");
        assertEquals(1, response.rowCount());
        assertEquals(7L, response.rows()[0][0]);

        execute("select count(distinct sys.cluster.name) from characters");
        assertEquals(1, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testSelectGlobalExpressionWithAlias() throws Exception {
        this.setup.groupBySetup();
        execute("select sys.cluster.name as cluster_name, race from characters " +
                "group by sys.cluster.name, race " +
                "order by cluster_name, race");
        assertEquals(3L, response.rowCount());
        assertEquals(cluster().clusterName(), response.rows()[0][0]);
        assertEquals(cluster().clusterName(), response.rows()[1][0]);
        assertEquals(cluster().clusterName(), response.rows()[2][0]);

        assertEquals("Android", response.rows()[0][1]);
        assertEquals("Human", response.rows()[1][1]);
        assertEquals("Vogon", response.rows()[2][1]);
    }

    @Test
    public void testSelectCountDistinctZero() throws Exception {
        execute("create table test (col1 int) replicas 0");
        ensureGreen();

        execute("select count(distinct col1) from test");

        assertEquals(1, response.rowCount());
        assertEquals(0L, response.rows()[0][0]);

    }

    @Test
    public void testShardSelect() throws Exception {
        execute("create table test (col1 int) clustered into 3 shards replicas 0");
        ensureGreen();

        execute("select count(*) from sys.shards where table_name='test'");
        assertEquals(1, response.rowCount());
        assertEquals(3L, response.rows()[0][0]);
    }
}
