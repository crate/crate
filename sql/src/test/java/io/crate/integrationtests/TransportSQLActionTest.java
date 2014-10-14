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
import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.TimestampFormat;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.action.sql.SQLResponse;
import io.crate.executor.TaskResult;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TransportSQLActionTest extends SQLTransportIntegrationTest {


    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private Setup setup = new Setup(sqlExecutor);

    private String copyFilePath = getClass().getResource("/essetup/data/copy").getPath();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    private <T> List<T> getCol(Object[][] result, int idx) {
        ArrayList<T> res = new ArrayList<>(result.length);
        for (Object[] row : result) {
            res.add((T) row[idx]);
        }
        return res;
    }

    @Test
    public void testSelectKeepsOrder() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" as b, \"_version\" as a from test");
        assertArrayEquals(new String[]{"b", "a"}, response.cols());
        assertEquals(1, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
    }

    @Test
    public void testSelectCountStar() throws Exception {
        execute("create table test (\"type\" string) with (number_of_replicas=0)");
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
        execute("create table test (name string) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test (name) values (?)", new Object[]{"Arthur"});
        execute("insert into test (name) values (?)", new Object[]{"Trillian"});
        refresh();
        execute("select count(*) from test where name = 'Trillian'");
        assertEquals(1, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testGroupByOnSysNodes() throws Exception {
        execute("select count(*), name from sys.nodes group by name");
        assertThat(response.rowCount(), is(2L));

        execute("select count(*), hostname from sys.nodes group by hostname");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testSysCluster() throws Exception {
        execute("select id from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat(((String) response.rows()[0][0]).length(), is(36)); // looks like a uuid
    }

    public void testSysClusterMasterNode() throws Exception {
        execute("select id from sys.nodes");
        List<String> nodes = new ArrayList<>();
        for (Object[] nodeId : response.rows()) {
            nodes.add((String) nodeId[0]);
        }

        execute("select master_node from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        String node = (String) response.rows()[0][0];
        assertTrue(nodes.contains(node));
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
        ensureGreen();
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
        execute("create table test (first_name string, last_name string, age double) with (number_of_replicas = 0)");
        ensureGreen();
        client().prepareIndex("test", "default", "id1").setRefresh(true)
                .setSource("{\"first_name\":\"Youri\",\"last_name\":\"Zoon\", \"age\": 38}")
                .execute().actionGet();

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
        prepareCreate("test")
                .addMapping("default",
                        "firstName", "type=string",
                        "lastName", "type=string")
                .execute().actionGet();
        ensureGreen();
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
    public void testSelectDynamicObjectNewColumns() throws Exception {

        execute("create table test (message string, person object(dynamic)) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test (message, person) values ('I''m addicted to kite', {name='Youri', addresses=[{city='Dirksland', country='NL'}]})");
        refresh();

        // check that new columns in dynamic objects show up eventually
        long rowCount = 0L;
        int retries = 3;
        while (rowCount == 0L && retries > 0) {
            execute("select message, person['name'], person['addresses']['city'] from test " +
                    "where person['name'] = 'Youri'");
            rowCount = response.rowCount();
            retries--;
            Thread.sleep(10);
        }
        assertEquals(1L, rowCount);
        assertArrayEquals(new String[]{"message", "person['name']", "person['addresses']['city']"},
                response.cols());
        assertArrayEquals(new Object[]{"I'm addicted to kite", "Youri",
                        new ArrayList<String>() {{
                            add("Dirksland");
                        }}},
                response.rows()[0]
        );
    }

    @Test
    public void testFilterByEmptyString() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "name", "type=string,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();
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
        ensureGreen();
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
        ensureGreen();

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
        execute("create table test (\"firstName\" string, \"lastName\" string)");
        ensureGreen();
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
        createIndex("test");
        ensureGreen();
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
        ensureGreen();
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
        ensureGreen();
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
    public void testSelectObject() throws Exception {
        createIndex("test");
        ensureGreen();
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
        ensureGreen();
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test limit 1");
        assertEquals(1, response.rowCount());
    }


    @Test
    public void testSqlRequestWithLimitAndOffset() throws Exception {
        createIndex("test");
        ensureGreen();
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
        ensureGreen();
        client().prepareIndex("test", "default", "id1").setSource("{}").execute().actionGet();
        client().prepareIndex("test", "default", "id2").setSource("{}").execute().actionGet();
        refresh();
        execute("select \"_id\" from test where \"_id\"='id1'");
        assertEquals(1, response.rowCount());
        assertEquals("id1", response.rows()[0][0]);
    }

    @Test
    public void testSqlRequestWithNotEqual() throws Exception {
        execute("create table test (id string primary key) with (number_of_replicas = 0)");
        ensureGreen();
        execute("insert into test (id) values (?)", new Object[][] {
                new Object[] { "id1" },
                new Object[] { "id2" }
        });
        refresh();
        execute("select id from test where id != 'id1'");
        assertEquals(1, response.rowCount());
        assertEquals("id2", response.rows()[0][0]);
    }


    @Test
    public void testSqlRequestWithOneOrFilter() throws Exception {
        execute("create table test (id string) with (number_of_replicas = 0)");
        ensureGreen();
        execute("insert into test (id) values ('id1'), ('id2'), ('id3')");
        refresh();
        execute("select id from test where id='id1' or id='id3'");
        assertEquals(2, response.rowCount());
        assertThat(this.<String>getCol(response.rows(), 0), containsInAnyOrder("id1", "id3"));
    }

    @Test
    public void testSqlRequestWithOneMultipleOrFilter() throws Exception {
        execute("create table test (id string) with (number_of_replicas = 0)");
        ensureGreen();
        execute("insert into test (id) values ('id1'), ('id2'), ('id3'), ('id4')");
        refresh();
        execute("select id from test where id='id1' or id='id2' or id='id4'");
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
                        TimestampFormat.parseTimestampString("2013-10-01") + "}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2")
                .setSource("{\"date\": " +
                        TimestampFormat.parseTimestampString("2013-10-02") + "}")
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
                        TimestampFormat.parseTimestampString("2013-10-01") + "}")
                .execute().actionGet();
        client().prepareIndex("test", "default", "id2")
                .setSource("{\"date\":" +
                        TimestampFormat.parseTimestampString("2013-10-02") + "}")
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
        execute("create table test (\"firstName\" string, \"lastName\" string)");
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
        ensureGreen();

        execute("insert into test values(true, '2013-09-10T21:51:43', 1.79769313486231570e+308, 3.402, 2147483647, 9223372036854775807, 32767, 'Youri')");
        execute("insert into test values(?, ?, ?, ?, ?, ?, ?, ?)",
                new Object[]{true, "2013-09-10T21:51:43", 1.79769313486231570e+308, 3.402, 2147483647, 9223372036854775807L, 32767, "Youri"});
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test");

        assertEquals(2, response.rowCount());
        assertEquals(true, response.rows()[0][0]);
        assertEquals(1378849903000L, response.rows()[0][1]);
        assertEquals(1.79769313486231570e+308, response.rows()[0][2]);
        assertEquals(3.402f, ((Number) response.rows()[0][3]).floatValue(), 0.002f);
        assertEquals(2147483647, response.rows()[0][4]);
        assertEquals(9223372036854775807L, response.rows()[0][5]);
        assertEquals(32767, response.rows()[0][6]);
        assertEquals("Youri", response.rows()[0][7]);

        assertEquals(true, response.rows()[1][0]);
        assertEquals(1378849903000L, response.rows()[1][1]);
        assertEquals(1.79769313486231570e+308, response.rows()[1][2]);
        assertEquals(3.402f, ((Number) response.rows()[1][3]).floatValue(), 0.002f);
        assertEquals(2147483647, response.rows()[1][4]);
        assertEquals(9223372036854775807L, response.rows()[1][5]);
        assertEquals(32767, response.rows()[1][6]);
        assertEquals("Youri", response.rows()[1][7]);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testArraySupport() throws Exception {
        execute("create table t1 (id int primary key, strings array(string), integers array(integer)) with (number_of_replicas=0)");
        ensureGreen();

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
        assertThat(((List<String>) response.rows()[0][1]).get(0), is("foo"));
        assertThat(((List<String>) response.rows()[0][1]).get(1), is("bar"));
        assertThat(((List<Integer>) response.rows()[0][2]).get(0), is(1));
        assertThat(((List<Integer>) response.rows()[0][2]).get(1), is(2));
        assertThat(((List<Integer>) response.rows()[0][2]).get(2), is(3));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testArrayInsideObject() throws Exception {
        execute("create table t1 (id int primary key, details object as (names array(string))) with (number_of_replicas=0)");
        ensureGreen();

        Map<String, Object> details = new HashMap<>();
        details.put("names", new Object[]{"Arthur", "Trillian"});
        execute("insert into t1 (id, details) values (?, ?)", new Object[]{1, details});
        refresh();

        execute("select details['names'] from t1");
        assertThat(response.rowCount(), is(1L));
        assertThat(((List<String>) response.rows()[0][0]).get(0), is("Arthur"));
        assertThat(((List<String>) response.rows()[0][0]).get(1), is("Trillian"));
    }

    @Test(expected = SQLActionException.class)
    public void testArrayInsideObjectArray() throws Exception {
        execute("create table t1 (id int primary key, details array(object as (names array(string)))) with (number_of_replicas=0)");
        ensureGreen();

        Map<String, Object> detail1 = new HashMap<>();
        detail1.put("names", new Object[]{"Arthur", "Trillian"});

        Map<String, Object> detail2 = new HashMap<>();
        detail2.put("names", new Object[]{"Ford", "Slarti"});

        List<Map<String, Object>> details = Arrays.asList(detail1, detail2);

        execute("insert into t1 (id, details) values (?, ?)", new Object[]{1, details});
        refresh();

        execute("select details['names'] from t1");

    }

    @Test
    public void testFullPathRequirement() throws Exception {
        // verifies that the "fullPath" setting in the es mapping is no longer required
        execute("create table t1 (id int primary key, details object as (id int, more_details object as (id int))) with (number_of_replicas=0)");
        ensureGreen();

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
        ensureGreen();

        execute("insert into t1 (id, strings) values (?, ?)",
                new Object[]{
                        1,
                        new String[]{"foo", null, "bar"},
                }
        );
        refresh();

        execute("select id, strings, integers from t1");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat(((List<String>) response.rows()[0][1]).get(0), is("foo"));
        assertThat(((List<String>) response.rows()[0][1]).get(1), is((String) null));
        assertThat(((List<String>) response.rows()[0][1]).get(2), is("bar"));
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
        ensureGreen();

        Map<String, Object> obj1 = new MapBuilder<String, Object>().put("name", "foo").put("age", 1).map();
        Map<String, Object> obj2 = new MapBuilder<String, Object>().put("name", "bar").put("age", 2).map();

        Object[] args = new Object[]{1, new Object[]{obj1, obj2}};
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

        List<String> names = (List<String>) response.rows()[0][0];
        assertThat(names.get(0), is("foo"));
        assertThat(names.get(1), is("bar"));

        execute("select objects['name'] from t1 where ? = ANY (objects['name'])", new Object[]{"foo"});
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
                        ") with (number_of_replicas=0)"
        );
        ensureGreen();

        execute("insert into test values(?, ?, ?, ?, ?, ?, ?, ?)",
                new Object[]{
                        new Boolean[]{true, false},
                        new String[]{"2013-09-10T21:51:43", "2013-11-10T21:51:43"},
                        new Double[]{1.79769313486231570e+308, 1.69769313486231570e+308},
                        new Float[]{3.402f, 3.403f, null},
                        new Integer[]{2147483647, 234583},
                        new Long[]{9223372036854775807L, 4L},
                        new Short[]{32767, 2},
                        new String[]{"Youri", "Juri"}
                }
        );
        refresh();

        execute("select * from test");
        assertEquals(true, ((List<Boolean>) response.rows()[0][0]).get(0));
        assertEquals(false, ((List<Boolean>) response.rows()[0][0]).get(1));

        assertThat(((List<Long>) response.rows()[0][1]).get(0), is(1378849903000L));
        assertThat(((List<Long>) response.rows()[0][1]).get(1), is(1384120303000L));

        assertThat(((List<Double>) response.rows()[0][2]).get(0), is(1.79769313486231570e+308));
        assertThat(((List<Double>) response.rows()[0][2]).get(1), is(1.69769313486231570e+308));


        assertEquals(3.402f, ((Number) ((List) response.rows()[0][3]).get(0)).floatValue(), 0.002f);
        assertEquals(3.403f, ((Number) ((List) response.rows()[0][3]).get(1)).floatValue(), 0.002f);
        assertThat(((List<Float>) response.rows()[0][3]).get(2), nullValue());

        assertThat(((List<Integer>) response.rows()[0][4]).get(0), is(2147483647));
        assertThat(((List<Integer>) response.rows()[0][4]).get(1), is(234583));

        assertThat(((List<Long>) response.rows()[0][5]).get(0), is(9223372036854775807L));
        assertThat(((List<Integer>) response.rows()[0][5]).get(1), is(4));

        assertThat(((List<Integer>) response.rows()[0][6]).get(0), is(32767));
        assertThat(((List<Integer>) response.rows()[0][6]).get(1), is(2));

        assertThat(((List<String>) response.rows()[0][7]).get(0), is("Youri"));
        assertThat(((List<String>) response.rows()[0][7]).get(1), is("Juri"));
    }

    @Test
    public void testInsertMultipleRows() throws Exception {
        prepareCreate("test")
                .addMapping("default",
                        "age", "type=integer",
                        "name", "type=string,store=true,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();

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
        ensureGreen();

        Object[] args = new Object[]{32, "Youri"};
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
        execute("create table test (age integer, name string) with (number_of_replicas=0)");
        ensureGreen();

        Object[] args = new Object[]{32, "Youri", 42, "Ruben"};
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
        execute("create table test (message string, person object) with (number_of_replicas=0)");
        ensureGreen();

        Map<String, Object> person = new HashMap<>();
        person.put("first_name", "Youri");
        person.put("last_name", "Zoon");
        Object[] args = new Object[]{"I'm addicted to kite", person};

        execute("insert into test values(?, ?)", args);
        assertEquals(1, response.rowCount());
        refresh();

        execute("select * from test");

        assertEquals(1, response.rowCount());
        assertArrayEquals(args, response.rows()[0]);
    }

    @Test
    public void testInsertEmptyObjectArray() throws Exception {
        execute("create table test (" +
                "  id integer primary key," +
                "  details array(object)" +
                ")");
        ensureGreen();
        execute("insert into test (id, details) values (?, ?)", new Object[]{1, new Map[0]});
        refresh();
        execute("select id, details from test");
        assertEquals(1, response.rowCount());
        assertEquals(1, response.rows()[0][0]);
        assertThat(response.rows()[0][1], instanceOf(List.class));
        assertThat(((List) response.rows()[0][1]).size(), is(0));
    }

    @Test
    public void testUpdate() throws Exception {
        execute("create table test (message string)");
        ensureGreen();

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
        ensureGreen();
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
        ensureGreen();

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
        ensureGreen();

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
        assertEquals(3.3, response.rows()[0][0]);

    }

    @Test
    public void testUpdateNestedObjectWithoutDetailedSchema() throws Exception {
        execute("create table test (coolness object)");
        ensureGreen();

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

    @Test(expected = SQLActionException.class)
    public void testUpdateWithNestedObjectArrayIdxAccess() throws Exception {
        execute("create table test (coolness array(float)) with (number_of_replicas=0)");
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
        prepareCreate("test")
                .addMapping("default",
                        "coolness", "type=object,index=not_analyzed")
                .execute().actionGet();
        ensureGreen();

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

        Map<String, Object> map = new HashMap<String, Object>() {{
            put("x", "1");
            put("y", new HashMap<String, Object>() {{
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

        Object[] args = new Object[]{"1",
                "A towel is about the most massively useful thing an interstellar hitch hiker can have."};
        execute("insert into test (pk_col, message) values (?, ?)", args);
        refresh();

        GetResponse response = client().prepareGet("test", "default", "1").execute().actionGet();
        assertTrue(response.getSourceAsMap().containsKey("message"));
    }

    @Test
    public void testInsertWithPrimaryKeyMultiValues() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[]{
                "1", "All the doors in this spaceship have a cheerful and sunny disposition.",
                "2", "I always thought something was fundamentally wrong with the universe"
        };
        execute("insert into test (pk_col, message) values (?, ?), (?, ?)", args);
        refresh();

        GetResponse response = client().prepareGet("test", "default", "1").execute().actionGet();
        assertTrue(response.getSourceAsMap().containsKey("message"));
    }

    @Test(expected = SQLActionException.class)
    public void testInsertWithUniqueConstraintViolation() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[]{
                "1", "All the doors in this spaceship have a cheerful and sunny disposition.",
        };
        execute("insert into test (pk_col, message) values (?, ?)", args);

        args = new Object[]{
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

    @Test(expected = SQLActionException.class)
    public void testInsertWithPKMissingOnInsert() throws Exception {
        createTestIndexWithPkMapping();

        Object[] args = new Object[]{
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
                "\"_meta\":{\"primary_keys\":[\"col1\"]}," +
                "\"_all\":{\"enabled\":false}," +
                "\"properties\":{" +
                "\"col1\":{\"type\":\"integer\",\"doc_values\":true}," +
                "\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":true}" +
                "}}}";

        String expectedSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.version.created\":\"1030499\"" +
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
    public void testCreateTableWithRefreshIntervalDisableRefresh() throws Exception {
        execute("create table test (id int primary key, content string) with (refresh_interval=0)");
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        String expectedSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.refresh_interval\":\"0\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);

        execute("ALTER TABLE test SET (refresh_interval = 5000)");
        String expectedSetSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.refresh_interval\":\"5000\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";
        JSONAssert.assertEquals(expectedSetSettings, getIndexSettings("test"), false);

        execute("ALTER TABLE test RESET (refresh_interval)");
        String expectedResetSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.refresh_interval\":\"1000\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";
        JSONAssert.assertEquals(expectedResetSettings, getIndexSettings("test"), false);
    }

    @Test
    public void testSqlAlchemyGeneratedCountWithStar() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count('*')).filter(Test.name == 'foo').scalar()

        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        refresh();

        execute(
                "SELECT count(?) AS count_1 FROM test WHERE test.col2 = ?",
                new Object[]{"*", "foo"}
        );
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testSqlAlchemyGeneratedCountWithPrimaryKeyCol() throws Exception {
        // generated using sqlalchemy
        // session.query(Test.col1).filter(Test.col2 == 'foo').scalar()

        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        refresh();

        execute(
                "SELECT count(test.col1) AS count_1 FROM test WHERE test.col2 = ?",
                new Object[]{"foo"}
        );
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testSqlAlchemyGroupByWithCountStar() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count('*'), Test.col2).group_by(Test.col2).order_by(desc(func.count('*'))).all()

        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        execute("insert into test values (?, ?)", new Object[]{3, "foo"});
        refresh();

        execute(
                "SELECT count(?) AS count_1, test.col2 AS test_col2 FROM test " +
                        "GROUP BY test.col2 order by count_1 desc",
                new Object[]{"*"}
        );

        assertEquals(2L, response.rows()[0][0]);
    }

    @Test
    public void testSqlAlchemyGroupByWithPrimaryKeyCol() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count(Test.col1), Test.col2).group_by(Test.col2).order_by(desc(func.count(Test.col1))).all()


        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        execute("insert into test values (?, ?)", new Object[]{3, "foo"});
        refresh();

        execute(
                "SELECT count(test.col1) AS count_1, test.col2 AS test_col2 FROM test " +
                        "GROUP BY test.col2 order by count_1 desc"
        );

        assertEquals(2L, response.rows()[0][0]);
    }

    @Test(expected = SQLActionException.class)
    public void testCreateTableAlreadyExistsException() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();
        execute("create table test (col1 integer primary key, col2 string)");
    }

    @Test
    public void testCreateTableWithReplicasAndShards() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)" +
                "clustered by (col1) into 10 shards with (number_of_replicas=2)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        String expectedMapping = "{\"default\":{" +
                "\"_meta\":{" +
                "\"primary_keys\":[\"col1\"]," +
                "\"routing\":\"col1\"}," +
                "\"_all\":{\"enabled\":false}," +
                "\"properties\":{" +
                "\"col1\":{\"type\":\"integer\",\"doc_values\":true}," +
                "\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":true}" +
                "}}}";

        String expectedSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"2\"," +
                "\"index.number_of_shards\":\"10\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);
    }


    @Test
    public void testGroupByEmpty() throws Exception {
        execute("create table test (col1 string)");
        ensureGreen();

        execute("select count(*), col1 from test group by col1");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGroupByMultiValueField() throws Exception {
        this.setup.groupBySetup();
        // inserting multiple values not supported anymore

        client().prepareIndex("characters", Constants.DEFAULT_MAPPING_TYPE).setSource(new HashMap<String, Object>() {{
            put("race", new String[]{"Android"});
            put("gender", new String[]{"male", "robot"});
            put("name", "Marvin2");
        }}).execute().actionGet();
        client().prepareIndex("characters", Constants.DEFAULT_MAPPING_TYPE).setSource(new HashMap<String, Object>() {{
            put("race", new String[]{"Android"});
            put("gender", new String[]{"male", "robot"});
            put("name", "Marvin3");
        }}).execute().actionGet();
        execute("refresh table characters");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column \"gender\" has a value that is an array. Group by doesn't work on Arrays");

        execute("select gender from characters group by gender");
    }

    @Test
    public void testDropTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();

        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        execute("drop table test");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        assertFalse(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());
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
    public void testSelectMatch() throws Exception {
        execute("create table quotes (quote string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureGreen();
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
        execute("create table quotes (quote string) with (number_of_replicas = 0)");
        ensureGreen();
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
                new Object[]{"time", "time"}
        );
        assertEquals(2L, response.rowCount());
        assertEquals("Time is an illusion. Lunchtime doubly so", response.rows()[0][0]);
    }

    @Test
    public void testCreateTableWithInlineDefaultIndex() throws Exception {
        execute("create table quotes (quote string index using plain) with (number_of_replicas = 0)");
        ensureGreen();
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
        execute("create table quotes (quote string index using fulltext) with (number_of_replicas = 0)");
        ensureGreen();
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
        execute("create table quotes (id int, quote string index off) with (number_of_replicas = 0)");
        ensureGreen();
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
                "index quote_fulltext using fulltext(quote) with (analyzer='english')) with (number_of_replicas = 0)");
        ensureGreen();
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
                "with(analyzer='english')) with (number_of_replicas = 0)");
        ensureGreen();
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
        execute("create table quotes (quote string) with (number_of_replicas = 0)");
        ensureGreen();
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
                "index quote_ft using fulltext(quote)) with (number_of_replicas = 0)");
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        execute("insert into quotes values (?), (?)",
                new Object[]{"Would it save you a lot of time if I just gave up and went mad now?",
                        "Time is an illusion. Lunchtime doubly so. Take your time."}
        );
        refresh();

        execute("select quote, \"_score\" from quotes where match(quote_ft, 'time') " +
                "and \"_score\" >= 0.98");
        assertEquals(1L, response.rowCount());
        assertEquals(1, ((Float) response.rows()[0][1]).compareTo(0.98f));
    }

    @Test
    public void testSelectMatchAnd() throws Exception {
        execute("create table quotes (id int, quote string, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='english')) with (number_of_replicas = 0)");
        ensureGreen();
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
                ") clustered by (id) into 3 shards with (number_of_replicas=10)");

        execute("select table_name, number_of_shards, number_of_replicas, clustered_by from " +
                "information_schema.tables " +
                "where table_name='quotes'");
        assertEquals(1L, response.rowCount());
        assertEquals("quotes", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals("10", response.rows()[0][2]);
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

    @Test(expected = SQLActionException.class)
    public void testSelectSysColumnsFromInformationSchema() throws Exception {
        execute("select sys.nodes.id, table_name, number_of_replicas from information_schema.tables");
    }

    private void nonExistingColumnSetup() {
        execute("create table quotes (" +
                "id integer primary key, " +
                "quote string index off, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='snowball')" +
                ") clustered by (id) into 3 shards with (number_of_replicas = 0)");
        ensureGreen();
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

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("cannot MATCH on non existing column quotes.something");

        execute("select * from quotes where match(something, 'bla')");
    }

    @Test
    public void testCopyFromFile() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas = 0)");
        ensureGreen();

        String filePath = Joiner.on(File.separator).join(copyFilePath, "test_copy_from.json");
        execute("copy quotes from ?", new Object[]{filePath});
        // 2 nodes on same machine resulting in double affected rows
        assertEquals(6L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));

        execute("select quote from quotes where id = 1");
        assertThat((String) response.rows()[0][0], is("Don't pañic."));
    }

    @Test
    public void testCopyFromIntoPartitionedTableWithPARTITIONKeyword() throws Exception {
        execute("create table quotes (" +
                "id integer primary key," +
                "date timestamp primary key," +
                "quote string index using fulltext" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureGreen();
        String filePath = Joiner.on(File.separator).join(copyFilePath, "test_copy_from.json");
        execute("copy quotes partition (date=1400507539938) from ?", new Object[]{filePath});
        refresh();
        execute("select id, date, quote from quotes order by id asc");
        assertEquals(3L, response.rowCount());
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Long) response.rows()[0][1], is(1400507539938L));
        assertThat((String) response.rows()[0][2], is("Don't pañic."));

        execute("select count(*) from information_schema.table_partitions where table_name = 'quotes'");
        assertThat((Long) response.rows()[0][0], is(1L));

        execute("copy quotes partition (date=1800507539938) from ?", new Object[]{filePath});
        refresh();

        execute("select partition_ident from information_schema.table_partitions " +
                "where table_name = 'quotes' " +
                "order by partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("04732d1g60qj0dpl6csjicpo"));
        assertThat((String) response.rows()[1][0], is("04732e1g60qj0dpl6csjicpo"));
    }

    @Test
    public void testCopyFromIntoPartitionedTable() throws Exception {
        execute("create table quotes (id integer primary key, " +
                "quote string index using fulltext) partitioned by (id)");
        ensureGreen();

        String filePath = Joiner.on(File.separator).join(copyFilePath, "test_copy_from.json");
        execute("copy quotes from ?", new Object[]{filePath});
        // 2 nodes on same machine resulting in double affected rows
        assertEquals(6L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();
        ensureGreen();

        for (String id : ImmutableList.of("1", "2", "3")) {
            String partitionName = new PartitionName("quotes", ImmutableList.of(new BytesRef(id))).stringValue();
            assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                    .getState().metaData().indices().get(partitionName));
            assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                    .getState().metaData().indices().get(partitionName).aliases().get("quotes"));
        }

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));
    }

    @Test
    public void testCopyFromFileWithoutPK() throws Exception {
        execute("create table quotes (id int, " +
                "quote string index using fulltext) with (number_of_replicas=0)");
        ensureGreen();

        String filePath = Joiner.on(File.separator).join(copyFilePath, "test_copy_from.json");
        execute("copy quotes from ?", new Object[]{filePath});
        // 2 nodes on same machine resulting in double affected rows
        assertEquals(6L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select * from quotes");
        assertEquals(6L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));
    }

    @Test
    public void testCopyFromDirectory() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas=0)");
        ensureGreen();

        execute("copy quotes from ? with (shared=true)", new Object[]{copyFilePath + "/*"});
        assertEquals(3L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testCopyFromFilePattern() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas=0)");
        ensureGreen();

        String filePath = Joiner.on(File.separator).join(copyFilePath, "*.json");
        execute("copy quotes from ?", new Object[]{filePath});
        // 2 nodes on same machine resulting in double affected rows
        assertEquals(6L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testSelectTableAlias() throws Exception {
        execute("create table quotes_en (id int primary key, quote string) with (number_of_replicas=0)");
        execute("create table quotes_de (id int primary key, quote string) with (number_of_replicas=0)");
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

    @Test(expected = SQLActionException.class)
    public void testSelectTableAliasSchemaExceptionColumnDefinition() throws Exception {
        execute("create table quotes_en (id int primary key, quote string, author string)");
        execute("create table quotes_de (id int primary key, quote2 string)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test(expected = SQLActionException.class)
    public void testSelectTableAliasSchemaExceptionColumnDataType() throws Exception {
        execute("create table quotes_en (id int primary key, quote int) with (number_of_replicas=0)");
        execute("create table quotes_de (id int primary key, quote string) with (number_of_replicas=0)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test(expected = SQLActionException.class)
    public void testSelectTableAliasSchemaExceptionPrimaryKeyRoutingColumn() throws Exception {
        execute("create table quotes_en (id int primary key, quote string)");
        execute("create table quotes_de (id int, quote string)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test(expected = SQLActionException.class)
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
        ensureGreen();
        execute("insert into characters_guide (race, gender, name) values ('Human', 'male', 'Arthur Dent')");
        execute("insert into characters_guide (race, gender, name) values ('Android', 'male', 'Marving')");
        execute("insert into characters_guide (race, gender, name) values ('Vogon', 'male', 'Jeltz')");
        execute("insert into characters_guide (race, gender, name) values ('Vogon', 'male', 'Kwaltz')");
        refresh();

        execute("create table characters_life (race string, gender string, name string)");
        ensureGreen();
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
                tableName
        ));
        ensureGreen();
        client().admin().indices().prepareAliases().addAlias(tableName,
                tableAlias).execute().actionGet();
        refresh();
        Thread.sleep(20);
        return tableAlias;
    }

    @Test
    public void testCreateTableWithExistingTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The table 'mytablealias' already exists.");

        execute(String.format("create table %s (content string index off)", tableAlias));
    }

    @Test
    public void testDropTableWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias not allowed in DROP TABLE statement.");
        execute(String.format("drop table %s", tableAlias));
    }

    @Test
    public void testCopyFromWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("aliases are read only");

        execute(String.format("copy %s from '/tmp/file.json'", tableAlias));

    }

    @Test
    public void testInsertWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("aliases are read only");

        execute(
                String.format("insert into %s (id, content) values (?, ?)", tableAlias),
                new Object[]{1, "bla"}
        );
    }

    @Test
    public void testUpdateWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("aliases are read only");

        execute(
                String.format("update %s set id=?, content=?", tableAlias),
                new Object[]{1, "bla"}
        );
    }

    @Test
    public void testDeleteWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("aliases are read only");

        execute(
                String.format("delete from %s where id=?", tableAlias),
                new Object[]{1}
        );
    }

    @Test
    public void testSelectCountDistinctZero() throws Exception {
        execute("create table test (col1 int) with (number_of_replicas=0)");
        ensureGreen();

        execute("select count(distinct col1) from test");

        assertEquals(1, response.rowCount());
        assertEquals(0L, response.rows()[0][0]);
    }

    @Test
    public void testAlterTable() throws Exception {
        execute("create table test (col1 int) with (number_of_replicas='0-all')");
        ensureGreen();

        execute("select number_of_replicas from information_schema.tables where table_name = 'test'");
        assertEquals("0-all", response.rows()[0][0]);

        execute("alter table test set (number_of_replicas=0)");
        execute("select number_of_replicas from information_schema.tables where table_name = 'test'");
        assertEquals("0", response.rows()[0][0]);
    }

    @Test
    public void testShardSelect() throws Exception {
        execute("create table test (col1 int) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();

        execute("select count(*) from sys.shards where table_name='test'");
        assertEquals(1, response.rowCount());
        assertEquals(3L, response.rows()[0][0]);
    }

    @Test
    public void testRefresh() throws Exception {
        execute("create table test (id int primary key, name string)");
        ensureGreen();
        execute("insert into test (id, name) values (0, 'Trillian'), (1, 'Ford'), (2, 'Zaphod')");
        execute("select count(*) from test");
        assertThat((Long) response.rows()[0][0], lessThan(3L));

        execute("refresh table test");
        assertFalse(response.hasRowCount());
        assertThat(response.rows(), is(TaskResult.EMPTY_RESULT.rows()));

        execute("select count(*) from test");
        assertThat((Long) response.rows()[0][0], is(3L));
    }

    @Test
    public void testCreateAlterAndDropBlobTable() throws Exception {
        execute("create blob table screenshots with (number_of_replicas=0)");
        execute("alter blob table screenshots set (number_of_replicas=1)");
        execute("select number_of_replicas from information_schema.tables " +
                "where schema_name = 'blob' and table_name = 'screenshots'");
        assertEquals("1", response.rows()[0][0]);
        execute("drop blob table screenshots");
    }

    @Test(expected = SQLActionException.class)
    public void testInsertWithClusteredByNull() throws Exception {
        execute("create table quotes (id integer, quote string) clustered by(id) " +
                "with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote) values(?, ?)",
                new Object[]{null, "I'd far rather be happy than right any day."});
    }

    @Test(expected = SQLActionException.class)
    public void testInsertWithClusteredByWithoutValue() throws Exception {
        execute("create table quotes (id integer, quote string) clustered by(id) " +
                "with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (quote) values(?)",
                new Object[]{"I'd far rather be happy than right any day."});
    }

    @Test
    public void testInsertSelectWithClusteredBy() throws Exception {
        execute("create table quotes (id integer, quote string) clustered by(id) " +
                "with (number_of_replicas=0)");
        ensureGreen();
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
        ensureGreen();
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
        ensureGreen();
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
        ensureGreen();
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
        ensureGreen();
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
        ensureGreen();
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
    public void testUpdateByIdWithMultiplePrimaryKeyAndClusteredBy() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) clustered by(author) with (number_of_replicas=0)");
        ensureGreen();
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
        ensureGreen();
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
    public void testDeleteByIdWithMultiplePrimaryKey() throws Exception {
        execute("create table quotes (id integer primary key, author string primary key, " +
                "quote string) with (number_of_replicas=0)");
        ensureGreen();
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
        ensureGreen();
        execute("insert into quotes (id, author, quote) values (?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Ford", "I'd far rather be happy than right any day.",
                        1, "Douglas", "Don't panic"}
        );
        assertEquals(2L, response.rowCount());
        refresh();

        execute("delete from quotes where id=1");
        // no rowCount available for deleteByQuery requests
        assertEquals(-1L, response.rowCount());
        refresh();

        execute("select quote from quotes where id=1");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testSelectWhereBoolean() {
        execute("create table a (v boolean)");
        ensureGreen();

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
        ensureGreen();

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
    public void testCreatePartitionedTableAndQueryMeta() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();

        execute("select * from information_schema.tables where schema_name='doc' order by table_name");
        assertEquals(1L, response.rowCount());
        assertEquals("quotes", response.rows()[0][1]);

        execute("select * from information_schema.columns where table_name='quotes' order by ordinal_position");
        assertEquals(3L, response.rowCount());
        assertEquals("id", response.rows()[0][2]);
        assertEquals("quote", response.rows()[1][2]);
        assertEquals("timestamp", response.rows()[2][2]);

    }

    @Test
    public void testInsertPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        String templateName = PartitionName.templateName("parted");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        assertThat(templatesResponse.getIndexTemplates().get(0).template(),
                is(templateName + "*"));
        assertThat(templatesResponse.getIndexTemplates().get(0).name(),
                is(templateName));
        assertTrue(templatesResponse.getIndexTemplates().get(0).aliases().containsKey("parted"));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{1, "Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();

        assertTrue(clusterService().state().metaData().aliases().containsKey("parted"));

        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef(String.valueOf(13959981214861L)))
        ).stringValue();
        MetaData metaData = client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData();
        assertNotNull(metaData.indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );

        execute("select id, name, date from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((String) response.rows()[0][1], is("Ford"));
        assertThat((Long) response.rows()[0][2], is(13959981214861L));
    }

    @Test
    public void testBulkInsertPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Ford", 13959981214861L,
                        2, "Trillian", 0L,
                        3, "Zaphod", null
                });
        assertThat(response.rowCount(), is(3L));
        ensureGreen();
        refresh();

        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef(String.valueOf(13959981214861L)))
        ).stringValue();
        assertTrue(cluster().clusterService().state().metaData().hasIndex(partitionName));
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );

        partitionName = new PartitionName("parted", Arrays.asList(new BytesRef(String.valueOf(0L)))).stringValue();
        assertTrue(cluster().clusterService().state().metaData().hasIndex(partitionName));
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );

        List<BytesRef> nullList = new ArrayList<>();
        nullList.add(null);
        partitionName = new PartitionName("parted", nullList).stringValue();
        assertTrue(cluster().clusterService().state().metaData().hasIndex(partitionName));
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );
    }

    @Test
    public void testBulkOperations() throws Exception {
        execute("create table test (id integer primary key, name string) with (number_of_replicas = 0)");
        ensureGreen();
        SQLBulkResponse bulkResp = execute("insert into test (id, name) values (?, ?), (?, ?)",
                new Object[][] {
                        {1, "Earth", 2, "Saturn"},    // bulk row 1
                        {3, "Moon", 4, "Mars"}        // bulk row 2
                });
        assertThat(bulkResp.results().length, is(4));
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(1L));
        }
        refresh();

        bulkResp = execute("insert into test (id, name) values (?, ?), (?, ?)",
            new Object[][] {
                {1, "Earth", 2, "Saturn"},    // bulk row 1
                {3, "Moon", 4, "Mars"}        // bulk row 2
            });
        assertThat(bulkResp.results().length, is(4));
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(-2L));
        }

        execute("select name from test order by id asc");
        assertEquals("Earth\nSaturn\nMoon\nMars\n", TestingHelpers.printedTable(response.rows()));


        bulkResp = execute("update test set name = 'bulk_update' where id = ?", new Object[][]{
                new Object[]{2},
                new Object[]{3},
                new Object[]{4},
        });
        assertThat(bulkResp.results().length, is(3));
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(1L));
        }
        refresh();

        execute("select count(*) from test where name = 'bulk_update'");
        assertThat((Long) response.rows()[0][0], is(3L));

        bulkResp = execute("delete from test where id = ?", new Object[][] {
                new Object[] { 1 },
                new Object[] { 3 }
        });
        assertThat(bulkResp.results().length, is(2));
        for (SQLBulkResponse.Result result : bulkResp.results()) {
            assertThat(result.rowCount(), is(1L));
        }
        refresh();

        execute("select count(*) from test");
        assertThat((Long) response.rows()[0][0], is(2L));

    }

    @Test
    public void testInsertPartitionedTableOnlyPartitionedColumns() throws Exception {
        execute("create table parted (name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureGreen();

        execute("insert into parted (name, date) values (?, ?)",
                new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();
        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef("Ford"), new BytesRef(String.valueOf(13959981214861L)))
        ).stringValue();
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );
        execute("select * from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), arrayContaining("date", "name"));
        assertThat((Long) response.rows()[0][0], is(13959981214861L));
        assertThat((String) response.rows()[0][1], is("Ford"));
    }

    @Test
    public void testInsertPartitionedTableOnlyPartitionedColumnsAlreadyExsists() throws Exception {
        execute("create table parted (name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureGreen();

        execute("insert into parted (name, date) values (?, ?)",
                new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();
        execute("insert into parted (name, date) values (?, ?)",
                new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();

        execute("select name, date from parted");
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("Ford"));
        assertThat((String) response.rows()[1][0], is("Ford"));

        assertThat((Long) response.rows()[0][1], is(13959981214861L));
        assertThat((Long) response.rows()[1][1], is(13959981214861L));
    }

    @Test(expected = SQLActionException.class)
    public void testInsertPartitionedTablePrimaryKeysDuplicate() throws Exception {
        execute("create table parted (" +
                "  id int, " +
                "  name string, " +
                "  date timestamp," +
                "  primary key (id, name)" +
                ") partitioned by (id, name)");
        ensureGreen();
        Long dateValue = System.currentTimeMillis();
        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{42, "Zaphod", dateValue});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();
        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{42, "Zaphod", 0L});
    }

    @Test
    public void testInsertPartitionedTableSomePartitionedColumns() throws Exception {
        // insert only some partitioned column values
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureGreen();

        execute("insert into parted (id, name) values (?, ?)",
                new Object[]{1, "Trillian"});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();
        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef("Trillian"), null)).stringValue();
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));

        execute("select id, name, date from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((String) response.rows()[0][1], is("Trillian"));
        assertNull(response.rows()[0][2]);
    }

    @Test
    public void testInsertPartitionedTableReversedPartitionedColumns() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureGreen();

        Long dateValue = System.currentTimeMillis();
        execute("insert into parted (id, date, name) values (?, ?, ?)",
                new Object[]{1, dateValue, "Trillian"});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();
        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef("Trillian"), new BytesRef(dateValue.toString()))).stringValue();
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
    }

    @Test
    public void testSelectFromPartitionedTableWhereClause() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("select id, quote from quotes where (timestamp = 1395961200000 or timestamp = 1395874800000) and id = 1");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testSelectFromPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();
        execute("select id, quote, timestamp as ts, timestamp from quotes where timestamp > 1395874800000");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(2));
        assertThat((String) response.rows()[0][1], is("Time is an illusion. Lunchtime doubly so"));
        assertThat((Long) response.rows()[0][2], is(1395961200000L));
    }

    @Test
    public void testSelectPrimaryKeyFromPartitionedTable() throws Exception {
        execute("create table stuff (" +
                "  id integer primary key, " +
                "  type byte primary key," +
                "  content string) " +
                "partitioned by(type) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
                new Object[]{1, 127, "Don't panic"});
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
                new Object[]{2, 126, "Time is an illusion. Lunchtime doubly so"});
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
                new Object[]{3, 126, "Now panic"});
        ensureGreen();
        refresh();

        execute("select id, type, content from stuff where id=2 and type=126");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(2));
        byte b = 126;
        assertThat((Byte) response.rows()[0][1], is(b));
        assertThat((String) response.rows()[0][2], is("Time is an illusion. Lunchtime doubly so"));

        // multiget
        execute("select id, type, content from stuff where id in (2, 3) and type=126 order by id");
        assertThat(response.rowCount(), is(2L));

        assertThat((Integer) response.rows()[0][0], is(2));
        assertThat((Byte) response.rows()[0][1], is(b));
        assertThat((String) response.rows()[0][2], is("Time is an illusion. Lunchtime doubly so"));

        assertThat((Integer) response.rows()[1][0], is(3));
        assertThat((Byte) response.rows()[1][1], is(b));
        assertThat((String) response.rows()[1][2], is("Now panic"));
    }

    @Test
    public void testUpdatePartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("update quotes set quote = ? where timestamp = ?",
                new Object[]{"I'd far rather be happy than right any day.", 1395874800000L});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select id, quote from quotes where timestamp = 1395874800000");
        assertEquals(1L, response.rowCount());
        assertEquals(1, response.rows()[0][0]);
        assertEquals("I'd far rather be happy than right any day.", response.rows()[0][1]);

        execute("update quotes set quote = ?",
                new Object[]{"Don't panic"});
        assertEquals(2L, response.rowCount());
        refresh();

        execute("select id, quote from quotes where quote = ?",
                new Object[]{"Don't panic"});
        assertEquals(2L, response.rowCount());
    }

    @Test
    public void testUpdatePartitionedUnknownPartition() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("update quotes set quote='now panic' where timestamp = ?", new Object[]{ 1395874800123L });
        refresh();

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testUpdatePartitionedUnknownColumn() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("update quotes set quote='now panic' where timestam = ?", new Object[]{ 1395874800123L });
        refresh();

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testUpdatePartitionedUnknownColumnKnownValue() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("update quotes set quote='now panic' where timestamp = ? and quote=?",
                new Object[]{ 1395874800123L, "Don't panic" });
        refresh();

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testUpdateUnknownColumnKnownValueAndConjunction() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Don't panic", 1395961200000L});
        ensureGreen();
        refresh();

        execute("update quotes set quote='now panic' where not timestamp = ? and quote=?",
                new Object[]{ 1395874800000L, "Don't panic" });
        refresh();
        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(1L));
    }


    @Test
    public void testDeleteFromPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{3, "I'd far rather be happy than right any day", 1396303200000L});
        ensureGreen();
        refresh();

        execute("delete from quotes where timestamp = 1395874800000 and id = 1");
        assertEquals(-1, response.rowCount());
        refresh();

        execute("select id, quote from quotes where timestamp = 1395874800000");
        assertEquals(0L, response.rowCount());

        execute("select id, quote from quotes");
        assertEquals(2L, response.rowCount());

        execute("delete from quotes");
        assertEquals(-1, response.rowCount());
        refresh();

        execute("select id, quote from quotes");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testDeleteFromPartitionedTableUnknownPartition() throws Exception {
        this.setup.partitionTableSetup();
        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='parted' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat((String)response.rows()[0][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1388534400000"))).ident()));
        assertThat((String)response.rows()[1][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1391212800000"))).ident()));

        execute("delete from parted where date = '2014-03-01'");
        refresh();
        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='parted' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(newResponse.rows(), is(response.rows()));
    }

    @Test
    public void testDeleteFromPartitionedTableWrongPartitionedColumn() throws Exception {
        this.setup.partitionTableSetup();

        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='parted' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat((String)response.rows()[0][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1388534400000"))).ident()));
        assertThat((String)response.rows()[1][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1391212800000"))).ident()));

        execute("delete from parted where dat = '2014-03-01'");
        refresh();
        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='parted' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(newResponse.rows(), is(response.rows()));
    }

    @Test
    public void testDeleteFromPartitionedTableDeleteByQuery() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{3, "I'd far rather be happy than right any day", 1396303200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{4, "Now panic", 1395874800000L});
        ensureGreen();
        refresh();

        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='quotes' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(response.rowCount(), is(3L));
        assertThat((String)response.rows()[0][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1395874800000"))).ident()));
        assertThat((String)response.rows()[1][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1395961200000"))).ident()));
        assertThat((String)response.rows()[2][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1396303200000"))).ident()));

        execute("delete from quotes where quote = 'Don''t panic'");
        refresh();

        execute("select * from quotes where quote = 'Don''t panic'");
        assertThat(this.response.rowCount(), is(0L));

        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='quotes' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(newResponse.rows(), is(response.rows()));
    }

    @Test
    public void testDeleteFromPartitionedTableDeleteByPartitionAndByQuery() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{3, "I'd far rather be happy than right any day", 1396303200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{4, "Now panic", 1395874800000L});
        ensureGreen();
        refresh();

        // does not match
        execute("delete from quotes where quote = 'Don''t panic' and timestamp=?", new Object[]{1396303200000L});
        refresh();
        execute("select * from quotes where timestamp=?", new Object[]{1396303200000L});
        assertThat(response.rowCount(), is(1L));

        // matches
        execute("delete from quotes where quote = 'I''d far rather be happy than right any day' and timestamp=?", new Object[]{1396303200000L});
        refresh();
        execute("select * from quotes where timestamp=?", new Object[]{1396303200000L});
        assertThat(response.rowCount(), is(0L));

        execute("delete from quotes where timestamp=? and x=5", new Object[]{1395874800000L});
        refresh();
        execute("select * from quotes where timestamp=?", new Object[]{1395874800000L});
        assertThat(response.rowCount(), is(2L));


    }

    @Test
    public void testDeleteFromPartitionedTableDeleteByPartitionAndQueryWithConjunction() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Don't panic", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{3, "Don't panic", 1396303200000L});
        ensureGreen();
        refresh();

        execute("delete from quotes where not timestamp=? and quote=?", new Object[]{1396303200000L, "Don't panic"});
        refresh();
        execute("select * from quotes");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testGlobalAggregatePartitionedColumns() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(0L));
        assertThat((Long) response.rows()[0][1], is(0L));
        assertNull(response.rows()[0][2]);
        assertNull(response.rows()[0][3]);
        assertNull(response.rows()[0][4]);
        assertNull(response.rows()[0][5]);

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{0, "Trillian", 100L});
        ensureGreen();
        refresh();

        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(1L));
        assertThat((Long) response.rows()[0][1], is(1L));
        assertThat((Long) response.rows()[0][2], is(100L));
        assertThat((Long) response.rows()[0][3], is(100L));
        assertThat((Long) response.rows()[0][4], is(100L));
        assertThat((Double) response.rows()[0][5], is(100.0));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{1, "Ford", 1001L});
        ensureGreen();
        refresh();

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{2, "Arthur", 1001L});
        ensureGreen();
        refresh();

        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2L));
        assertThat((Long) response.rows()[0][1], is(3L));
        assertThat((Long) response.rows()[0][2], is(100L));
        assertThat((Long) response.rows()[0][3], is(1001L));
        assertThat((Long) response.rows()[0][4], isOneOf(100L, 1001L));
        assertThat((Double) response.rows()[0][5], is(700.6666666666666));
    }

    @Test
    public void testGroupByPartitionedColumns() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("select date, count(*) from parted group by date");
        assertThat(response.rowCount(), is(0L));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{0, "Trillian", 100L});
        ensureGreen();
        refresh();

        execute("select date, count(*) from parted group by date");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(100L));
        assertThat((Long) response.rows()[0][1], is(1L));

        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Arthur", null,
                        2, "Ford", null
                });
        ensureGreen();
        refresh();

        execute("select date, count(*) from parted group by date order by count(*) desc");
        assertThat(response.rowCount(), is(2L));
        assertNull(response.rows()[0][0]);
        assertThat((Long) response.rows()[0][1], is(2L));
        assertThat((Long) response.rows()[1][0], is(100L));
        assertThat((Long) response.rows()[1][1], is(1L));
    }

    @Test
    public void testGroupByPartitionedColumnWhereClause() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("select date, count(*) from parted where date > 0 group by date");
        assertThat(response.rowCount(), is(0L));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{0, "Trillian", 100L});
        ensureGreen();
        refresh();

        execute("select date, count(*) from parted where date > 0 group by date");
        assertThat(response.rowCount(), is(1L));

        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Arthur", 0L,
                        2, "Ford", 2437646253L
                }
        );
        ensureGreen();
        refresh();

        execute("select date, count(*) from parted where date > 100 group by date");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2437646253L));
        assertThat((Long) response.rows()[0][1], is(1L));
    }

    @Test
    public void testGlobalAggregateWhereClause() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted where date > 0");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(0L));
        assertThat((Long) response.rows()[0][1], is(0L));
        assertNull(response.rows()[0][2]);
        assertNull(response.rows()[0][3]);
        assertNull(response.rows()[0][4]);
        assertNull(response.rows()[0][5]);

        execute("insert into parted (id, name, date) values " +
                        "(?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Arthur", 0L,
                        2, "Ford", 2437646253L,
                        3, "Zaphod", 1L,
                        4, "Trillian", 0L
                });
        assertThat(response.rowCount(), is(4L));
        ensureGreen();
        refresh();

        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted where date > 0");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2L));
        assertThat((Long) response.rows()[0][1], is(2L));
        assertThat((Long) response.rows()[0][2], is(1L));
        assertThat((Long) response.rows()[0][3], is(2437646253L));
        assertThat((Long) response.rows()[0][4], isOneOf(1L, 2437646253L));
        assertThat((Double) response.rows()[0][5], is(1.218823127E9));
    }

    @Test
    public void testDropPartitionedTable() throws Exception {
        execute("create table quotes (" +
                "  id integer, " +
                "  quote string, " +
                "  date timestamp" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, date) values(?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("drop table quotes");
        assertEquals(1L, response.rowCount());

        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices()
                .prepareGetTemplates(PartitionName.templateName("quotes")).execute().get();
        assertThat(getIndexTemplatesResponse.getIndexTemplates().size(), is(0));

        IndicesStatusResponse statusResponse = client().admin().indices()
                .prepareStatus().execute().get();
        assertThat(statusResponse.getIndices().size(), is(0));

        AliasesExistResponse aliasesExistResponse = client().admin().indices()
                .prepareAliasesExist("quotes").execute().get();
        assertFalse(aliasesExistResponse.exists());
    }

    @Test
    public void testPartitionedTableSelectById() throws Exception {
        execute("create table quotes (id integer, quote string, num double, primary key (id, num)) partitioned by (num)");
        ensureGreen();
        execute("insert into quotes (id, quote, num) values (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Don't panic", 4.0d,
                        2, "Time is an illusion. Lunchtime doubly so", -4.0d
                });
        ensureGreen();
        refresh();
        execute("select * from quotes where id = 1 and num = 4");
        assertThat(response.rowCount(), is(1L));
        assertThat(Joiner.on(", ").join(response.cols()), is("id, num, quote"));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Double) response.rows()[0][1], is(4.0d));
        assertThat((String) response.rows()[0][2], is("Don't panic"));
    }

    @Test
    public void testInsertDynamicToPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp," +
                "author object(dynamic) as (name string)) " +
                "partitioned by(date) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, date, author) values(?, ?, ?, ?), (?, ?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        new HashMap<String, Object>() {{
                            put("name", "Douglas");
                        }},
                        2, "Time is an illusion. Lunchtime doubly so", 1395961200000L,
                        new HashMap<String, Object>() {{
                            put("name", "Ford");
                        }}});
        ensureGreen();
        refresh();

        execute("select * from information_schema.columns where table_name = 'quotes'");
        assertEquals(5L, response.rowCount());

        execute("insert into quotes (id, quote, date, author) values(?, ?, ?, ?)",
                new Object[]{3, "I'd far rather be happy than right any day", 1395874800000L,
                        new HashMap<String, Object>() {{
                            put("name", "Douglas");
                            put("surname", "Adams");
                        }}});
        ensureGreen();
        refresh();

        execute("select * from information_schema.columns where table_name = 'quotes'");
        assertEquals(6L, response.rowCount());

        execute("select author['surname'] from quotes order by id");
        assertEquals(3L, response.rowCount());
        assertNull(response.rows()[0][0]);
        assertNull(response.rows()[1][0]);
        assertEquals("Adams", response.rows()[2][0]);
    }

    @Test
    public void testPartitionedTableAllConstraintsRoundTrip() throws Exception {
        execute("create table quotes (id integer primary key, quote string, " +
                "date timestamp primary key, user_id string primary key) " +
                "partitioned by(date, user_id) clustered by (id) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L, "Arthur"});
        assertEquals(1L, response.rowCount());
        execute("insert into quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L, "Ford"});
        assertEquals(1L, response.rowCount());
        ensureGreen();
        refresh();

        execute("select id, quote from quotes where user_id = 'Arthur'");
        assertEquals(1L, response.rowCount());

        execute("update quotes set quote = ? where user_id = ?",
                new Object[]{"I'd far rather be happy than right any day", "Arthur"});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("delete from quotes where user_id = 'Arthur' and id = 1 and date = 1395874800000");
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(1L, response.rowCount());

        execute("delete from quotes"); // this will delete all partitions
        execute("delete from quotes"); // this should still work even though only the template exists

        execute("drop table quotes");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testPartitionedTableSchemaUpdateSameColumnNumber() throws Exception {
        execute("create table foo (" +
                "   id int primary key," +
                "   date timestamp primary key" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into foo (id, date, foo) values (1, '2014-01-01', 'foo')");
        execute("insert into foo (id, date, bar) values (2, '2014-02-01', 'bar')");
        refresh();
        ensureGreen();

        // schema updates are async and cannot reliably be forced
        int retry = 0;
        while (retry < 100) {
            execute("select * from foo");
            if (response.cols().length == 4) { // at some point both foo and bar columns must be present
                break;
            }
            Thread.sleep(100);
            retry++;
        }
        assertTrue(retry < 100);
    }

    @Test
    public void testPartitionedTableNestedAllConstraintsRoundTrip() throws Exception {
        execute("create table quotes (" +
                "id integer, " +
                "quote string, " +
                "created object as(" +
                "  date timestamp, " +
                "  user_id string)" +
                ") partitioned by(created['date']) clustered by (id) with (number_of_replicas=0)");
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        execute("insert into quotes (id, quote, created) values(?, ?, ?)",
                new Object[]{1, "Don't panic", new MapBuilder<String, Object>().put("date", 1395874800000L).put("user_id", "Arthur").map()});
        assertEquals(1L, response.rowCount());
        execute("insert into quotes (id, quote, created) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", new MapBuilder<String, Object>().put("date", 1395961200000L).put("user_id", "Ford").map()});
        assertEquals(1L, response.rowCount());
        ensureGreen();
        refresh();

        execute("select id, quote, created['date'] from quotes where created['user_id'] = 'Arthur'");
        assertEquals(1L, response.rowCount());
        assertThat((Long) response.rows()[0][2], is(1395874800000L));

        execute("update quotes set quote = ? where created['date'] = ?",
                new Object[]{"I'd far rather be happy than right any day", 1395874800000L});
        assertEquals(1L, response.rowCount());

        execute("refresh table quotes");

        execute("select count(*) from quotes where quote=?", new Object[]{"I'd far rather be happy than right any day"});
        assertThat((Long) response.rows()[0][0], is(1L));

        execute("delete from quotes where created['user_id'] = 'Arthur' and id = 1 and created['date'] = 1395874800000");
        assertEquals(-1L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(1L, response.rowCount());

        execute("drop table quotes");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testAlterPartitionTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='0-all')");
        ensureGreen();
        assertThat(response.rowCount(), is(1L));

        String templateName = PartitionName.templateName("quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(3));

        execute("alter table quotes set (number_of_replicas=0)");
        ensureGreen();

        templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(0));
        assertThat(templateSettings.getAsBoolean(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, true), is(false));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(3));

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureGreen();
        refresh();

        assertTrue(clusterService().state().metaData().aliases().containsKey("quotes"));

        execute("select number_of_replicas, number_of_shards from information_schema.tables where table_name = 'quotes'");
        assertEquals("0", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);

        execute("alter table quotes set (number_of_replicas='1-all')");
        ensureGreen();

        execute("select number_of_replicas from information_schema.tables where table_name = 'quotes'");
        assertEquals("1-all", response.rows()[0][0]);

        templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1), is(0));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(3));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("1-all"));

        List<String> partitions = ImmutableList.of(
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395961200000"))).stringValue()
        );
        Thread.sleep(1000);
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(
                partitions.get(0), partitions.get(1)
        ).execute().get();

        for (String index : partitions) {
            assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("1"));
            assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("1-all"));
        }
    }

    @Test
    public void testAlterTableResetEmptyPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='1-all')");
        ensureGreen();
        assertThat(response.rowCount(), is(1L));

        String templateName = PartitionName.templateName("quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("1-all"));

        execute("alter table quotes reset (number_of_replicas)");
        ensureGreen();

        templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));

    }

    @Test
    public void testAlterTableResetPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='1-all')");
        ensureGreen();
        assertThat(response.rowCount(), is(1L));

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureGreen();
        refresh();

        execute("alter table quotes reset (number_of_replicas)");
        ensureGreen();

        String templateName = PartitionName.templateName("quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));

        List<String> partitions = ImmutableList.of(
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395961200000"))).stringValue()
        );
        Thread.sleep(1000);
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(
                partitions.get(0), partitions.get(1)
        ).execute().get();

        for (String index : partitions) {
            assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("1"));
            assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));
        }

    }

    @Test
    public void testAlterPartitionedTablePartition() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();
        assertThat(response.rowCount(), is(1L));

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureGreen();
        refresh();

        execute("alter table quotes partition (date=1395874800000) set (number_of_replicas=1)");
        ensureGreen();
        List<String> partitions = ImmutableList.of(
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395961200000"))).stringValue()
        );

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(
                partitions.get(0), partitions.get(1)
        ).execute().get();
        assertThat(settingsResponse.getSetting(partitions.get(0), IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("1"));
        assertThat(settingsResponse.getSetting(partitions.get(1), IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("0"));

        String templateName = PartitionName.templateName("quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(0));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));

    }

    @Test
    public void testSelectFormatFunction() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
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
    public void testRefreshPartitionedTableAllPartitions() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) with (refresh_interval=0)");
        ensureGreen();

        execute("refresh table parted");
        assertThat(response.rowCount(), is(-1L));

        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01'), " +
                "(2, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount(), is(2L));
        ensureGreen();

        // cannot tell what rows are visible
        // could be none, could be all
        execute("select count(*) from parted");
        // cannot exactly tell which rows are visible
        assertThat((Long) response.rows()[0][0], lessThanOrEqualTo(2L));

        execute("refresh table parted");
        assertThat(response.rowCount(), is(-1L));

        // assert that all is available after refresh
        execute("select count(*) from parted");
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test(expected = SQLActionException.class)
    public void testRefreshEmptyPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) with (refresh_interval=0)");
        ensureGreen();

        execute("refresh table parted partition '0400===='");
    }

    @Test
    public void testRefreshPartitionedTableSinglePartitions() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) " +
                "with (number_of_replicas=0, refresh_interval=-1)");
        ensureGreen();
        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01')," +
                "(2, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount(), is(2L));

        execute("refresh table parted");
        assertThat(response.rowCount(), is(-1L));

        // assert that after refresh all columns are available
        execute("select * from parted");
        assertThat(response.rowCount(), is(2L));

        execute("insert into parted (id, name, date) values " +
                "(3, 'Zaphod', '1970-01-01')," +
                "(4, 'Marvin', '1970-01-07')");
        assertThat(response.rowCount(), is(2L));

        // cannot exactly tell which rows are visible
        execute("select * from parted");
        // cannot exactly tell how much rows are visible at this point
        assertThat(response.rowCount(), lessThanOrEqualTo(4L));

        execute("refresh table parted PARTITION (date='1970-01-01')");
        assertThat(response.rowCount(), is(-1L));

        // assert all partition rows are available after refresh
        execute("select * from parted where date='1970-01-01'");
        assertThat(response.rowCount(), is(2L));

        execute("refresh table parted PARTITION (date='1970-01-07')");
        assertThat(response.rowCount(), is(-1L));

        // assert all partition rows are available after refresh
        execute("select * from parted where date='1970-01-07'");
        assertThat(response.rowCount(), is(2L));
    }


    @Test
    public void testUpdateVersionHandling() throws Exception {
        execute("create table test (id int primary key, c int) with (number_of_replicas=0, refresh_interval=0)");
        ensureGreen();
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


        // without primary key optimization:
        execute("update test set c = 4 where _version = 2"); // this one works
        assertThat(response.rowCount(), is(1L));
        execute("update test set c = 5 where _version = 2"); // this doesn't
        assertThat(response.rowCount(), is(0L));


        execute("refresh table test");
        execute("select _version, c from test");
        assertThat((Long) response.rows()[0][0], is(3L));
        assertThat((Integer) response.rows()[0][1], is(4));
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
        assertThat(response.rowCount(), is(3L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(2));
        assertThat((Integer) response.rows()[2][0], is(4)); // null values matched because of negation

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
    public void testInsertAndSelectGeoType() throws Exception {
        execute("create table geo_point_table (id int primary key, p geo_point) with (number_of_replicas=0)");
        execute("insert into geo_point_table (id, p) values (?, ?)", new Object[]{1, new Double[]{47.22, 12.09}});
        execute("insert into geo_point_table (id, p) values (?, ?)", new Object[]{2, new Double[]{57.22, 7.12}});
        refresh();

        execute("select p from geo_point_table order by id desc");

        assertThat(response.rowCount(), is(2L));
        assertThat((List<Double>) response.rows()[0][0], is(Arrays.asList(57.22, 7.12)));
        assertThat((List<Double>) response.rows()[1][0], is(Arrays.asList(47.22, 12.09)));
    }

    @Test
    public void testCountPartitionedTable() throws Exception {
        execute("create table parted (" +
                "  id int, " +
                "  name string, " +
                "  date timestamp" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureGreen();

        execute("select count(*) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(0L));

        execute("insert into parted (id, name, date) values (1, 'Trillian', '1970-01-01'), (2, 'Ford', '2010-01-01')");
        ensureGreen();
        refresh();

        execute("select count(*) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testGroupByOnIpType() throws Exception {
        execute("create table t (i ip) with (number_of_replicas=0)");
        ensureGreen();
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
    public void testAlterTableAddColumn() throws Exception {
        execute("create table t (id int primary key) with (number_of_replicas=0)");
        execute("alter table t add column name string");

        execute("select data_type from information_schema.columns where " +
                "table_name = 't' and column_name = 'name'");
        assertThat((String) response.rows()[0][0], is("string"));

        execute("alter table t add column o object as (age int)");
        execute("select data_type from information_schema.columns where " +
                "table_name = 't' and column_name = 'o'");
        assertThat((String) response.rows()[0][0], is("object"));
    }

    @Test
    public void testAlterTableAddColumnOnPartitionedTable() throws Exception {
        execute("create table t (id int primary key, date timestamp primary key) " +
                "partitioned by (date) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");

        execute("insert into t (id, date) values (1, '2014-01-01')");
        execute("insert into t (id, date) values (10, '2015-01-01')");
        ensureGreen();
        refresh();

        execute("alter table t partition (date='2014-01-01') add name string");

        GetIndexTemplatesResponse templatesResponse = client().admin().indices().getTemplates(new GetIndexTemplatesRequest(".partitioned.t.")).actionGet();
        IndexTemplateMetaData metaData = templatesResponse.getIndexTemplates().get(0);
        String mappingSource = metaData.mappings().get(Constants.DEFAULT_MAPPING_TYPE).toString();
        Map mapping = (Map) XContentFactory.xContent(mappingSource)
                .createParser(mappingSource)
                .mapAndClose()
                .get(Constants.DEFAULT_MAPPING_TYPE);
        assertNull(((Map) mapping.get("properties")).get("name"));

        execute("insert into t (id, date, name) values (2, '2014-01-01', 100)"); // insert integer as name
        refresh();
        execute("select name from t where id = 2 and date = '2014-01-01'");

        assertThat((String) response.rows()[0][0], is("100")); // is returned as string

        execute("alter table t add name2 string"); // now template is also updated
        execute("select * from t");
        assertThat(Arrays.asList(response.cols()), Matchers.contains("date", "id", "name", "name2"));

        templatesResponse = client().admin().indices().getTemplates(new GetIndexTemplatesRequest(".partitioned.t.")).actionGet();
        metaData = templatesResponse.getIndexTemplates().get(0);
        mappingSource = metaData.mappings().get(Constants.DEFAULT_MAPPING_TYPE).toString();
        mapping = (Map) XContentFactory.xContent(mappingSource)
                .createParser(mappingSource)
                .mapAndClose().get(Constants.DEFAULT_MAPPING_TYPE);
        assertNotNull(((Map) mapping.get("properties")).get("name2"));
    }

    @Test
    public void testAlterTableAddColumnAsPrimaryKey() throws Exception {
        execute("create table t (id int primary key) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureGreen();
        execute("alter table t add column name string primary key");
        execute("select constraint_name from information_schema.table_constraints " +
                "where table_name = 't' and schema_name = 'doc' and constraint_type = 'PRIMARY_KEY'");

        assertThat(response.rowCount(), is(1L));
        assertThat((String[]) response.rows()[0][0], equalTo(new String[]{"name", "id"}));
    }

    @Test
    public void testAlterTableAddObjectColumnToExistingObject() throws Exception {
        execute("create table t (o object as (x string)) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureGreen();
        execute("alter table t add o['y'] int");
        try {
            execute("alter table t add o object as (z string)");
            assertTrue(false);
        } catch (SQLActionException e) {
            // column o exists already
        }
        execute("select * from information_schema.columns where " +
                "table_name = 't' and schema_name='doc'" +
                "order by column_name asc");
        assertThat(response.rowCount(), is(3L));

        List<String> fqColumnNames = new ArrayList<>();
        for (Object[] row : response.rows()) {
            fqColumnNames.add((String) row[2]);
        }
        assertThat(fqColumnNames, Matchers.contains("o", "o['x']", "o['y']"));
    }

    @Test
    public void testGeoTypeQueries() throws Exception {
        // setup
        execute("create table t (id int primary key, i int, p geo_point) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (id, i, p) values (1, 1, 'POINT (10 20)')");
        execute("insert into t (id, i, p) values (2, 1, 'POINT (11 21)')");
        refresh();

        // order by
        execute("select distance(p, 'POINT (11 21)') from t order by 1");
        assertThat(response.rowCount(), is(2L));

        Double result1 = (Double) response.rows()[0][0];
        Double result2 = (Double) response.rows()[1][0];

        assertThat(result1, is(0.0d));
        assertThat(result2, is(152462.70754934277));

        String stmtOrderBy = "SELECT id " +
                "FROM t " +
                "ORDER BY distance(p, 'POINT(30.0 30.0)')";
        execute(stmtOrderBy);
        assertThat(response.rowCount(), is(2L));
        String expectedOrderBy =
                "2\n" +
                        "1\n";
        assertEquals(expectedOrderBy, TestingHelpers.printedTable(response.rows()));

        // aggregation (max())
        String stmtAggregate = "SELECT i, max(distance(p, 'POINT(30.0 30.0)')) " +
                "FROM t " +
                "GROUP BY i";
        execute(stmtAggregate);
        assertThat(response.rowCount(), is(1L));
        String expectedAggregate = "1| 2297790.338709135\n";
        assertEquals(expectedAggregate, TestingHelpers.printedTable(response.rows()));

        // queries
        execute("select p from t where distance(p, 'POINT (11 21)') > 0.0");
        List<Double> row = (List<Double>) response.rows()[0][0];
        assertThat(row.get(0), is(10.0d));
        assertThat(row.get(1), is(20.0d));

        execute("select p from t where distance(p, 'POINT (11 21)') < 10.0");
        row = (List<Double>) response.rows()[0][0];
        assertThat(row.get(0), is(11.0d));
        assertThat(row.get(1), is(21.0d));

        execute("select p from t where distance(p, 'POINT (11 21)') < 10.0 or distance(p, 'POINT (11 21)') > 10.0");
        assertThat(response.rowCount(), is(2L));

        execute("select p from t where distance(p, 'POINT (10 20)') = 0");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testWithinQuery() throws Exception {
        execute("create table t (id int primary key, p geo_point) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (id, p) values (1, 'POINT (10 10)')");
        refresh();

        execute("select within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))') from t");
        assertThat((Boolean) response.rows()[0][0], is(true));

        execute("select * from t where within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))')");
        assertThat(response.rowCount(), is(1L));
        execute("select * from t where within(p, 'POLYGON (( 5 5, 30 5, 30 30, 5 35, 5 5 ))') = false");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testInsertFromQueryGlobalAggregate() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("create table aggs (" +
                " c long," +
                " s double" +
                ") with (number_of_replicas=0)");
        ensureGreen();

        execute("insert into aggs (c, s) (select count(*), sum(position) from locations)");
        assertThat(response.rowCount(), is(1L));

        execute("refresh table aggs");
        execute("select c, s from aggs");
        assertThat(response.rowCount(), is(1L));
        assertThat(((Number) response.rows()[0][0]).longValue(), is(13L));
        assertThat((Double) response.rows()[0][1], is(38.0));
    }

    @Test
    public void testInsertFromQueryCount() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("create table aggs (" +
                " c long" +
                ") with (number_of_replicas=0)");
        ensureGreen();

        execute("insert into aggs (c) (select count(*) from locations)");
        assertThat(response.rowCount(), is(1L));

        execute("refresh table aggs");
        execute("select c from aggs");
        assertThat(response.rowCount(), is(1L));
        assertThat(((Number) response.rows()[0][0]).longValue(), is(13L));
    }

    @Test
    public void testInsertFromQuery() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("select * from locations order by id");
        Object[][] rowsOriginal = response.rows();

        execute("create table locations2 (" +
                " id string primary key," +
                " name string," +
                " date timestamp," +
                " kind string," +
                " position long," +         // <<-- original type is integer, testing implicit cast
                " description string," +
                " race object," +
                " index name_description_ft using fulltext(name, description) with (analyzer='english')" +
                ") clustered by(id) into 2 shards with(number_of_replicas=0)");

        execute("insert into locations2 (select * from locations)");
        assertThat(response.rowCount(), is(13L));

        execute("refresh table locations2");
        execute("select * from locations2 order by id");
        assertThat(response.rowCount(), is(13L));
        assertThat(response.rows(), is(rowsOriginal));
    }

    @Test
    public void testInsertToPartitionFromQuery() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("select name from locations order by id");
        assertThat(response.rowCount(), is(13L));
        String firstName = (String) response.rows()[0][0];

        execute("create table locations_parted (" +
                " id string primary key," +
                " name string primary key," +
                " date timestamp" +
                ") clustered by(id) into 2 shards partitioned by(name) with(number_of_replicas=0)");

        execute("insert into locations_parted (id, name, date) (select id, name, date from locations)");
        assertThat(response.rowCount(), is(13L));

        execute("refresh table locations_parted");
        execute("select name from locations_parted order by id");
        assertThat(response.rowCount(), is(13L));
        assertThat((String) response.rows()[0][0], is(firstName));
    }

    @Test
    public void testInsertFromQueryWithGeoType() throws Exception {
        execute("create table t (p geo_point) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (p) values (?)", new Object[]{new Double[]{10.d, 10.d}});
        execute("refresh table t");
        execute("create table t2 (p geo_point) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t2 (p) (select p from t)");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testTwoSubStrOnSameColumn() throws Exception {
        this.setup.groupBySetup();
        execute("select name, substr(name, 4, 3), substr(name, 3, 4) from sys.nodes order by name");
        assertThat((String) response.rows()[0][0], is("node_0"));
        assertThat((String) response.rows()[0][1], is("e_0"));
        assertThat((String) response.rows()[0][2], is("de_0"));
        assertThat((String) response.rows()[1][0], is("node_1"));
        assertThat((String) response.rows()[1][1], is("e_1"));
        assertThat((String) response.rows()[1][2], is("de_1"));

    }

    @Test
    public void testMathFunctionNullArguments() throws Exception {
        testMathFunction("round(null)");
        testMathFunction("ceil(null)");
        testMathFunction("floor(null)");
        testMathFunction("abs(null)");
        testMathFunction("sqrt(null)");
        testMathFunction("log(null)");
        testMathFunction("log(null, 1)");
        testMathFunction("log(1, null)");
        testMathFunction("log(null, null)");
        testMathFunction("ln(null)");
    }

    public void testMathFunction(String function) {
        assertNull(execute("select " + function + " from sys.cluster").rows()[0][0]);
    }

    @Test
    public void testSelectWhereArithmeticScalar() throws Exception {
        execute("create table t (d double, i integer) clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (d) values (?), (?), (?)", new Object[]{1.3d, 1.6d, 2.2d});
        execute("refresh table t");

        execute("select * from t where round(d) < 2");
        assertThat(response.rowCount(), is(1L));

        execute("select * from t where ceil(d) < 3");
        assertThat(response.rowCount(), is(2L));

        execute("select floor(d) from t where floor(d) = 2");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2L));

        execute("insert into t (d, i) values (?, ?)", new Object[]{-0.2, 10});
        execute("refresh table t");

        execute("select abs(d) from t where abs(d) = 0.2");
        assertThat(response.rowCount(), is(1L));
        assertThat((Double) response.rows()[0][0], is(0.2));

        execute("select ln(i) from t where ln(i) = 2.302585092994046");
        assertThat(response.rowCount(), is(1L));
        assertThat((Double) response.rows()[0][0], is(2.302585092994046));

        execute("select log(i, 100) from t where log(i, 100) = 0.5");
        assertThat(response.rowCount(), is(1L));
        assertThat((Double) response.rows()[0][0], is(0.5));

        execute("select round(d), count(*) from t where abs(d) > 1 group by 1 order by 1");
        assertThat(response.rowCount(), is(2L));
        assertThat((Long) response.rows()[0][0], is(1L));
        assertThat((Long) response.rows()[0][1], is(1L));
        assertThat((Long) response.rows()[1][0], is(2L));
        assertThat((Long) response.rows()[1][1], is(2L));
    }

    @Test
    public void testSelectOrderByScalar() throws Exception {
        execute("create table t (d double, i integer, name string) clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (d, name) values (?, ?)", new Object[][] {
                new Object[] {1.3d, "Arthur" },
                new Object[] {1.6d, null },
                new Object[] {2.2d, "Marvin" }
        });
        execute("refresh table t");

        execute("select * from t order by round(d) * 2 + 3");
        assertThat(response.rowCount(), is(3L));
        assertThat((Double) response.rows()[0][0], is(1.3d));

        execute("select name from t order by substr(name, 1, 1) nulls first");
        assertThat(response.rowCount(), is(3L));
        assertThat(response.rows()[0][0], nullValue());
        assertThat((String) response.rows()[1][0], is("Arthur"));

        execute("select * from t order by ceil(d), d");
        assertThat(response.rowCount(), is(3L));
        assertThat((Double) response.rows()[0][0], is(1.3d));

        execute("select * from t order by floor(d), d");
        assertThat(response.rowCount(), is(3L));
        assertThat((Double) response.rows()[0][0], is(1.3d));

        execute("insert into t (d, i) values (?, ?), (?, ?)", new Object[]{-0.2, 10, 0.1, 5});
        execute("refresh table t");

        execute("select * from t order by abs(d)");
        assertThat(response.rowCount(), is(5L));
        assertThat((Double) response.rows()[0][0], is(0.1));

        execute("select i from t order by ln(i)");
        assertThat(response.rowCount(), is(5L));
        assertThat((Integer) response.rows()[0][0], is(5));

        execute("select i from t order by log(i, 100)");
        assertThat(response.rowCount(), is(5L));
        assertThat((Integer) response.rows()[0][0], is(5));
    }

    @Test
    public void testSelectWhereArithmeticScalarTwoReferences() throws Exception {
        execute("create table t (d double, i integer) clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (d, i) values (?, ?), (?, ?), (?, ?)", new Object[]{
                1.3d, 1,
                1.6d, 2,
                2.2d, 9,
                -3.4, 6});
        execute("refresh table t");

        execute("select i from t where round(d) = i order by i");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(2));
    }

    @Test
    public void testSelectWhereArithmeticScalarTwoReferenceArgs() throws Exception {
        execute("create table t (x long, base long) clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (x, base) values (?, ?), (?, ?), (?, ?)", new Object[]{
                144L, 12L, // 2
                65536L, 2L, // 16
                9L, 3L, // 2
                700L, 3L // 5.9630...
        });
        execute("refresh table t");

        execute("select x, base, log(x, base) from t where log(x, base) = 2.0 order by x");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(9));
        assertThat((Integer) response.rows()[0][1], is(3));
        assertThat((Double) response.rows()[0][2], is(2.0));
        assertThat((Integer) response.rows()[1][0], is(144));
        assertThat((Integer) response.rows()[1][1], is(12));
        assertThat((Double) response.rows()[1][2], is(2.0));
    }

    @Test
    public void testScalarInOrderByAndSelect() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (-1, 4, 90.5), (193384, 31234594433, 99.0)");
        execute("insert into t (i, l, d) values (1, 2, 99.0), (-1, 4, 99.0)");
        refresh();
        execute("select l, log(d,l) from t order by l, log(d,l) desc");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("2| 6.6293566200796095\n" +
                        "2| 6.499845887083206\n" +
                        "4| 3.3146783100398047\n" +
                        "4| 3.249922943541603\n" +
                        "31234594433| 0.19015764044502392\n"));
    }

    @Test
    public void testNonIndexedColumnInRegexScalar() throws Exception {
        execute("create table regex_noindex (i integer, s string INDEX OFF) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into regex_noindex (i, s) values (?, ?)", new Object[][]{
                new Object[]{1, "foo"},
                new Object[]{2, "bar"},
                new Object[]{3, "foobar"}
        });
        refresh();
        execute("select regexp_replace(s, 'foo', 'crate') from regex_noindex order by i");
        assertThat(response.rowCount(), is(3L));
        assertThat((String) response.rows()[0][0], is("crate"));
        assertThat((String) response.rows()[1][0], is("bar"));
        assertThat((String) response.rows()[2][0], is("cratebar"));

        execute("select regexp_matches(s, '^(bar).*') from regex_noindex order by i");
        assertThat(response.rows()[0][0], nullValue());
        assertThat(((String[]) response.rows()[1][0])[0], is("bar"));
        assertThat(response.rows()[2][0], nullValue());
    }

    @Test
    public void testFulltextColumnInRegexScalar() throws Exception {
        execute("create table regex_fulltext (i integer, s string INDEX USING FULLTEXT) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into regex_fulltext (i, s) values (?, ?)", new Object[][]{
                new Object[]{1, "foo is first"},
                new Object[]{2, "bar is second"},
                new Object[]{3, "foobar is great"},
                new Object[]{4, "crate is greater"}
        });
        refresh();

        execute("select regexp_replace(s, 'is', 'was') from regex_fulltext order by i");
        assertThat(response.rowCount(), is(4L));
        assertThat((String) response.rows()[0][0], is("foo was first"));
        assertThat((String) response.rows()[1][0], is("bar was second"));
        assertThat((String) response.rows()[2][0], is("foobar was great"));
        assertThat((String) response.rows()[3][0], is("crate was greater"));

        execute("select regexp_matches(s, '(\\w+) is (\\w+)') from regex_fulltext order by i");
        String[] match1 = (String[]) response.rows()[0][0];
        assertThat(match1[0], is("foo"));
        assertThat(match1[1], is("first"));

        String[] match2 = (String[]) response.rows()[1][0];
        assertThat(match2[0], is("bar"));
        assertThat(match2[1], is("second"));

        String[] match3 = (String[]) response.rows()[2][0];
        assertThat(match3[0], is("foobar"));
        assertThat(match3[1], is("great"));

        String[] match4 = (String[]) response.rows()[3][0];
        assertThat(match4[0], is("crate"));
        assertThat(match4[1], is("greater"));
    }

    @Test
    public void testSelectRandomTwoTimes() throws Exception {
        execute("select random(), random() from sys.cluster limit 1");
        assertThat(response.rows()[0][0], is(not(response.rows()[0][1])));

        this.setup.groupBySetup();
        execute("select random(), random() from characters limit 1");
        assertThat(response.rows()[0][0], is(not(response.rows()[0][1])));
    }

    @Test
    public void testSelectArithmeticOperatorInWhereClause() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (2, 5, 90.5), (193384, 31234594433, 99.0), (10, 21, 99.0), (-1, 4, 99.0)");
        refresh();

        execute("select i from t where i%2 = 0 order by i");
        assertThat(response.rowCount(), is(3L));

        assertThat(TestingHelpers.printedTable(response.rows()), is("2\n10\n193384\n"));

        execute("select l from t where i * -1 > 0");
        assertThat(response.rowCount(), is(1L));
        assertThat(TestingHelpers.printedTable(response.rows()), is("4\n"));

        execute("select l from t where i * 2 = l");
        assertThat(response.rowCount(), is(1L));
        assertThat(TestingHelpers.printedTable(response.rows()), is("2\n"));

        execute("select i%3, sum(l) from t where i+1 > 2 group by i%3 order by sum(l)");
        assertThat(response.rowCount(), is(2L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "2| 5.0\n" +
                        "1| 3.1234594454E10\n"));
    }

    @Test
    public void testSelectArithMetricOperatorInOrderBy() throws Exception {
        execute("create table t (i integer, l long, d double) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (2, 5, 90.5), (193384, 31234594433, 99.0), (10, 21, 99.0), (-1, 4, 99.0)");
        refresh();

        execute("select i, i%3 from t order by i%3, l");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "-1| -1\n" +
                        "1| 1\n" +
                        "10| 1\n" +
                        "193384| 1\n" +
                        "2| 2\n"));
    }

    @Test
    public void testSelectFailingSearchScript() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("log(x, b): given arguments would result in: 'NaN'");

        execute("create table t (i integer, l long, d double) clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (i, l, d) values (1, 2, 90.5)");
        refresh();

        execute("select log(d, l) from t where log(d, -1) >= 0");
    }

    @Test
    public void testSelectGroupByFailingSearchScript() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("log(x, b): given arguments would result in: 'NaN'");

        execute("create table t (i integer, l long, d double) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (i, l, d) values (1, 2, 90.5), (0, 4, 100)");
        execute("refresh table t");

        execute("select log(d, l) from t where log(d, -1) >= 0 group by log(d, l)");
    }

    @Test
    public void testNumericScriptOnAllTypes() throws Exception {
        // this test validates that no exception is thrown
        execute("create table t (b byte, s short, i integer, l long, f float, d double, t timestamp) with (number_of_replicas=0)");
        ensureGreen();
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

    public void testMatchNotOnSubColumn() throws Exception {
        execute("create table matchbox (" +
                "  s string index using fulltext with (analyzer='german')," +
                "  o object as (" +
                "    s string index using fulltext with (analyzer='german')," +
                "    m string index using fulltext with (analyzer='german')" +
                "  )" +
                ") with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into matchbox (s, o) values ('Arthur Dent', {s='Zaphod Beeblebroox', m='Ford Prefect'})");
        refresh();
        execute("select * from matchbox where match(s, 'Arthur')");
        assertThat(response.rowCount(), is(1L));

        execute("select * from matchbox where match(o['s'], 'Arthur')");
        assertThat(response.rowCount(), is(0L));

        execute("select * from matchbox where match(o['s'], 'Zaphod')");
        assertThat(response.rowCount(), is(1L));

        execute("select * from matchbox where match(s, 'Zaphod')");
        assertThat(response.rowCount(), is(0L));

        execute("select * from matchbox where match(o['m'], 'Ford')");
        assertThat(response.rowCount(), is(1L));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("cannot MATCH on non existing column matchbox.m");

        execute("select * from matchbox where match(m, 'Ford')");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testSelectWhereMultiColumnMatchDifferentTypesDifferentScore() throws Exception {
        this.setup.setUpLocations();
        refresh();
        execute("select name, description, kind, _score from locations " +
                "where match((kind, name_description_ft 0.5), 'Planet earth') using most_fields with (analyzer='english')");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("Alpha Centauri| 4.1 light-years northwest of earth| Star System| 0.049483635\n" +
                        "| This Planet doesn't really exist| Planet| 0.04724514\nAllosimanius Syneca| Allosimanius Syneca is a planet noted for ice, snow, mind-hurtling beauty and stunning cold.| Planet| 0.021473126\n" +
                        "Bartledan| An Earthlike planet on which Arthur Dent lived for a short time, Bartledan is inhabited by Bartledanians, a race that appears human but only physically.| Planet| 0.018788986\n" +
                        "Galactic Sector QQ7 Active J Gamma| Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, the planet Preliumtarn of the famed Sevorbeupstry and Quentulus Quazgar Mountains.| Galaxy| 0.017716927\n"));

        execute("select name, description, kind, _score from locations " +
                "where match((kind, name_description_ft 0.5), 'Planet earth') using cross_fields");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("Alpha Centauri| 4.1 light-years northwest of earth| Star System| 0.06658964\n" +
                        "| This Planet doesn't really exist| Planet| 0.06235056\n" +
                        "Allosimanius Syneca| Allosimanius Syneca is a planet noted for ice, snow, mind-hurtling beauty and stunning cold.| Planet| 0.02889618\n" +
                        "Bartledan| An Earthlike planet on which Arthur Dent lived for a short time, Bartledan is inhabited by Bartledanians, a race that appears human but only physically.| Planet| 0.025284158\n" +
                        "Galactic Sector QQ7 Active J Gamma| Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, the planet Preliumtarn of the famed Sevorbeupstry and Quentulus Quazgar Mountains.| Galaxy| 0.02338146\n"));
    }

    @Test
    public void testMatchTypes() throws Exception {
        this.setup.setUpLocations();
        refresh();

        execute("select name, _score from locations where match((kind 0.8, name_description_ft 0.6), 'planet earth') using best_fields");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("Alpha Centauri| 0.22184466\n| 0.21719791\nAllosimanius Syneca| 0.09626817\nBartledan| 0.08423465\nGalactic Sector QQ7 Active J Gamma| 0.08144922\n"));

        execute("select name, _score from locations where match((kind 0.6, name_description_ft 0.8), 'planet earth') using most_fields");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("Alpha Centauri| 0.12094267\n| 0.1035407\nAllosimanius Syneca| 0.05248235\nBartledan| 0.045922056\nGalactic Sector QQ7 Active J Gamma| 0.038827762\n"));

        execute("select name, _score from locations where match((kind 0.4, name_description_ft 1.0), 'planet earth') using cross_fields");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("Alpha Centauri| 0.14147125\n| 0.116184436\nAllosimanius Syneca| 0.061390605\nBartledan| 0.05371678\nGalactic Sector QQ7 Active J Gamma| 0.043569162\n"));

        execute("select name, _score from locations where match((kind 1.0, name_description_ft 0.4), 'Alpha Centauri') using phrase");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("Alpha Centauri| 0.94653714\n"));

        execute("select name, _score from locations where match(name_description_ft, 'Alpha Centauri') using phrase_prefix");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("Alpha Centauri| 1.5739591\n"));
    }

    @Test
    public void testMatchOptions() throws Exception {
        this.setup.setUpLocations();
        refresh();

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (analyzer='english') order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("NULL| 0.6417877\nAltair| 0.2895972\nNorth West Ripple| 0.25339755\nOuter Eastern Rim| 0.2246257\n"));

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (fuzziness=0.5) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("Outer Eastern Rim| 1.4109559\nNULL| 1.4109559\nNorth West Ripple| 1.2808706\nGalactic Sector QQ7 Active J Gamma| 1.2808706\nAltair| 0.3842612\nAlgol| 0.25617412\n"));

        execute("select name, _score from locations where match((kind, name_description_ft), 'gala') " +
                "using best_fields with (operator='or', minimum_should_match=2) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is(""));

        execute("select name, _score from locations where match((kind, name_description_ft), 'gala') " +
                "using phrase_prefix with (slop=1) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("NULL| 0.664469\nOuter Eastern Rim| 0.5898516\nGalactic Sector QQ7 Active J Gamma| 0.34636837\nAlgol| 0.32655922\nAltair| 0.32655922\nNorth West Ripple| 0.2857393\n"));

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using phrase with (tie_breaker=2.0) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("NULL| 0.40825456\nAltair| 0.18054473\nNorth West Ripple| 0.15797664\nOuter Eastern Rim| 0.1428891\n"));

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (zero_terms_query='all') order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
                is("NULL| 0.6417877\nAltair| 0.2895972\nNorth West Ripple| 0.25339755\nOuter Eastern Rim| 0.2246257\n"));
    }

    @Test
    public void testTestGroupByOnClusteredByColumn() throws Exception {
        execute("create table foo (id int, name string, country string) clustered by (country) with (number_of_replicas = 0)");
        ensureGreen();

        execute("insert into foo (id, name, country) values (?, ?, ?)", new Object[][]{
                new Object[] { 1, "Arthur", "Austria" },
                new Object[] { 2, "Trillian", "Austria" },
                new Object[] { 3, "Marvin", "Austria" },
                new Object[] { 4, "Jeltz", "Germany" },
                new Object[] { 5, "Ford", "Germany" },
                new Object[] { 6, "Slartibardfast", "Italy" },
        });
        refresh();

        execute("select count(*), country from foo group by country order by count(*) desc");
        assertThat(response.rowCount(), is(3L));
        assertThat((String) response.rows()[0][1], is("Austria"));
        assertThat((String) response.rows()[1][1], is("Germany"));
        assertThat((String) response.rows()[2][1], is("Italy"));
    }

    @Test
    public void testGroupByOnAllPrimaryKeys() throws Exception {
        execute("create table foo (id int primary key, name string primary key) with (number_of_replicas = 0)");
        ensureGreen();

        execute("insert into foo (id, name) values (?, ?)", new Object[][] {
                new Object[] { 1, "Arthur" },
                new Object[] { 2, "Trillian" },
                new Object[] { 3, "Slartibardfast" },
                new Object[] { 4, "Marvin" },
        });
        refresh();

        execute("select count(*), name from foo group by id, name order by name desc");
        assertThat(response.rowCount(), is(4L));
        assertThat((String) response.rows()[0][1], is("Trillian"));
        assertThat((String) response.rows()[1][1], is("Slartibardfast"));
        assertThat((String) response.rows()[2][1], is("Marvin"));
        assertThat((String) response.rows()[3][1], is("Arthur"));
    }

    @Test
    public void testRegexpMatchOperator() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        refresh();
        execute("select distinct name from locations where name ~ '[A-Z][a-z0-9]+' order by name");
        assertThat(response.rowCount(), is(5L));
        assertThat((String) response.rows()[0][0], is("Aldebaran"));
        assertThat((String) response.rows()[1][0], is("Algol"));
        assertThat((String) response.rows()[2][0], is("Altair"));
        assertThat((String) response.rows()[3][0], is("Argabuthon"));
        assertThat((String) response.rows()[4][0], is("Bartledan"));

        execute("select name from locations where name !~ '[A-Z][a-z0-9]+' order by name");
        assertThat(response.rowCount(), is(8L));
        assertThat((String) response.rows()[0][0], is(""));
        assertThat((String) response.rows()[1][0], is("Allosimanius Syneca"));
        assertThat((String) response.rows()[2][0], is("Alpha Centauri"));
        assertThat((String) response.rows()[3][0], is("Arkintoofle Minor"));
        assertThat((String) response.rows()[4][0], is("Galactic Sector QQ7 Active J Gamma"));
        assertThat((String) response.rows()[5][0], is("North West Ripple"));
        assertThat((String) response.rows()[6][0], is("Outer Eastern Rim"));
        assertThat(response.rows()[7][0], is(nullValue()));
    }

    @Test
    public void testWhereColumnEqColumnAndFunctionEqFunction() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        refresh();

        execute("select name from locations where name = name");
        assertThat(response.rowCount(), is(12L));  // one name is null and  null = null -> false

        execute("select name from locations where substr(name, 1, 1) = substr(name, 1, 1)");
        assertThat(response.rowCount(), is(12L));
    }
}


