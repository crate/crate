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

import io.crate.action.sql.SQLActionException;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SQLTypeMappingTest extends SQLTransportIntegrationTest {

    private void setUpSimple() throws IOException {
        setUpSimple(2);
    }

    private void setUpSimple(int numShards) {
        String stmt = String.format(Locale.ENGLISH, "create table t1 (" +
                                                    " id integer primary key," +
                                                    " string_field string," +
                                                    " boolean_field boolean," +
                                                    " byte_field byte," +
                                                    " short_field short," +
                                                    " integer_field integer," +
                                                    " long_field long," +
                                                    " float_field float," +
                                                    " double_field double," +
                                                    " timestamp_field timestamp with time zone," +
                                                    " object_field object as (\"inner\" timestamp with time zone)," +
                                                    " ip_field ip" +
                                                    ") clustered by (id) into %d shards " +
                                                    "with (number_of_replicas=0, column_policy = 'dynamic')", numShards);
        execute(stmt);
        ensureYellow();
    }

    @Test
    public void testInsertAtNodeWithoutShard() throws Exception {
        setUpSimple(1);

        execute("insert into t1 (id, string_field, timestamp_field, byte_field) values (?, ?, ?, ?)",
                new Object[]{1, "With", "1970-01-01T00:00:00", 127},
                createSessionOnNode(internalCluster().getNodeNames()[0]));

        execute("insert into t1 (id, string_field, timestamp_field, byte_field) values (?, ?, ?, ?)",
                new Object[]{2, "Without", "1970-01-01T01:00:00", Byte.MIN_VALUE},
                createSessionOnNode(internalCluster().getNodeNames()[1]));

        refresh();
        SQLResponse response = execute("select id, string_field, timestamp_field, byte_field from t1 order by id");

        assertEquals(1, response.rows()[0][0]);
        assertEquals("With", response.rows()[0][1]);
        assertEquals(0L, response.rows()[0][2]);
        assertEquals((byte) 127, response.rows()[0][3]);

        assertEquals(2, response.rows()[1][0]);
        assertEquals("Without", response.rows()[1][1]);
        assertEquals(3600000L, response.rows()[1][2]);
        assertEquals((byte) -128, response.rows()[1][3]);
    }

    public void setUpObjectTable() {
        execute("create table test12 (" +
                " object_field object(dynamic) as (size byte, created timestamp with time zone)," +
                " strict_field object(strict) as (path string, created timestamp with time zone)," +
                " no_dynamic_field object(ignored) as (" +
                "  path string, " +
                "  dynamic_again object(dynamic) as (field timestamp with time zone)" +
                " )" +
                ") clustered into 2 shards with(number_of_replicas=0)");
        ensureYellow();
    }

    /**
     * Disabled JDBC usage cause of text mode JSON encoding which is not type safe on numeric types.
     * E.g. byte values are always converted to integers,
     * see {@link com.fasterxml.jackson.core.JsonGenerator#writeNumber(short)}.
     */
    @UseJdbc(0)
    @Test
    public void testParseInsertObject() throws Exception {
        setUpObjectTable();

        execute("insert into test12 (object_field, strict_field, " +
                "no_dynamic_field) values (?,?,?)",
            new Object[]{
                new HashMap<String, Object>() {{
                    put("size", 127);
                    put("created", "2013-11-19");
                }},
                new HashMap<String, Object>() {{
                    put("path", "/dev/null");
                    put("created", "1970-01-01T00:00:00");
                }},
                new HashMap<String, Object>() {{
                    put("path", "/etc/shadow");
                    put("dynamic_again", new HashMap<String, Object>() {{
                            put("field", 1384790145.289);
                        }}
                    );
                }}
            });
        refresh();

        SQLResponse response = execute("select object_field, strict_field, no_dynamic_field from test12");
        assertEquals(1, response.rowCount());
        assertThat(response.rows()[0][0], instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> objectMap = (Map<String, Object>) response.rows()[0][0];
        assertEquals(1384819200000L, objectMap.get("created"));
        assertEquals((byte) 127, objectMap.get("size"));

        assertThat(response.rows()[0][1], instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> strictMap = (Map<String, Object>) response.rows()[0][1];
        assertEquals("/dev/null", strictMap.get("path"));
        assertEquals(0L, strictMap.get("created"));

        assertThat(response.rows()[0][2], instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> noDynamicMap = (Map<String, Object>) response.rows()[0][2];
        assertEquals("/etc/shadow", noDynamicMap.get("path"));
        assertEquals(
            new HashMap<String, Object>() {{
                put("field", 1384790145289L);
            }},
            noDynamicMap.get("dynamic_again")
        );

        response = execute("select object_field['created'], object_field['size'], " +
                           "no_dynamic_field['dynamic_again']['field'] from test12");
        assertEquals(1384819200000L, response.rows()[0][0]);
        assertEquals((byte) 127, response.rows()[0][1]);
        assertEquals(1384790145289L, response.rows()[0][2]);
    }

    @Test
    public void testInsertObjectField() throws Exception {
        expectedException.expect(SQLActionException.class);

        setUpObjectTable();
        execute("insert into test12 (object_field['size']) values (127)");

    }

    @Test
    public void testInvalidInsertIntoObject() throws Exception {
        setUpObjectTable();

        expectedException.expect(SQLActionException.class);
        // Value formatting differs between jdbc & non jdbc
        expectedException.expectMessage(allOf(
            containsString("Validation failed for object_field"),
            containsString("Invalid value"),
            containsString("for type 'object'"))
        );
        execute("insert into test12 (object_field, strict_field) values (?,?)", new Object[]{
            new HashMap<String, Object>() {{
                put("created", true);
                put("size", 127);
            }},
            new HashMap<String, Object>() {{
                put("path", "/dev/null");
                put("created", 0);
            }}
        });
    }

    @Test
    public void testInvalidWhereClause() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot cast `129` of type `bigint` to type `char`");

        setUpSimple();
        execute("delete from t1 where byte_field=129");
    }

    @Test
    public void testInvalidWhereInWhereClause() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot cast `'a'` of type `text` to type `char`");

        setUpSimple();
        execute("update t1 set byte_field=0 where byte_field in ('a')");
    }

    @Test
    public void testSetUpdate() throws Exception {
        setUpSimple();

        execute("insert into t1 (id, byte_field, short_field, integer_field, long_field, " +
                "float_field, double_field, boolean_field, string_field, timestamp_field," +
                "object_field) values (?,?,?,?,?,?,?,?,?,?,?)", new Object[]{
            0, 0, 0, 0, 0, 0.0f, 1.0, false, "", "1970-01-01", new HashMap<String, Object>() {{
            put("inner", "1970-01-01");
        }}
        });
        execute("update t1 set " +
                "byte_field=?," +
                "short_field=?," +
                "integer_field=?," +
                "long_field=?," +
                "float_field=?," +
                "double_field=?," +
                "boolean_field=?," +
                "string_field=?," +
                "timestamp_field=?," +
                "object_field=?," +
                "ip_field=? " +
                "where id=0", new Object[]{
            Byte.MAX_VALUE, Short.MIN_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE,
            1.0f, Math.PI, true, "a string", "2013-11-20",
            new HashMap<String, Object>() {{
                put("inner", "2013-11-20");
            }}, "127.0.0.1"
        });
        refresh();

        SQLResponse response = execute("select id, byte_field, short_field, integer_field, long_field," +
                                       "float_field, double_field, boolean_field, string_field, timestamp_field," +
                                       "object_field, ip_field from t1 where id=0");
        assertEquals(1, response.rowCount());
        assertEquals(0, response.rows()[0][0]);
        assertEquals((byte) 127, response.rows()[0][1]);
        assertEquals((short) -32768, response.rows()[0][2]);
        assertEquals(0x7fffffff, response.rows()[0][3]);
        assertEquals(0x8000000000000000L, response.rows()[0][4]);
        assertEquals(1.0f, ((Number) response.rows()[0][5]).floatValue(), 0.01f);
        assertEquals(Math.PI, response.rows()[0][6]);
        assertEquals(true, response.rows()[0][7]);
        assertEquals("a string", response.rows()[0][8]);
        assertEquals(1384905600000L, response.rows()[0][9]);
        assertEquals(new HashMap<String, Object>() {{
            put("inner", 1384905600000L);
        }}, response.rows()[0][10]);
        assertEquals("127.0.0.1", response.rows()[0][11]);
    }

    /**
     * We must fix this test to run ALL statements via JDBC or not because object/map values are NOT preserving exact
     * its elements numeric types (e.g. Long(0) becomes Integer(0)). This is caused by the usage of JSON for psql text
     * serialization. See e.g. {@link org.codehaus.jackson.io.NumberOutput#outputLong(long, byte[], int)}.
     */
    @UseJdbc(0)
    @Test
    public void testGetRequestMapping() throws Exception {
        setUpSimple();
        execute("insert into t1 (id, string_field, boolean_field, byte_field, short_field, integer_field," +
                "long_field, float_field, double_field, object_field," +
                "timestamp_field, ip_field) values " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", new Object[]{
            0, "Blabla", true, 120, 1000, 1200000,
            120000000000L, 1.4, 3.456789, new HashMap<String, Object>() {{
            put("inner", "1970-01-01");
        }},
            "1970-01-01", "127.0.0.1"
        });
        refresh();
        SQLResponse getResponse = execute("select * from t1 where id=0");
        SQLResponse searchResponse = execute("select * from t1 limit 1");
        for (int i = 0; i < getResponse.rows()[0].length; i++) {
            assertThat(getResponse.rows()[0][i], is(searchResponse.rows()[0][i]));
        }
    }

    @Test
    public void testInsertObjectIntoString() throws Exception {
        execute("create table t1 (o object)");
        execute("insert into t1 values ({a='abc'})");
        waitForMappingUpdateOnAll("t1", "o.a");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot cast `{\"a\"=['123', '456']}` of type `object` to type `object`");
        execute("insert into t1 values ({a=['123', '456']})");
    }

    /**
     * Disable JDBC/PSQL as object values are streamed via JSON on the PSQL wire protocol which is not type safe.
     */
    @UseJdbc(0)
    @Test
    public void testInsertNewObjectColumn() throws Exception {
        setUpSimple();
        execute("insert into t1 (id, new_col) values (?,?)", new Object[]{
            0,
            new HashMap<String, Object>() {{
                put("a_date", "1970-01-01");
                put("an_int", 127);
                put("a_long", Long.MAX_VALUE);
                put("a_boolean", true);
            }}
        });
        refresh();

        waitForMappingUpdateOnAll("t1", "new_col");
        SQLResponse response = execute("select id, new_col from t1 where id=0");
        @SuppressWarnings("unchecked")
        Map<String, Object> mapped = (Map<String, Object>) response.rows()[0][1];
        assertEquals("1970-01-01", mapped.get("a_date"));
        assertEquals(0x7fffffffffffffffL, mapped.get("a_long"));
        assertEquals(true, mapped.get("a_boolean"));

        // The inner value will result in an Long type as we rely on ES mappers here and the dynamic ES parsing
        // will define integers as longs (no concrete type was specified so use long to be safe)
        assertEquals(127L, mapped.get("an_int"));
    }

    @Test
    public void testInsertNewColumnToObject() throws Exception {
        setUpObjectTable();
        Map<String, Object> objectContent = new HashMap<String, Object>() {{
            put("new_col", "a string");
            put("another_new_col", "1970-01-01T00:00:00");
        }};
        execute("insert into test12 (object_field) values (?)",
            new Object[]{objectContent});
        refresh();
        SQLResponse response = execute("select object_field from test12");
        assertEquals(1, response.rowCount());
        @SuppressWarnings("unchecked")
        Map<String, Object> selectedObject = (Map<String, Object>) response.rows()[0][0];

        assertThat((String) selectedObject.get("new_col"), is("a string"));
        assertEquals("1970-01-01T00:00:00", selectedObject.get("another_new_col"));
    }

    @Test
    public void testInsertNewColumnToStrictObject() throws Exception {

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(
            containsString("dynamic introduction of [another_new_col] within [strict_field] is not allowed"));

        setUpObjectTable();
        Map<String, Object> strictContent = new HashMap<String, Object>() {{
            put("another_new_col", "1970-01-01T00:00:00");
        }};
        execute("insert into test12 (strict_field) values (?)",
            new Object[]{strictContent});
    }

    @Test
    public void testInsertNewColumnToIgnoredObject() throws Exception {

        setUpObjectTable();
        Map<String, Object> notDynamicContent = new HashMap<String, Object>() {{
            put("new_col", "a string");
            put("another_new_col", "1970-01-01T00:00:00");
        }};
        execute("insert into test12 (no_dynamic_field) values (?)",
            new Object[]{notDynamicContent});
        refresh();
        SQLResponse response = execute("select no_dynamic_field from test12");
        assertEquals(1, response.rowCount());
        @SuppressWarnings("unchecked")
        Map<String, Object> selectedNoDynamic = (Map<String, Object>) response.rows()[0][0];
        // no mapping applied
        assertThat((String) selectedNoDynamic.get("new_col"), is("a string"));
        assertThat((String) selectedNoDynamic.get("another_new_col"), is("1970-01-01T00:00:00"));
    }

    /* TODO: find a good policy for unknown types or support them all
    @Test
    public void testUnknownTypesSelect() throws Exception {
        this.setup.setUpObjectMappingWithUnknownTypes();
        SQLResponse response = execute("select * from ut");
        assertEquals(2, response.rowCount());
        assertArrayEquals(new String[]{"name", "population"}, response.cols());

        response = execute("select name, location from ut order by name");
        assertEquals("Berlin", response.rows()[0][0]);
        assertEquals(null, response.rows()[0][1]);
    }


    @Test
    public void testUnknownTypesInsert() throws Exception {
        this.setup.setUpObjectMappingWithUnknownTypes();
        SQLResponse response = execute(
                "insert into ut (name, location, population) values (?, ?, ?)",
                new Object[]{"Köln", "2014-01-09", 0}
        );
        assertEquals(1, response.rowCount());
        refresh();

        response = execute("select name, location, population from ut order by name");
        assertEquals(3, response.rowCount());
        assertEquals("Berlin", response.rows()[0][0]);
        assertEquals(null, response.rows()[0][1]);

        assertEquals("Dornbirn", response.rows()[1][0]);
        assertEquals(null, response.rows()[1][1]);

        assertEquals("Köln", response.rows()[2][0]);
        assertEquals(null, response.rows()[2][1]);
    }

    @Test
    public void testUnknownTypesUpdate() throws Exception {
        this.setup.setUpObjectMappingWithUnknownTypes();
        execute("update ut set location='2014-01-09' where name='Berlin'");
        SQLResponse response = execute("select name, location from ut where name='Berlin'");
        assertEquals(1, response.rowCount());
        assertEquals("Berlin", response.rows()[0][0]);
        assertEquals("52.5081,13.4416", response.rows()[0][1]);
    } */

    @Test
    public void testDynamicEmptyArray() throws Exception {
        execute("create table arr (id short primary key, tags array(string)) " +
                "with (number_of_replicas=0, column_policy = 'dynamic')");
        ensureYellow();
        execute("insert into arr (id, tags, new) values (1, ['wow', 'much', 'wow'], [])");
        refresh();
        waitNoPendingTasksOnAll();
        execute("select column_name, data_type from information_schema.columns where table_name='arr'");
        Object[] columns = TestingHelpers.getColumn(response.rows(), 0);
        assertThat(Arrays.asList(columns), not(hasItems((Object) "new")));
    }

    @Test
    public void testDynamicNullArray() throws Exception {
        execute("create table arr (id short primary key, tags array(string)) " +
                "with (number_of_replicas=0, column_policy = 'dynamic')");
        ensureYellow();
        execute("insert into arr (id, tags, new) values (2, ['wow', 'much', 'wow'], [null])");
        refresh();
        waitNoPendingTasksOnAll();
        execute("select column_name, data_type from information_schema.columns where table_name='arr'");
        Object[] columns = TestingHelpers.getColumn(response.rows(), 0);
        assertThat(Arrays.asList(columns), not(hasItems((Object) "new")));
    }

    @Test
    public void testDynamicNullArrayAndDouble() throws Exception {
        execute("create table arr (id short primary key, tags array(string)) " +
                "with (number_of_replicas=0, column_policy = 'dynamic')");
        ensureYellow();
        execute("insert into arr (id, tags, new) values (3, ['wow', 'much', 'wow'], ?)", new Object[]{new Double[]{null, 42.7}});
        refresh();
        waitNoPendingTasksOnAll();
        awaitBusy(() -> {
            SQLResponse res = execute("select column_name, data_type from information_schema.columns where table_name='arr'");
            for (Object[] row : res.rows()) {
                if ("new".equals(row[0]) && "double_array".equals(row[1])) {
                    return true;
                }
            }
            return false;
        });
    }

    @Test
    public void testTwoLevelNestedArrayColumn() throws Exception {
        execute("create table assets (categories array(object as (items array(object as (id int)))))");
        execute("insert into assets (categories) values ([{items=[{id=10}, {id=20}]}])");
        ensureYellow();
        execute("refresh table assets");

        refresh();
        waitNoPendingTasksOnAll();
        execute("select categories['items']['id'] from assets");
        Object[] columns = TestingHelpers.getColumn(response.rows(), 0);
//TODO: Re-enable once SQLResponse also includes the data types for the columns
//        assertThat(response.columnTypes()[0], is((DataType)new ArrayType(new ArrayType(IntegerType.INSTANCE))));
        assertThat(((List) ((List) columns[0]).get(0)).get(0), is(10));
        assertThat(((List) ((List) columns[0]).get(0)).get(1), is(20));
    }

    @Test
    public void testThreeLevelNestedArrayColumn() throws Exception {
        execute("create table assets (categories array(object as (subcategories array(object as (" +
                "items array(object as (id int)))))))");
        execute("insert into assets (categories) values ([{subcategories=[{items=[{id=10}, {id=20}]}]}])");
        ensureYellow();
        execute("refresh table assets");

        refresh();
        waitNoPendingTasksOnAll();
        execute("select categories['subcategories']['items']['id'] from assets");
        Object[] columns = TestingHelpers.getColumn(response.rows(), 0);
//TODO: Re-enable once SQLResponse also includes the data types for the columns
//        assertThat(response.columnTypes()[0],
//                   is((DataType)new ArrayType(new ArrayType(new ArrayType(IntegerType.INSTANCE)))));
        assertThat(((List) ((List) ((List) columns[0]).get(0)).get(0)).get(0), is(10));
        assertThat(((List) ((List) ((List) columns[0]).get(0)).get(0)).get(1), is(20));
    }

    @Test
    public void testTwoLevelNestedObjectInArray() throws Exception {
        execute("create table assets (\n" +
                "    categories array(object (dynamic) as (\n" +
                "       subcategories object (dynamic) as (\n" +
                "          id int))))");
        execute("insert into assets(categories) values ([{subcategories={id=10}}, {subcategories={id=20}}])");
        ensureYellow();
        execute("refresh table assets");

        refresh();
        waitNoPendingTasksOnAll();
        SQLResponse response = execute("select categories[1]['subcategories']['id'] from assets");
        assertEquals(response.rowCount(), 1L);
        assertThat((Integer) response.rows()[0][0], is(10));
    }

    @Test
    public void testInsertTimestamp() {
        // This is a regression test that we allow timestamps that have more than 13 digits.
        execute(
            "create table ts_table (" +
            "   ts timestamp with time zone" +
            ") clustered into 2 shards with (number_of_replicas=0)");
        ensureYellow();
        // biggest Long that can be converted to Double without losing precision
        // equivalent to 33658-09-27T01:46:39.999Z
        long maxDateMillis = 999999999999999L;
        // smallest Long that can be converted to Double without losing precision
        // equivalent to -29719-04-05T22:13:20.001Z
        long minDateMillis = -999999999999999L;

        execute("insert into ts_table (ts) values (?)", new Object[]{ minDateMillis });
        execute("insert into ts_table (ts) values (?)", new Object[]{ 0L });
        execute("insert into ts_table (ts) values (?)", new Object[]{ maxDateMillis });
        // TODO: select timestamps with correct sorting
        refresh();
        SQLResponse response = execute("select * from ts_table order by ts desc");
        assertEquals(response.rowCount(), 3L);
    }

    @Test
    public void testInsertTimestampPreferMillis() {
        execute("create table ts_table (ts timestamp with time zone) " +
                "clustered into 2 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into ts_table (ts) values (?)", new Object[]{ 1000L });
        execute("insert into ts_table (ts) values (?)", new Object[]{ "2016" });
        refresh();
        SQLResponse response = execute("select ts from ts_table order by ts asc");
        assertThat((Long) response.rows()[0][0], is(1000L));
        assertThat((Long) response.rows()[1][0], is(2016L));
    }

    @Test
    public void test_insert_time_without_time_zone() {
        execute("create table eons_table (dt time) " +
                "clustered into 2 shards with (number_of_replicas=0)");
        execute("insert into eons_table (dt) values (?)", new Long[]{ 45296789L });
        refresh();
        SQLResponse response = execute("select dt from eons_table");
        assertThat(response.rows()[0][0], is(45296789L));
    }
}
