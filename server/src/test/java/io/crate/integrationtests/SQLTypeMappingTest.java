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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.assertj.core.data.Offset;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.Asserts;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SQLTypeMappingTest extends IntegTestCase {

    private void setUpSimple() {
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
    public void testInsertAtNodeWithoutShard() {
        setUpSimple(1);

        try (var session = createSessionOnNode(cluster().getNodeNames()[0])) {
            execute(
                "insert into t1 (id, string_field, timestamp_field, byte_field) values (?, ?, ?, ?)",
                new Object[]{1, "With", "1970-01-01T00:00:00", 127},
                session
            );
        }

        try (var session = createSessionOnNode(cluster().getNodeNames()[1])) {
            execute(
                "insert into t1 (id, string_field, timestamp_field, byte_field) values (?, ?, ?, ?)",
                new Object[] {2, "Without", "1970-01-01T01:00:00", Byte.MIN_VALUE},
                session
            );
        }

        refresh();
        SQLResponse response = execute("select id, string_field, timestamp_field, byte_field from t1 order by id");

        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isEqualTo("With");
        assertThat(response.rows()[0][2]).isEqualTo(0L);
        assertThat(response.rows()[0][3]).isEqualTo((byte) 127);

        assertThat(response.rows()[1][0]).isEqualTo(2);
        assertThat(response.rows()[1][1]).isEqualTo("Without");
        assertThat(response.rows()[1][2]).isEqualTo(3600000L);
        assertThat(response.rows()[1][3]).isEqualTo((byte) -128);
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
    @SuppressWarnings("unchecked")
    public void testParseInsertObject() throws Exception {
        setUpObjectTable();

        execute("insert into test12 (object_field, strict_field, " +
                "no_dynamic_field) values (?,?,?)",
            new Object[]{
                Map.of(
                    "size", 127,
                    "created", "2013-11-19"),
                Map.of(
                    "path", "/dev/null",
                    "created", "1970-01-01T00:00:00"),
                Map.of(
                    "path", "/etc/shadow",
                    "dynamic_again", Map.of("field", 1384790145.289))});
        refresh();

        SQLResponse response = execute("select object_field, strict_field, no_dynamic_field from test12");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isInstanceOf(Map.class);
        Map<String, Object> objectMap = (Map<String, Object>) response.rows()[0][0];
        assertThat(objectMap.get("created")).isEqualTo(1384819200000L);
        assertThat(objectMap.get("size")).isEqualTo((byte) 127);

        assertThat(response.rows()[0][1]).isInstanceOf(Map.class);
        Map<String, Object> strictMap = (Map<String, Object>) response.rows()[0][1];
        assertThat(strictMap.get("path")).isEqualTo("/dev/null");
        assertThat(strictMap.get("created")).isEqualTo(0L);

        assertThat(response.rows()[0][2]).isInstanceOf(Map.class);
        Map<String, Object> noDynamicMap = (Map<String, Object>) response.rows()[0][2];
        assertThat(noDynamicMap.get("path")).isEqualTo("/etc/shadow");
        assertThat(noDynamicMap.get("dynamic_again")).isEqualTo(Map.of("field", 1384790145289L));

        response = execute("select object_field['created'], object_field['size'], " +
                           "no_dynamic_field['dynamic_again']['field'] from test12");
        assertThat(response.rows()[0][0]).isEqualTo(1384819200000L);
        assertThat(response.rows()[0][1]).isEqualTo((byte) 127);
        assertThat(response.rows()[0][2]).isEqualTo(1384790145289L);
    }

    @Test
    public void testInsertObjectField() throws Exception {
        setUpObjectTable();

        Asserts.assertSQLError(() -> execute("insert into test12 (object_field['size']) values (127)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Invalid column reference \"object_field\"['size'] used in INSERT INTO statement");
    }

    @Test
    public void testInvalidInsertIntoObject() throws Exception {
        setUpObjectTable();

        Asserts.assertSQLError(
            () -> execute("insert into test12 (object_field, strict_field) values (?,?)", new Object[]{
                Map.of("created", true, "size", 127),
                Map.of("path", "/dev/null", "created", 0)
            }))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot cast object element `created` with value `true` to type `timestamp with time zone`");

        waitNoPendingTasksOnAll();
    }

    @Test
    public void testInvalidWhereClause() throws Exception {
        setUpSimple();

        Asserts.assertSQLError(() -> execute("delete from t1 where byte_field=129"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot cast `129` of type `integer` to type `byte`");
    }

    @Test
    public void testInvalidWhereInWhereClause() throws Exception {
        setUpSimple();

        Asserts.assertSQLError(() -> execute("update t1 set byte_field=0 where byte_field in (129)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot cast `[129]` of type `integer_array` to type `byte_array`");
    }

    @Test
    public void testSetUpdate() throws Exception {
        setUpSimple();

        execute("insert into t1 (id, byte_field, short_field, integer_field, long_field, " +
                "float_field, double_field, boolean_field, string_field, timestamp_field," +
                "object_field) values (?,?,?,?,?,?,?,?,?,?,?)",
                new Object[]{0, 0, 0, 0, 0, 0.0f, 1.0, false, "", "1970-01-01", Map.of("inner", "1970-01-01")});
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
                "where id=0",
                new Object[]{
                    Byte.MAX_VALUE, Short.MIN_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE,
                    1.0f, Math.PI, true, "a string", "2013-11-20",
                    Map.of("inner", "2013-11-20"), "127.0.0.1"
                });
        refresh();

        SQLResponse response = execute("select id, byte_field, short_field, integer_field, long_field," +
                                       "float_field, double_field, boolean_field, string_field, timestamp_field," +
                                       "object_field, ip_field from t1 where id=0");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(0);
        assertThat(response.rows()[0][1]).isEqualTo((byte) 127);
        assertThat(response.rows()[0][2]).isEqualTo((short) -32768);
        assertThat(response.rows()[0][3]).isEqualTo(0x7fffffff);
        assertThat(response.rows()[0][4]).isEqualTo(0x8000000000000000L);
        assertThat(((Number) response.rows()[0][5]).floatValue()).isCloseTo(1.0f, Offset.offset(0.01f));
        assertThat(response.rows()[0][6]).isEqualTo(Math.PI);
        assertThat(response.rows()[0][7]).isEqualTo(true);
        assertThat(response.rows()[0][8]).isEqualTo("a string");
        assertThat(response.rows()[0][9]).isEqualTo(1384905600000L);
        assertThat(response.rows()[0][10]).isEqualTo(Map.of("inner", 1384905600000L));
        assertThat(response.rows()[0][11]).isEqualTo("127.0.0.1");
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
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                new Object[]{
                    0, "Blabla", true, 120, 1000, 1200000,
                    120000000000L, 1.4, 3.456789, Map.of("inner", "1970-01-01"),
                    "1970-01-01", "127.0.0.1"});
        refresh();
        SQLResponse getResponse = execute("select * from t1 where id=0");
        SQLResponse searchResponse = execute("select * from t1 limit 1");
        for (int i = 0; i < getResponse.rows()[0].length; i++) {
            assertThat(getResponse.rows()[0][i]).isEqualTo(searchResponse.rows()[0][i]);
        }
    }

    @Test
    public void testInsertObjectIntoString() throws Exception {
        execute("create table t1 (o object)");
        execute("insert into t1 values ({a='abc'})");
        waitForMappingUpdateOnAll("t1", "o.a");

        Asserts.assertSQLError(() -> execute("insert into t1 values ({a=['123', '456']})"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot cast object element `a` with value `[123, 456]` to type `text`");
    }

    /**
     * Disable JDBC/PSQL as object values are streamed via JSON on the PSQL wire protocol which is not type safe.
     */
    @UseJdbc(0)
    @Test
    public void testInsertNewObjectColumn() throws Exception {
        setUpSimple();
        execute("insert into t1 (id, new_col) values (?,?)",
                new Object[]{
                    0,
                    Map.of(
                        "a_date", "1970-01-01",
                        "an_int", 127,
                        "a_long", Long.MAX_VALUE,
                        "a_boolean", true)
                    });
        refresh();

        // Need to go through all new leafs as waiting on top-level can be resolved if at least one of the root->child is succeeded.
        waitForMappingUpdateOnAll("t1", "new_col.a_date", "new_col.an_int", "new_col.a_long", "new_col.a_boolean");
        SQLResponse response = execute("select id, new_col from t1 where id=0");
        @SuppressWarnings("unchecked")
        Map<String, Object> mapped = (Map<String, Object>) response.rows()[0][1];
        assertThat(mapped.get("a_date")).isEqualTo("1970-01-01");
        assertThat(mapped.get("a_long")).isEqualTo(0x7fffffffffffffffL);
        assertThat(mapped.get("a_boolean")).isEqualTo(true);

        // The inner value will result in an Long type as we rely on ES mappers here and the dynamic ES parsing
        // will define integers as longs (no concrete type was specified so use long to be safe)
        assertThat(mapped.get("an_int")).isEqualTo(127L);
    }

    @Test
    public void testInsertNewColumnToObject() throws Exception {
        setUpObjectTable();
        Map<String, Object> objectContent = Map.of(
            "new_col", "a string",
            "another_new_col", "1970-01-01T00:00:00");
        execute("insert into test12 (object_field) values (?)",
            new Object[]{objectContent});
        refresh();
        SQLResponse response = execute("select object_field from test12");
        assertThat(response.rowCount()).isEqualTo(1);
        @SuppressWarnings("unchecked")
        Map<String, Object> selectedObject = (Map<String, Object>) response.rows()[0][0];

        assertThat((String) selectedObject.get("new_col")).isEqualTo("a string");
        assertThat(selectedObject.get("another_new_col")).isEqualTo("1970-01-01T00:00:00");
    }

    @Test
    public void testInsertNewColumnToStrictObject() throws Exception {
        setUpObjectTable();

        Asserts.assertSQLError(() -> execute("insert into test12 (strict_field) values (?)",
                                    new Object[]{Map.of("another_new_col", "1970-01-01T00:00:00")}))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot add column `another_new_col` to strict object `strict_field`");

        waitNoPendingTasksOnAll();
    }

    @Test
    public void testInsertNewColumnToIgnoredObject() throws Exception {

        setUpObjectTable();
        Map<String, Object> notDynamicContent = Map.of(
            "new_col", "a string",
            "another_new_col", "1970-01-01T00:00:00");
        execute("insert into test12 (no_dynamic_field) values (?)",
            new Object[]{notDynamicContent});
        refresh();
        SQLResponse response = execute("select no_dynamic_field from test12");
        assertThat(response.rowCount()).isEqualTo(1);
        @SuppressWarnings("unchecked")
        Map<String, Object> selectedNoDynamic = (Map<String, Object>) response.rows()[0][0];
        // no mapping applied
        assertThat((String) selectedNoDynamic.get("new_col")).isEqualTo("a string");
        assertThat((String) selectedNoDynamic.get("another_new_col")).isEqualTo("1970-01-01T00:00:00");
    }

    /* TODO: find a good policy for unknown types or support them all
    @Test
    public void testUnknownTypesSelect() throws Exception {
        this.setup.setUpObjectMappingWithUnknownTypes();
        SQLResponse response = execute("select * from ut");
        assertThat(response.rowCount()).isEqualTo(2);
        assertArrayEquals(new String[]{"name", "population"}, response.cols());

        response = execute("select name, location from ut order by name");
        assertThat(response.rows()[0][0]).isEqualTo("Berlin");
        assertThat(response.rows()[0][1]).isEqualTo(null);
    }


    @Test
    public void testUnknownTypesInsert() throws Exception {
        this.setup.setUpObjectMappingWithUnknownTypes();
        SQLResponse response = execute(
                "insert into ut (name, location, population) values (?, ?, ?)",
                new Object[]{"Köln", "2014-01-09", 0}
        );
        assertThat(response.rowCount()).isEqualTo(1);
        refresh();

        response = execute("select name, location, population from ut order by name");
        assertThat(response.rowCount()).isEqualTo(3);
        assertThat(response.rows()[0][0]).isEqualTo("Berlin");
        assertThat(response.rows()[0][1]).isEqualTo(null);

        assertThat(response.rows()[1][0]).isEqualTo("Dornbirn");
        assertThat(response.rows()[1][1]).isEqualTo(null);

        assertThat(response.rows()[2][0]).isEqualTo("Köln");
        assertThat(response.rows()[2][1]).isEqualTo(null);
    }

    @Test
    public void testUnknownTypesUpdate() throws Exception {
        this.setup.setUpObjectMappingWithUnknownTypes();
        execute("update ut set location='2014-01-09' where name='Berlin'");
        SQLResponse response = execute("select name, location from ut where name='Berlin'");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo("Berlin");
        assertThat(response.rows()[0][1]).isEqualTo("52.5081,13.4416");
    } */

    @Test
    public void test_dynamic_empty_array_does_not_result_in_new_column() throws Exception {
        execute("create table arr (id short primary key, tags array(string)) " +
                "with (number_of_replicas=0, column_policy = 'dynamic')");
        ensureYellow();
        execute("insert into arr (id, tags, new) values (1, ['wow', 'much', 'wow'], [])");
        refresh();
        waitNoPendingTasksOnAll();
        execute("select column_name, data_type from information_schema.columns where table_name='arr' order by 1");
        assertThat(response).hasRows(
            "id| smallint",
            "tags| text_array"
        );
        assertThat(execute("select _doc from arr")).hasRows(
            "{id=1, new=[], tags=[wow, much, wow]}"
        );
    }

    @Test
    public void testDynamicNullArray_does_not_result_in_new_column() throws Exception {
        execute("create table arr (id short primary key, tags array(string)) " +
                "with (number_of_replicas=0, column_policy = 'dynamic')");
        ensureYellow();
        execute("insert into arr (id, tags, new, \"2\") values (2, ['wow', 'much', 'wow'], [null], [])");
        refresh();
        waitNoPendingTasksOnAll();
        execute("select column_name, data_type from information_schema.columns where table_name='arr' order by 1");
        assertThat(response).hasRows(
            "id| smallint",
            "tags| text_array"
        );
        assertThat(execute("select _doc from arr")).hasRows(
            "{2=[], id=2, new=[null], tags=[wow, much, wow]}"
        );
    }

    @Test
    public void testDynamicNullArrayAndDouble() throws Exception {
        execute("create table arr (id short primary key, tags array(string)) " +
                "with (number_of_replicas=0, column_policy = 'dynamic')");
        execute("insert into arr (id, tags, new) values (3, ['wow', 'much', 'wow'], ?)", new Object[]{new Double[]{null, 42.7}});
        assertBusy(() -> {
            SQLResponse res = execute(
                "select column_name, data_type from information_schema.columns where " +
                " table_name='arr' order by ordinal_position desc limit 1");
            assertThat(res).hasRows("new| double precision_array");
        });
    }

    // https://github.com/crate/crate/issues/13990
    @Test
    public void test_dynamic_null_array_overridden_to_integer_becomes_null() {
        execute("create table t (a int) with (column_policy ='dynamic')");
        execute("insert into t (x) values ([])");
        execute("insert into t (x) values (1)");
        refresh();
        execute("select * from t");
        assertThat(printedTable(response.rows())).contains("NULL| NULL", "NULL| 1");
        // takes different paths than without order by like above
        execute("select * from t order by x nulls first");
        assertThat(response).hasRows(
            "NULL| NULL",
            "NULL| 1"
        );
    }

    // https://github.com/crate/crate/issues/13990
    @Test
    public void test_dynamic_null_array_get_by_pk() throws Exception {
        execute("create table t (a int primary key, o object(dynamic))");
        execute("insert into t (a, o) values (1, {x={y=[]}})");
        execute("insert into t (a, o) values (2, {x={y={}}})");
        execute("refresh table t");
        waitForMappingUpdateOnAll("t", "o.x.y");
        execute("select * from t where a = 1");
        assertThat(response).hasRows("1| {x={y=NULL}}");
        execute("select * from t where a = 2");
        assertThat(response).hasRows("2| {x={y={}}}");
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
//        assertThat(response.columnTypes()[0]).isEqualTo((DataType)new ArrayType(new ArrayType(IntegerType.INSTANCE)));
        assertThat(((List<?>) ((List<?>) columns[0]).get(0)).get(0)).isEqualTo(10);
        assertThat(((List<?>) ((List<?>) columns[0]).get(0)).get(1)).isEqualTo(20);
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
        assertThat(((List<?>) ((List<?>) ((List<?>) columns[0]).get(0)).get(0)).get(0)).isEqualTo(10);
        assertThat(((List<?>) ((List<?>) ((List<?>) columns[0]).get(0)).get(0)).get(1)).isEqualTo(20);
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
        assertThat(1L).isEqualTo(response.rowCount());
        assertThat((Integer) response.rows()[0][0]).isEqualTo(10);
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
        assertThat(3L).isEqualTo(response.rowCount());
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
        assertThat((Long) response.rows()[0][0]).isEqualTo(1000L);
        assertThat((Long) response.rows()[1][0]).isEqualTo(2016L);
    }
}
