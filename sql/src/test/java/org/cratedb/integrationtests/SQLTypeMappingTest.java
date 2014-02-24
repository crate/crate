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

import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.ColumnUnknownException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.UnsupportedFeatureException;
import org.cratedb.sql.ValidationException;
import org.elasticsearch.client.Client;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class SQLTypeMappingTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static SQLParseService parseService;
    private Setup setup = new Setup(this);

    @Before
    protected void beforeSQLTypeMappingTest() {
        parseService = cluster().getInstance(SQLParseService.class);
    }

    @AfterClass
    protected static void afterSQLTypeMappingTest() {
        parseService = null;
    }

    private void setUpSimple() throws IOException {
        setUpSimple(2);
    }

    private void setUpSimple(int numShards) throws IOException {
        String stmt = String.format("create table t1 (" +
                " id integer primary key," +
                " string_field string," +
                " boolean_field boolean," +
                " byte_field byte," +
                " short_field short," +
                " integer_field integer," +
                " long_field long," +
                " float_field float," +
                " double_field double," +
                " timestamp_field timestamp," +
                " object_field object as (\"inner\" timestamp)," +
                " ip_field ip" +
                ") clustered by (id) into %d shards replicas 0", numShards);
        execute(stmt);

    }

    @Test
    public void testInsertAtNodeWithoutShard() throws Exception {
        setUpSimple(1);

        Iterator<Client> iterator = clients().iterator();
        Client client1 = iterator.next();
        Client client2 = iterator.next();

        client1.execute(SQLAction.INSTANCE, new SQLRequest(
            "insert into t1 (id, string_field, " +
                "timestamp_field, byte_field) values (?, ?, ?, ?)", new Object[]{1, "With",
            "1970-01-01T00:00:00", 127})).actionGet();

        client2.execute(SQLAction.INSTANCE, new SQLRequest(
            "insert into t1 (id, string_field, timestamp_field, byte_field) values (?, ?, ?, ?)",
            new Object[]{2, "Without", "1970-01-01T01:00:00", Byte.MIN_VALUE})).actionGet();
        refresh();
        SQLResponse response = execute("select id, string_field, timestamp_field, byte_field from t1 order by id");

        assertEquals(1, response.rows()[0][0]);
        assertEquals("With", response.rows()[0][1]);
        assertEquals(0, response.rows()[0][2]);
        assertEquals(127, response.rows()[0][3]);

        assertEquals(2, response.rows()[1][0]);
        assertEquals("Without", response.rows()[1][1]);
        assertEquals(3600000, response.rows()[1][2]);
        assertEquals(-128, response.rows()[1][3]);
    }

    public void setUpObjectTable() throws IOException {
        execute("create table test12 (" +
                " object_field object(dynamic) as (size byte, created timestamp)," +
                " strict_field object(strict) as (path string, created timestamp)," +
                " no_dynamic_field object(ignored) as (" +
                "  path string, " +
                "  dynamic_again object(dynamic) as (field timestamp)" +
                " )" +
                ") clustered into 2 shards replicas 0");
        refresh();
    }

    @Test
    public void testParseInsertObject() throws Exception {
        setUpObjectTable();

        execute("insert into test12 (object_field, strict_field, " +
                "no_dynamic_field) values (?,?,?)",
                new Object[]{
                    new HashMap<String, Object>(){{
                        put("size", 127);
                        put("created", "2013-11-19");
                    }},
                    new HashMap<String, Object>(){{
                       put("path", "/dev/null");
                       put("created", "1970-01-01T00:00:00");
                    }},
                    new HashMap<String, Object>(){{
                        put("path", "/etc/shadow");
                        put("dynamic_again", new HashMap<String, Object>(){{
                                put("field", 1384790145.289);
                            }}
                        );
                    }}
        });
        refresh();

        SQLResponse response = execute("select object_field, strict_field, " +
                "no_dynamic_field from test12");
        assertEquals(1, response.rowCount());
        assertThat(response.rows()[0][0], instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> objectMap = (Map<String, Object>)response.rows()[0][0];
        assertEquals(1384819200000L, objectMap.get("created"));
        assertEquals(127, objectMap.get("size"));

        assertThat(response.rows()[0][1], instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> strictMap = (Map<String, Object>)response.rows()[0][1];
        assertEquals("/dev/null", strictMap.get("path"));
        assertEquals(0, strictMap.get("created"));

        assertThat(response.rows()[0][2], instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> noDynamicMap = (Map<String, Object>)response.rows()[0][2];
        assertEquals("/etc/shadow", noDynamicMap.get("path"));
        assertEquals(
                new HashMap<String, Object>(){{ put("field", 1384790145289L); }},
                noDynamicMap.get("dynamic_again")
        );

        response = execute("select object_field['created'], object_field['size'], " +
                "no_dynamic_field['dynamic_again']['field'] from test12");
        assertEquals(1384819200000L, response.rows()[0][0]);
        assertEquals(127, response.rows()[0][1]);
        assertEquals(1384790145289L, response.rows()[0][2]);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsertObjectField() throws Exception {

        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("line 1:33: mismatched input '[' expecting ')'");

        setUpObjectTable();
        execute("insert into test12 (object_field['size']) values (127)");

    }

    @Test
    public void testInvalidInsertIntoObject() throws Exception {

        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("Validation failed for object_field.created: Invalid timestamp");

        setUpObjectTable();
        execute("insert into test12 (object_field, strict_field) values (?,?)", new Object[]{
                new HashMap<String, Object>(){{
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

        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("invalid byte literal 129");

        setUpSimple();
        execute("delete from t1 where byte_field=129");
    }

    @Test
    public void testInvalidWhereInWhereClause() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("invalid type in IN LIST 'string', expected 'byte'");

        setUpSimple();
        execute("update t1 set byte_field=0 where byte_field in ('a')");
    }

    @Test
    public void testSetUpdate() throws Exception {
        setUpSimple();

        execute("insert into t1 (id, byte_field, short_field, integer_field, long_field, " +
                "float_field, double_field, boolean_field, string_field, timestamp_field," +
                "object_field) values (?,?,?,?,?,?,?,?,?,?,?)", new Object[]{
                    0, 0, 0, 0, 0, 0.0, 1.0, false, "", "1970-01-01", new HashMap<String, Object>(){{ put("inner", "1970-01-01"); }}
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
                "ip_field=?" +
                "where id=0", new Object[]{
                    Byte.MAX_VALUE, Short.MIN_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE,
                    1.0, Math.PI, true, "a string", "2013-11-20",
                    new HashMap<String, Object>() {{put("inner", "2013-11-20");}}, "127.0.0.1"
        });
        refresh();

        SQLResponse response = execute("select id, byte_field, short_field, integer_field, long_field," +
                "float_field, double_field, boolean_field, string_field, timestamp_field," +
                "object_field, ip_field from t1 where id=0");
        assertEquals(1, response.rowCount());
        assertEquals(0, response.rows()[0][0]);
        assertEquals(127, response.rows()[0][1]);
        assertEquals(-32768, response.rows()[0][2]);
        assertEquals(0x7fffffff, response.rows()[0][3]);
        assertEquals(0x8000000000000000L, response.rows()[0][4]);
        assertEquals(1.0, response.rows()[0][5]);
        assertEquals(Math.PI, response.rows()[0][6]);
        assertEquals(true, response.rows()[0][7]);
        assertEquals("a string", response.rows()[0][8]);
        assertEquals(1384905600000L, response.rows()[0][9]);
        assertEquals(new HashMap<String, Object>() {{ put("inner", 1384905600000L); }}, response.rows()[0][10]);
        assertEquals("127.0.0.1", response.rows()[0][11]);
    }

    @Test
    public void testGetRequestMapping() throws Exception {
        setUpSimple();
        execute("insert into t1 (id, string_field, boolean_field, byte_field, short_field, integer_field," +
                "long_field, float_field, double_field, object_field," +
                "timestamp_field, ip_field) values " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", new Object[]{
                0, "Blabla", true, 120, 1000, 1200000,
                120000000000L, 1.4, 3.456789, new HashMap<String, Object>(){{put("inner", "1970-01-01");}},
                "1970-01-01", "127.0.0.1"
        });
        refresh();
        SQLResponse getResponse = execute("select * from t1 where id=0");
        SQLResponse searchResponse = execute("select * from t1 limit 1");
        for (int i=0; i < getResponse.rows()[0].length; i++) {
            assertThat(getResponse.rows()[0][i], is(searchResponse.rows()[0][i]));
        }
    }

    @Test
    public void testInsertNewColumn() throws Exception {
        setUpSimple();
        execute("insert into t1 (id, new_col) values (?,?)", new Object[]{0, "1970-01-01"});
        refresh();
        SQLResponse response = execute("select id, new_col from t1 where id=0");
        assertEquals(0, response.rows()[0][1]);
    }

    @Test
    public void testInsertNewObjectColumn() throws Exception {
        setUpSimple();
        execute("insert into t1 (id, new_col) values (?,?)", new Object[]{
                0,
                new HashMap<String, Object>(){{
                    put("a_date", "1970-01-01");
                    put("an_int", 127);
                    put("a_long", Long.MAX_VALUE);
                    put("a_boolean", true);
                }}
        });
        refresh();

        SQLResponse response = execute("select id, new_col from t1 where id=0");
        @SuppressWarnings("unchecked")
        Map<String, Object> mapped = (Map<String, Object>)response.rows()[0][1];
        assertEquals(0, mapped.get("a_date"));
        assertEquals(127, mapped.get("an_int"));
        assertEquals(0x7fffffffffffffffL, mapped.get("a_long"));
        assertEquals(true, mapped.get("a_boolean"));
    }

    @Test
    public void testInsertNewColumnToObject() throws Exception {
        setUpObjectTable();
        Map<String, Object> objectContent = new HashMap<String, Object>(){{
            put("new_col", "a string");
            put("another_new_col", "1970-01-01T00:00:00");
        }};
        execute("insert into test12 (object_field) values (?)",
                new Object[]{objectContent});
        refresh();
        SQLResponse response = execute("select object_field from test12");
        assertEquals(1, response.rowCount());
        @SuppressWarnings("unchecked")
        Map<String, Object> selectedObject = (Map<String, Object>)response.rows()[0][0];

        assertThat((String)selectedObject.get("new_col"), is("a string"));
        assertEquals(0, selectedObject.get("another_new_col"));
    }

    @Test
    public void testInsertNewColumnToStrictObject() throws Exception {

        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column 'strict_field.another_new_col' unknown");

        setUpObjectTable();
        Map<String, Object> strictContent = new HashMap<String, Object>(){{
            put("new_col", "a string");
            put("another_new_col", "1970-01-01T00:00:00");
        }};
        execute("insert into test12 (strict_field) values (?)",
                new Object[]{strictContent});
    }

    @Test
    public void testInsertNewColumnToIgnoredObject() throws Exception {

        setUpObjectTable();
        Map<String, Object> notDynamicContent = new HashMap<String, Object>(){{
            put("new_col", "a string");
            put("another_new_col", "1970-01-01T00:00:00");
        }};
        execute("insert into test12 (no_dynamic_field) values (?)",
                new Object[]{notDynamicContent});
        refresh();
        SQLResponse response = execute("select no_dynamic_field from test12");
        assertEquals(1, response.rowCount());
        @SuppressWarnings("unchecked")
        Map<String, Object> selectedNoDynamic = (Map<String, Object>)response.rows()[0][0];
        // no mapping applied
        assertThat((String)selectedNoDynamic.get("new_col"), is("a string"));
        assertThat((String)selectedNoDynamic.get("another_new_col"), is("1970-01-01T00:00:00"));
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
    public void testUnknownTypesSelectGlobalAggregate() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("unknown function: any(null)");

        this.setup.setUpObjectMappingWithUnknownTypes();
        execute("select any(location) from ut");
    }

    @Test
    public void testUnknownTypesSelectGroupBy() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("unknown column 'location' not allowed in GROUP BY");

        this.setup.setUpObjectMappingWithUnknownTypes();
        execute("select count(*) from ut group by location");
    }

}
