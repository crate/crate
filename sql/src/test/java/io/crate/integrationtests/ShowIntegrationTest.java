/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class ShowIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testShowCrateSystemTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table must be a doc table");
        execute("show create table sys.shards");
    }

    @Test
    public void testShowCreateBlobTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table must be a doc table");
        execute("create blob table table_blob");
        execute("show create table blob.table_blob");
    }

    @Test
    public void testShowCrateTableSimple() throws Exception {
        String expected = "CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                "   \"col_bool\" BOOLEAN,\n" +
                "   \"col_byte\" BYTE,\n" +
                "   \"col_double\" DOUBLE,\n" +
                "   \"col_float\" FLOAT,\n" +
                "   \"col_geo\" GEO_POINT,\n" +
                "   \"col_int\" INTEGER,\n" +
                "   \"col_long\" LONG,\n" +
                "   \"col_short\" SHORT,\n" +
                "   \"col_str\" STRING,\n" +
                "   \"col_ts\" TIMESTAMP\n" +
                ")\n";
        execute("create table test (" +
                " col_bool boolean," +
                " col_byte byte," +
                " col_short short," +
                " col_int integer," +
                " col_long long," +
                " col_float float," +
                " col_double double," +
                " col_str string," +
                " col_ts timestamp," +
                " col_geo geo_point" +
                ")");
        execute("show create table test");
        assertRow(expected);
        execute("show create table doc.test");
        assertRow(expected);
    }

    @Test
    public void testShowCreateTableNested() throws Exception {
        execute("create table test (" +
                " col_arr_str array(string)," +
                " col_arr_obj_a array(object)," +
                " col_arr_obj_b array(object(strict) as (id int))," +
                " col_obj_a object," +
                " col_obj_b object(dynamic) as (arr array(integer), obj object(strict) as (id int, name string))" +
                ")");
        execute("show create table test");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                "   \"col_arr_obj_a\" ARRAY(OBJECT (DYNAMIC)),\n" +
                "   \"col_arr_obj_b\" ARRAY(OBJECT (STRICT) AS (\n" +
                "      \"id\" INTEGER\n" +
                "   )),\n" +
                "   \"col_arr_str\" ARRAY(STRING),\n" +
                "   \"col_obj_a\" OBJECT (DYNAMIC),\n" +
                "   \"col_obj_b\" OBJECT (DYNAMIC) AS (\n" +
                "      \"arr\" ARRAY(INTEGER),\n" +
                "      \"obj\" OBJECT (STRICT) AS (\n" +
                "         \"id\" INTEGER,\n" +
                "         \"name\" STRING\n" +
                "      )\n" +
                "   )\n" +
                ")\n");

    }

    @Test
    public void testShowCreateCustomSchemaTable() throws Exception {
        execute("create table my.test (id long, name string) clustered into 2 shards");
        execute("show create table my.test");
        String expected = "CREATE TABLE IF NOT EXISTS \"my\".\"test\" (\n" +
                "   \"id\" LONG,\n" +
                "   \"name\" STRING\n" +
                ")\n" +
                "CLUSTERED INTO 2 SHARDS\n" +
                "WITH (";
        assertRow(expected);
    }

    @Test
    public void testShowCreateTableIndexes() throws Exception {
        execute("create table test (" +
                " col_a string index off," +
                " col_b string index using plain," +
                " col_c string index using fulltext," +
                " col_d string index using fulltext with (analyzer='english')," +
                " col_e string," +
                " col_f string," +
                " index index_ft using fulltext(\"col_e\",\"col_f\") with (analyzer='english')" +
                ") " +
                "clustered into 2 shards");
        execute("show create table test");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                "   \"col_a\" STRING INDEX OFF,\n" +
                "   \"col_b\" STRING,\n" +
                "   \"col_c\" STRING INDEX USING FULLTEXT WITH (\n" +
                "      analyzer = 'standard'\n" +
                "   ),\n" +
                "   \"col_d\" STRING INDEX USING FULLTEXT WITH (\n" +
                "      analyzer = 'english'\n" +
                "   ),\n" +
                "   \"col_e\" STRING,\n" +
                "   \"col_f\" STRING,\n" +
                "   INDEX \"index_ft\" USING FULLTEXT (\"col_e\", \"col_f\") WITH (\n" +
                "      analyzer = 'english'\n" +
                "   )\n" +
                ")\n" +
                "CLUSTERED INTO 2 SHARDS\n" +
                "WITH (");
    }

    @Test
    public void testShowCreateTablePartitioned() throws Exception {
        execute("create table test (" +
                " id long," +
                " date timestamp" +
                ") " +
                "partitioned by (\"date\")");
        execute("show create table test");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                "   \"date\" TIMESTAMP,\n" +
                "   \"id\" LONG\n" +
                ")\n" +
                "CLUSTERED INTO 5 SHARDS\n" +
                "PARTITIONED BY (\"date\")\n" +
                "WITH (");
    }

    @Test
    public void testShowCreateTableWithPK() throws Exception {
        execute("create table test_pk_single (" +
                " id integer primary key," +
                " name string" +
                ") clustered into 8 shards");
        execute("show create table test_pk_single");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test_pk_single\" (\n" +
                "   \"id\" INTEGER,\n" +
                "   \"name\" STRING,\n" +
                "   PRIMARY KEY (\"id\")\n" +
                ")\n" +
                "CLUSTERED BY (\"id\") INTO 8 SHARDS\n" +
                "WITH (\n");

        execute("create table test_pk_multi (" +
                " id integer," +
                " col_z string primary key," +
                " col_a string primary key" +
                ") clustered into 8 shards");
        execute("show create table test_pk_multi");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test_pk_multi\" (\n" +
                "   \"col_a\" STRING,\n" +
                "   \"col_z\" STRING,\n" +
                "   \"id\" INTEGER,\n" +
                "   PRIMARY KEY (\"col_z\", \"col_a\")\n" +
                ")\n" +
                "CLUSTERED INTO 8 SHARDS\n" +
                "WITH (\n");
    }

    private void assertRow(String expected) {
        assertEquals(1L, response.rowCount());
        try {
            assertTrue(((String) response.rows()[0][0]).startsWith(expected));
        } catch (Throwable e) {
            String msg = String.format("Row does not start with expected string:\n\nExpected: %s\nActual: %s\n", expected, response.rows()[0][0]);
            throw new AssertionError(msg);
        }
    }
}
