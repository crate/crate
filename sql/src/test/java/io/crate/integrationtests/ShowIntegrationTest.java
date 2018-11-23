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
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseRandomizedSchema;
import io.crate.testing.UseSemiJoins;
import org.junit.Test;

import java.util.Locale;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.core.Is.is;

@UseRandomizedSchema(random = false)
public class ShowIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testShowCrateSystemTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow SHOW CREATE " +
                                        "operations, as it is read-only.");
        execute("show create table sys.shards");
    }

    @Test
    public void testShowCreateBlobTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"blob.table_blob\" doesn't support or allow " +
                                        "SHOW CREATE operations.");
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
                "clustered into 4 shards " +
                "partitioned by (\"date\")");
        execute("show create table test");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                  "   \"date\" TIMESTAMP,\n" +
                  "   \"id\" LONG\n" +
                  ")\n" +
                  "CLUSTERED INTO 4 SHARDS\n" +
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

    @Test
    public void testShowCreateTableWithGeneratedColumn() throws Exception {
        execute("create table test_generated_column (" +
                " day1 AS date_trunc('day', ts)," +
                " day2 AS (date_trunc('day', ts)) INDEX OFF," +
                " day3 GENERATED ALWAYS AS date_trunc('day', ts)," +
                " day4 GENERATED ALWAYS AS (date_trunc('day', ts))," +
                " col1 AS ts + 1," +
                " col2 string GENERATED ALWAYS AS ts + 1," +
                " col3 string GENERATED ALWAYS AS (ts + 1)," +
                " name AS concat(\"user\"['name'], 'foo')," +
                " ts timestamp," +
                " \"user\" object AS (name string)" +
                ")");
        execute("show create table test_generated_column");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test_generated_column\" (\n" +
                  "   \"col1\" LONG GENERATED ALWAYS AS CAST(\"ts\" AS long) + 1,\n" +
                  "   \"col2\" STRING GENERATED ALWAYS AS CAST((CAST(\"ts\" AS long) + 1) AS string),\n" +
                  "   \"col3\" STRING GENERATED ALWAYS AS CAST((CAST(\"ts\" AS long) + 1) AS string),\n" +
                  "   \"day1\" TIMESTAMP GENERATED ALWAYS AS date_trunc('day', \"ts\"),\n" +
                  "   \"day2\" TIMESTAMP GENERATED ALWAYS AS date_trunc('day', \"ts\") INDEX OFF,\n" +
                  "   \"day3\" TIMESTAMP GENERATED ALWAYS AS date_trunc('day', \"ts\"),\n" +
                  "   \"day4\" TIMESTAMP GENERATED ALWAYS AS date_trunc('day', \"ts\"),\n" +
                  "   \"name\" STRING GENERATED ALWAYS AS concat(\"user\"['name'], 'foo'),\n" +
                  "   \"ts\" TIMESTAMP,\n" +
                  "   \"user\" OBJECT (DYNAMIC) AS (\n" +
                  "      \"name\" STRING\n" +
                  "   )\n" +
                  ")");
    }

    @Test
    public void testWeirdIdentifiers() throws Exception {
        execute("CREATE TABLE with_quote (\"\"\"\" string) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();

        execute("SHOW CREATE TABLE with_quote");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"with_quote\" (\n" +
                  "   \"\"\"\" STRING\n" +
                  ")\n" +
                  "CLUSTERED INTO 1 SHARDS");

    }

    private void assertRow(String expected) {
        assertEquals(1L, response.rowCount());
        try {
            assertTrue(((String) response.rows()[0][0]).startsWith(expected));
        } catch (Throwable e) {
            String msg = String.format(Locale.ENGLISH, "Row does not start with expected string:%n%n" +
                                                       "Expected: %s%nActual: %s%n", expected, response.rows()[0][0]);
            throw new AssertionError(msg);
        }
    }

    @Test
    public void testShowSchemas() throws Exception {
        execute("create table my_s1.my_table (id long) clustered into 1 shards with (number_of_replicas='0')");
        execute("create table my_s2.my_table (id long) clustered into 1 shards with (number_of_replicas='0')");
        execute("show schemas like 'my_%'");
        assertThat(printedTable(response.rows()), is("my_s1\n" +
                                                                    "my_s2\n"));
    }

    @Test
    public void testShowColumns() throws Exception {
        execute("create table my_table1 (" +
                "column11 integer, " +
                "column12 integer, " +
                "column13 long, " +
                "column21 integer, " +
                "column22 string, " +
                "column31 integer)"
        );

        execute("create table my_s1.my_table1 (" +
                "col11 timestamp, " +
                "col12 integer, " +
                "col13 integer, " +
                "col22 long, " +
                "col31 integer)"
        );

        execute("show columns from my_table1");
        assertThat(printedTable(response.rows()),
            is("column11| integer\n" +
               "column12| integer\n" +
               "column13| long\n" +
               "column21| integer\n" +
               "column22| string\n" +
               "column31| integer\n"));

        execute("show columns in my_table1 like '%2'");
        assertThat(printedTable(response.rows()),
            is("column12| integer\n" +
               "column22| string\n"));

        execute("show columns from my_table1 where column_name = 'column12'");
        assertThat(printedTable(response.rows()), is("column12| integer\n"));

        execute("show columns in my_table1 from my_s1 where data_type = 'long'");
        assertThat(printedTable(response.rows()), is("col22| long\n"));

        execute("show columns in my_table1 from my_s1 like 'col1%'");
        assertThat(printedTable(response.rows()),
            is("col11| timestamp\n" +
               "col12| integer\n" +
               "col13| integer\n"));

        execute("show columns from my_table1 in my_s1 like '%1'");
        assertThat(printedTable(response.rows()),
            is("col11| timestamp\n" +
               "col31| integer\n"));

    }

    @Test
    public void testShowTable() throws Exception {
        String tableName = "test";
        String schemaName = "my";
        execute(String.format(Locale.ENGLISH, "create table %s.%s (id long, name string)", schemaName, tableName));
        execute("create table foo (id long, name string)");

        execute("show tables");
        assertThat(printedTable(response.rows()), is("foo\n" +
                                                                    "test\n"));

        execute(String.format(Locale.ENGLISH, "show tables from %s", schemaName));
        assertThat(printedTable(response.rows()), is("test\n"));

        execute(String.format(Locale.ENGLISH, "show tables in %s", schemaName));
        assertThat(printedTable(response.rows()), is("test\n"));

        execute(String.format(Locale.ENGLISH, "show tables from %s like 'hello'", schemaName));
        assertEquals(0, response.rowCount());

        execute(String.format(Locale.ENGLISH, "show tables from %s like '%%'", schemaName));
        assertThat(printedTable(response.rows()), is("test\n"));

        execute("show tables like '%es%'");
        assertThat(printedTable(response.rows()), is("test\n"));

        execute("show tables like '%'");
        assertThat(printedTable(response.rows()), is("foo\n" +
                                                                    "test\n"));

        execute(String.format(Locale.ENGLISH, "show tables where table_name = '%s'", tableName));
        assertThat(printedTable(response.rows()), is("test\n"));

        execute("show tables where table_name like '%es%'");
        assertThat(printedTable(response.rows()), is("test\n"));

        execute(String.format(Locale.ENGLISH, "show tables from %s where table_name like '%%es%%'", schemaName));
        assertThat(printedTable(response.rows()), is("test\n"));

        execute(String.format(Locale.ENGLISH, "show tables in %s where table_name like '%%es%%'", schemaName));
        assertThat(printedTable(response.rows()), is("test\n"));

        execute(String.format(Locale.ENGLISH, "show tables from %s where table_name = 'hello'", schemaName));
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testShowSearchPath() {
        execute("show search_path");
        assertThat(printedTable(response.rows()), is("[pg_catalog, doc]\n"));
    }

    @UseHashJoins(1)
    @Test
    public void testShowEnableHashJoin() {
        execute("show enable_hashjoin");
        assertThat(printedTable(response.rows()), is("true\n"));
    }

    @UseSemiJoins(1)
    @Test
    public void testShowEnableSemiJoin() {
        execute("show enable_semijoin");
        assertThat(printedTable(response.rows()), is("true\n"));
    }
}
