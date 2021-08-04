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

import io.crate.testing.UseHashJoins;
import io.crate.testing.UseRandomizedSchema;
import org.junit.Test;

import java.util.Locale;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;

@UseRandomizedSchema(random = false)
public class ShowIntegrationTest extends SQLIntegrationTestCase {

    @Test
    public void testShowCrateSystemTable() throws Exception {
        assertThrowsMatches(() -> execute("show create table sys.shards"),
                     isSQLError(is("The relation \"sys.shards\" doesn't support or allow SHOW CREATE operations, as it is read-only."),
                         INTERNAL_ERROR, BAD_REQUEST, 4007));
    }

    @Test
    public void testShowCreateBlobTable() throws Exception {
        execute("create blob table table_blob");
        assertThrowsMatches(() -> execute("show create table blob.table_blob"),
                     isSQLError(is("The relation \"blob.table_blob\" doesn't support or allow SHOW CREATE operations."),
                         INTERNAL_ERROR, BAD_REQUEST, 4007));
    }

    @Test
    public void testShowCrateTableSimple() {
        String expected = "CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                          "   \"col_bool\" BOOLEAN,\n" +
                          "   \"col_byte\" CHAR,\n" +
                          "   \"col_short\" SMALLINT,\n" +
                          "   \"col_int\" INTEGER,\n" +
                          "   \"col_long\" BIGINT,\n" +
                          "   \"col_float\" REAL,\n" +
                          "   \"col_double\" DOUBLE PRECISION,\n" +
                          "   \"col_str\" TEXT,\n" +
                          "   \"col_ts\" TIMESTAMP WITHOUT TIME ZONE,\n" +
                          "   \"col_ts_z\" TIMESTAMP WITH TIME ZONE,\n" +
                          "   \"col_geo\" GEO_POINT\n" +
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
                " col_ts timestamp without time zone," +
                " col_ts_z timestamp with time zone," +
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
                  "   \"col_arr_str\" ARRAY(TEXT),\n" +
                  "   \"col_arr_obj_a\" ARRAY(OBJECT(DYNAMIC)),\n" +
                  "   \"col_arr_obj_b\" ARRAY(OBJECT(STRICT) AS (\n" +
                  "      \"id\" INTEGER\n" +
                  "   )),\n" +
                  "   \"col_obj_a\" OBJECT(DYNAMIC),\n" +
                  "   \"col_obj_b\" OBJECT(DYNAMIC) AS (\n" +
                  "      \"arr\" ARRAY(INTEGER),\n" +
                  "      \"obj\" OBJECT(STRICT) AS (\n" +
                  "         \"id\" INTEGER,\n" +
                  "         \"name\" TEXT\n" +
                  "      )\n" +
                  "   )\n" +
                  ")\n");

    }

    @Test
    public void testShowCreateCustomSchemaTable() {
        execute("create table my.test (id long, name string) clustered into 2 shards");
        execute("show create table my.test");
        String expected = "CREATE TABLE IF NOT EXISTS \"my\".\"test\" (\n" +
                          "   \"id\" BIGINT,\n" +
                          "   \"name\" TEXT\n" +
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
                " col_d string index using fulltext with (analyzer='stop')," +
                " col_e string," +
                " col_f string," +
                " index index_ft using fulltext(\"col_e\",\"col_f\") with (analyzer='stop')" +
                ") " +
                "clustered into 2 shards");
        execute("show create table test");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                  "   \"col_a\" TEXT INDEX OFF,\n" +
                  "   \"col_b\" TEXT,\n" +
                  "   \"col_c\" TEXT INDEX USING FULLTEXT WITH (\n" +
                  "      analyzer = 'standard'\n" +
                  "   ),\n" +
                  "   \"col_d\" TEXT INDEX USING FULLTEXT WITH (\n" +
                  "      analyzer = 'stop'\n" +
                  "   ),\n" +
                  "   \"col_e\" TEXT,\n" +
                  "   \"col_f\" TEXT,\n" +
                  "   INDEX \"index_ft\" USING FULLTEXT (\"col_e\", \"col_f\") WITH (\n" +
                  "      analyzer = 'stop'\n" +
                  "   )\n" +
                  ")\n" +
                  "CLUSTERED INTO 2 SHARDS\n" +
                  "WITH (");
    }

    @Test
    public void testShowCreateTablePartitioned() {
        execute("create table test (" +
                " id long," +
                " date timestamp with time zone" +
                ") " +
                "clustered into 4 shards " +
                "partitioned by (\"date\")");
        execute("show create table test");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                  "   \"id\" BIGINT,\n" +
                  "   \"date\" TIMESTAMP WITH TIME ZONE\n" +
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
                  "   \"name\" TEXT,\n" +
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
                  "   \"id\" INTEGER,\n" +
                  "   \"col_z\" TEXT,\n" +
                  "   \"col_a\" TEXT,\n" +
                  "   PRIMARY KEY (\"col_z\", \"col_a\")\n" +
                  ")\n" +
                  "CLUSTERED INTO 8 SHARDS\n" +
                  "WITH (\n");
    }

    @Test
    public void testShowCreateTableWithGeneratedColumn() {
        execute("create table test_generated_column (" +
                " day1 AS date_trunc('day', ts)," +
                " day2 AS (date_trunc('day', ts)) INDEX OFF," +
                " day3 GENERATED ALWAYS AS date_trunc('day', ts)," +
                " day4 GENERATED ALWAYS AS (date_trunc('day', ts))," +
                " col1 AS ts + 1," +
                " col2 string GENERATED ALWAYS AS ts + 1," +
                " col3 string GENERATED ALWAYS AS (ts + 1)," +
                " name AS concat(\"user\"['name'], 'foo')," +
                " ts timestamp with time zone," +
                " \"user\" object AS (name string)" +
                ")");
        execute("show create table test_generated_column");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"test_generated_column\" (\n" +
                  "   \"day1\" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', \"ts\"),\n" +
                  "   \"day2\" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', \"ts\") INDEX OFF,\n" +
                  "   \"day3\" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', \"ts\"),\n" +
                  "   \"day4\" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', \"ts\"),\n" +
                  "   \"col1\" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS \"ts\" + CAST(1 AS bigint),\n" +
                  "   \"col2\" TEXT GENERATED ALWAYS AS _cast((\"ts\" + CAST(1 AS bigint)), 'text'),\n" +
                  "   \"col3\" TEXT GENERATED ALWAYS AS _cast((\"ts\" + CAST(1 AS bigint)), 'text'),\n" +
                  "   \"name\" TEXT GENERATED ALWAYS AS concat(\"user\"['name'], 'foo'),\n" +
                  "   \"ts\" TIMESTAMP WITH TIME ZONE,\n" +
                  "   \"user\" OBJECT(DYNAMIC) AS (\n" +
                  "      \"name\" TEXT\n" +
                  "   )\n" +
                  ")");
    }

    @Test
    public void testWeirdIdentifiers() throws Exception {
        execute("CREATE TABLE with_quote (\"\"\"\" string) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();

        execute("SHOW CREATE TABLE with_quote");
        assertRow("CREATE TABLE IF NOT EXISTS \"doc\".\"with_quote\" (\n" +
                  "   \"\"\"\" TEXT\n" +
                  ")\n" +
                  "CLUSTERED INTO 1 SHARDS");

    }

    private void assertRow(String expected) {
        assertEquals(1L, response.rowCount());
        try {
            assertThat(((String) response.rows()[0][0]), startsWith(expected));
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
    public void testShowColumns() {
        execute("create table my_table1 (" +
                "column11 integer, " +
                "column12 integer, " +
                "column13 long, " +
                "column21 integer, " +
                "column22 string, " +
                "column31 integer)"
        );

        execute("create table my_s1.my_table1 (" +
                "col11 timestamp with time zone, " +
                "col12 integer, " +
                "col13 integer, " +
                "col22 long, " +
                "col31 integer)"
        );

        execute("show columns from my_table1");
        assertThat(printedTable(response.rows()),
            is("column11| integer\n" +
               "column12| integer\n" +
               "column13| bigint\n" +
               "column21| integer\n" +
               "column22| text\n" +
               "column31| integer\n"));

        execute("show columns in my_table1 like '%2'");
        assertThat(printedTable(response.rows()),
            is("column12| integer\n" +
               "column22| text\n"));

        execute("show columns from my_table1 where column_name = 'column12'");
        assertThat(printedTable(response.rows()), is("column12| integer\n"));

        execute("show columns in my_table1 from my_s1 where data_type = 'bigint'");
        assertThat(printedTable(response.rows()), is("col22| bigint\n"));

        execute("show columns in my_table1 from my_s1 like 'col1%'");
        assertThat(printedTable(response.rows()),
            is("col11| timestamp with time zone\n" +
               "col12| integer\n" +
               "col13| integer\n"));

        execute("show columns from my_table1 in my_s1 like '%1'");
        assertThat(printedTable(response.rows()),
            is("col11| timestamp with time zone\n" +
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
        assertThat(printedTable(response.rows()), is("pg_catalog, doc\n"));
    }

    @UseHashJoins(1)
    @Test
    public void testShowEnableHashJoin() {
        execute("show enable_hashjoin");
        assertThat(printedTable(response.rows()), is("true\n"));
    }

    @Test
    public void testShowUnknownSetting() {
        assertThrowsMatches(() -> execute("show foo"),
                     isSQLError(is("Unknown session setting name 'foo'."),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));
    }

    @UseHashJoins(1)
    @Test
    public void testShowAll() {
        execute("show all");
        assertThat(printedTable(response.rows()), is(
            "enable_hashjoin| true| Considers using the Hash Join instead of the Nested Loop Join implementation.\n" +
            "error_on_unknown_object_key| true| Raises or suppresses ObjectKeyUnknownException when querying nonexistent keys to dynamic objects.\n" +
            "max_index_keys| 32| Shows the maximum number of index keys.\n" +
            "optimizer_deduplicate_order| true| Indicates if the optimizer rule DeduplicateOrder is activated.\n" +
            "optimizer_merge_aggregate_and_collect_to_count| true| Indicates if the optimizer rule MergeAggregateAndCollectToCount is activated.\n" +
            "optimizer_merge_aggregate_rename_and_collect_to_count| true| Indicates if the optimizer rule MergeAggregateRenameAndCollectToCount is activated.\n" +
            "optimizer_merge_filter_and_collect| true| Indicates if the optimizer rule MergeFilterAndCollect is activated.\n" +
            "optimizer_merge_filters| true| Indicates if the optimizer rule MergeFilters is activated.\n" +
            "optimizer_move_filter_beneath_fetch_or_eval| true| Indicates if the optimizer rule MoveFilterBeneathFetchOrEval is activated.\n" +
            "optimizer_move_filter_beneath_group_by| true| Indicates if the optimizer rule MoveFilterBeneathGroupBy is activated.\n" +
            "optimizer_move_filter_beneath_hash_join| true| Indicates if the optimizer rule MoveFilterBeneathHashJoin is activated.\n" +
            "optimizer_move_filter_beneath_nested_loop| true| Indicates if the optimizer rule MoveFilterBeneathNestedLoop is activated.\n" +
            "optimizer_move_filter_beneath_order| true| Indicates if the optimizer rule MoveFilterBeneathOrder is activated.\n" +
            "optimizer_move_filter_beneath_project_set| true| Indicates if the optimizer rule MoveFilterBeneathProjectSet is activated.\n" +
            "optimizer_move_filter_beneath_rename| true| Indicates if the optimizer rule MoveFilterBeneathRename is activated.\n" +
            "optimizer_move_filter_beneath_union| true| Indicates if the optimizer rule MoveFilterBeneathUnion is activated.\n" +
            "optimizer_move_filter_beneath_window_agg| true| Indicates if the optimizer rule MoveFilterBeneathWindowAgg is activated.\n" +
            "optimizer_move_order_beneath_fetch_or_eval| true| Indicates if the optimizer rule MoveOrderBeneathFetchOrEval is activated.\n" +
            "optimizer_move_order_beneath_nested_loop| true| Indicates if the optimizer rule MoveOrderBeneathNestedLoop is activated.\n" +
            "optimizer_move_order_beneath_rename| true| Indicates if the optimizer rule MoveOrderBeneathRename is activated.\n" +
            "optimizer_move_order_beneath_union| true| Indicates if the optimizer rule MoveOrderBeneathUnion is activated.\n" +
            "optimizer_remove_redundant_fetch_or_eval| true| Indicates if the optimizer rule RemoveRedundantFetchOrEval is activated.\n" +
            "optimizer_rewrite_collect_to_get| true| Indicates if the optimizer rule RewriteCollectToGet is activated.\n" +
            "optimizer_rewrite_filter_on_outer_join_to_inner_join| true| Indicates if the optimizer rule RewriteFilterOnOuterJoinToInnerJoin is activated.\n" +
            "optimizer_rewrite_group_by_keys_limit_to_top_n_distinct| true| Indicates if the optimizer rule RewriteGroupByKeysLimitToTopNDistinct is activated.\n" +
            "optimizer_rewrite_insert_from_sub_query_to_insert_from_values| true| Indicates if the optimizer rule RewriteInsertFromSubQueryToInsertFromValues is activated.\n" +
            "optimizer_rewrite_to_query_then_fetch| true| Indicates if the optimizer rule RewriteToQueryThenFetch is activated.\n" +
            "search_path| pg_catalog, doc| Sets the schema search order.\n" +
            "server_version| 10.5| Reports the emulated PostgreSQL version number\n" +
            "server_version_num| 100500| Reports the emulated PostgreSQL version number\n")
        );
    }
     }
