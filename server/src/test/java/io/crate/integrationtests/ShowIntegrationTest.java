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
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.util.Locale;

import org.assertj.core.api.AbstractStringAssert;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.Asserts;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;

@UseRandomizedSchema(random = false)
public class ShowIntegrationTest extends IntegTestCase {

    @Test
    public void testShowCrateSystemTable() throws Exception {
        Asserts.assertSQLError(() -> execute("show create table sys.shards"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining("The relation \"sys.shards\" doesn't support or allow SHOW CREATE operations");
    }

    @Test
    public void testShowCreateBlobTable() throws Exception {
        execute("create blob table table_blob");
        Asserts.assertSQLError(() -> execute("show create table blob.table_blob"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining("The relation \"blob.table_blob\" doesn't support or allow SHOW CREATE operations");
    }

    @Test
    public void testShowCrateTableSimple() {
        String expected = "CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
                          "   \"col_bool\" BOOLEAN,\n" +
                          "   \"col_byte\" BYTE,\n" +
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
        assertFirstRow().startsWith(expected);
        execute("show create table doc.test");
        assertFirstRow().startsWith(expected);
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
        assertFirstRow().startsWith(
            "CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
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
        assertFirstRow().startsWith(expected);
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
        assertFirstRow().startsWith(
            "CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
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
        assertFirstRow()
            .startsWith(
                "CREATE TABLE IF NOT EXISTS \"doc\".\"test\" (\n" +
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
        assertFirstRow().startsWith(
            "CREATE TABLE IF NOT EXISTS \"doc\".\"test_pk_single\" (\n" +
            "   \"id\" INTEGER NOT NULL,\n" +
            "   \"name\" TEXT,\n" +
            "   PRIMARY KEY (\"id\")\n" +
            ")\n" +
            "CLUSTERED BY (\"id\") INTO 8 SHARDS\n" +
            "WITH (\n");

        execute("create table test_pk_multi (" +
                " id integer," +
                " col_z string constraint c_1 primary key," +
                " col_a string constraint c_1 primary key" +
                ") clustered into 8 shards");
        execute("show create table test_pk_multi");
        assertFirstRow().startsWith(
            "CREATE TABLE IF NOT EXISTS \"doc\".\"test_pk_multi\" (\n" +
            "   \"id\" INTEGER,\n" +
            "   \"col_z\" TEXT NOT NULL,\n" +
            "   \"col_a\" TEXT NOT NULL,\n" +
            "   CONSTRAINT c_1 PRIMARY KEY (\"col_z\", \"col_a\")\n" +
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
        assertFirstRow().startsWith(
            """
            CREATE TABLE IF NOT EXISTS "doc"."test_generated_column" (
               "day1" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', "ts"),
               "day2" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', "ts") INDEX OFF,
               "day3" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', "ts"),
               "day4" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', "ts"),
               "col1" TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS "ts" + CAST(1 AS bigint),
               "col2" TEXT GENERATED ALWAYS AS "ts" + CAST(1 AS bigint),
               "col3" TEXT GENERATED ALWAYS AS "ts" + CAST(1 AS bigint),
               "name" TEXT GENERATED ALWAYS AS concat("user"['name'], 'foo'),
               "ts" TIMESTAMP WITH TIME ZONE,
               "user" OBJECT(DYNAMIC) AS (
                  "name" TEXT
               )
            """
        );
    }

    @Test
    public void testWeirdIdentifiers() throws Exception {
        execute("CREATE TABLE with_quote (\"\"\"\" string) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();

        execute("SHOW CREATE TABLE with_quote");
        assertFirstRow().startsWith("CREATE TABLE IF NOT EXISTS \"doc\".\"with_quote\" (\n" +
                                    "   \"\"\"\" TEXT\n" +
                                    ")\n" +
                                    "CLUSTERED INTO 1 SHARDS");

    }

    @Test
    public void testShowSchemas() throws Exception {
        execute("create table my_s1.my_table (id long) clustered into 1 shards with (number_of_replicas='0')");
        execute("create table my_s2.my_table (id long) clustered into 1 shards with (number_of_replicas='0')");
        execute("show schemas like 'my_%'");
        assertThat(response).hasRows("my_s1",
                                     "my_s2");
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
        assertThat(response).hasRows(
            "column11| integer",
               "column12| integer",
               "column13| bigint",
               "column21| integer",
               "column22| text",
               "column31| integer");

        execute("show columns in my_table1 like '%2'");
        assertThat(response)
            .hasRows("column12| integer",
                     "column22| text");

        execute("show columns from my_table1 where column_name = 'column12'");
        assertThat(response).hasRows("column12| integer");

        execute("show columns in my_table1 from my_s1 where data_type = 'bigint'");
        assertThat(response).hasRows("col22| bigint");

        execute("show columns in my_table1 from my_s1 like 'col1%'");
        assertThat(response).hasRows(
            "col11| timestamp with time zone",
               "col12| integer",
               "col13| integer");

        execute("show columns from my_table1 in my_s1 like '%1'");
        assertThat(response).hasRows(
            "col11| timestamp with time zone",
            "col31| integer");
    }

    @Test
    public void testShowTable() throws Exception {
        String tableName = "test";
        String schemaName = "my";
        execute(String.format(Locale.ENGLISH, "create table %s.%s (id long, name string)", schemaName, tableName));
        execute("create table foo (id long, name string)");

        execute("show tables");
        assertThat(response).hasRows("foo", "test");

        execute(String.format(Locale.ENGLISH, "show tables from %s", schemaName));
        assertThat(response).hasRows("test");

        execute(String.format(Locale.ENGLISH, "show tables in %s", schemaName));
        assertThat(response).hasRows("test");

        execute(String.format(Locale.ENGLISH, "show tables from %s like 'hello'", schemaName));
        assertThat(response).hasRowCount(0L);

        execute(String.format(Locale.ENGLISH, "show tables from %s like '%%'", schemaName));
        assertThat(response).hasRows("test");

        execute("show tables like '%es%'");
        assertThat(response).hasRows("test");

        execute("show tables like '%'");
        assertThat(response).hasRows("foo", "test");

        execute(String.format(Locale.ENGLISH, "show tables where table_name = '%s'", tableName));
        assertThat(response).hasRows("test");

        execute("show tables where table_name like '%es%'");
        assertThat(response).hasRows("test");

        execute(String.format(Locale.ENGLISH, "show tables from %s where table_name like '%%es%%'", schemaName));
        assertThat(response).hasRows("test");

        execute(String.format(Locale.ENGLISH, "show tables in %s where table_name like '%%es%%'", schemaName));
        assertThat(response).hasRows("test");

        execute(String.format(Locale.ENGLISH, "show tables from %s where table_name = 'hello'", schemaName));
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testShowSearchPath() {
        execute("show search_path");
        assertThat(response).hasRows("doc");
    }

    @UseHashJoins(1)
    @Test
    public void testShowEnableHashJoin() {
        execute("show enable_hashjoin");
        assertThat(response).hasRows("true");
    }

    @Test
    public void testShowUnknownSetting() {
        Asserts.assertSQLError(() -> execute("show foo"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Unknown session setting name 'foo'.");
    }

    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    @UseJdbc(1)
    @Test
    public void testShowAll() {
        execute("show all");
        assertThat(response).hasRows(
            "application_name| PostgreSQL JDBC Driver| Optional application name. Can be set by a client to identify the application which created the connection",
            "datestyle| ISO| Display format for date and time values.",
            "enable_hashjoin| true| Considers using the Hash Join instead of the Nested Loop Join implementation.",
            "error_on_unknown_object_key| true| Raises or suppresses ObjectKeyUnknownException when querying nonexistent keys to dynamic objects.",
            "max_identifier_length| 255| Shows the maximum length of identifiers in bytes.",
            "max_index_keys| 32| Shows the maximum number of index keys.",
            "memory.operation_limit| 0| Memory limit in bytes for an individual operation. 0 by-passes the operation limit, relying entirely on the global circuit breaker limits",
            "optimizer_deduplicate_order| true| Indicates if the optimizer rule DeduplicateOrder is activated.",
            "optimizer_eliminate_cross_join| true| Indicates if the optimizer rule EliminateCrossJoin is activated.",
            "optimizer_equi_join_to_lookup_join| true| Indicates if the optimizer rule EquiJoinToLookupJoin is activated.",
            "optimizer_merge_aggregate_and_collect_to_count| true| Indicates if the optimizer rule MergeAggregateAndCollectToCount is activated.",
            "optimizer_merge_aggregate_rename_and_collect_to_count| true| Indicates if the optimizer rule MergeAggregateRenameAndCollectToCount is activated.",
            "optimizer_merge_filter_and_collect| true| Indicates if the optimizer rule MergeFilterAndCollect is activated.",
            "optimizer_merge_filter_and_foreign_collect| true| Indicates if the optimizer rule MergeFilterAndForeignCollect is activated.",
            "optimizer_merge_filters| true| Indicates if the optimizer rule MergeFilters is activated.",
            "optimizer_move_constant_join_conditions_beneath_join| true| Indicates if the optimizer rule MoveConstantJoinConditionsBeneathJoin is activated.",
            "optimizer_move_filter_beneath_correlated_join| true| Indicates if the optimizer rule MoveFilterBeneathCorrelatedJoin is activated.",
            "optimizer_move_filter_beneath_eval| true| Indicates if the optimizer rule MoveFilterBeneathEval is activated.",
            "optimizer_move_filter_beneath_group_by| true| Indicates if the optimizer rule MoveFilterBeneathGroupBy is activated.",
            "optimizer_move_filter_beneath_join| true| Indicates if the optimizer rule MoveFilterBeneathJoin is activated.",
            "optimizer_move_filter_beneath_order| true| Indicates if the optimizer rule MoveFilterBeneathOrder is activated.",
            "optimizer_move_filter_beneath_project_set| true| Indicates if the optimizer rule MoveFilterBeneathProjectSet is activated.",
            "optimizer_move_filter_beneath_rename| true| Indicates if the optimizer rule MoveFilterBeneathRename is activated.",
            "optimizer_move_filter_beneath_union| true| Indicates if the optimizer rule MoveFilterBeneathUnion is activated.",
            "optimizer_move_filter_beneath_window_agg| true| Indicates if the optimizer rule MoveFilterBeneathWindowAgg is activated.",
            "optimizer_move_limit_beneath_eval| true| Indicates if the optimizer rule MoveLimitBeneathEval is activated.",
            "optimizer_move_limit_beneath_rename| true| Indicates if the optimizer rule MoveLimitBeneathRename is activated.",
            "optimizer_move_order_beneath_eval| true| Indicates if the optimizer rule MoveOrderBeneathEval is activated.",
            "optimizer_move_order_beneath_nested_loop| true| Indicates if the optimizer rule MoveOrderBeneathNestedLoop is activated.",
            "optimizer_move_order_beneath_rename| true| Indicates if the optimizer rule MoveOrderBeneathRename is activated.",
            "optimizer_move_order_beneath_union| true| Indicates if the optimizer rule MoveOrderBeneathUnion is activated.",
            "optimizer_optimize_collect_where_clause_access| true| Indicates if the optimizer rule OptimizeCollectWhereClauseAccess is activated.",
            "optimizer_remove_redundant_eval| true| Indicates if the optimizer rule RemoveRedundantEval is activated.",
            "optimizer_reorder_hash_join| true| Indicates if the optimizer rule ReorderHashJoin is activated.",
            "optimizer_reorder_nested_loop_join| true| Indicates if the optimizer rule ReorderNestedLoopJoin is activated.",
            "optimizer_rewrite_filter_on_outer_join_to_inner_join| true| Indicates if the optimizer rule RewriteFilterOnOuterJoinToInnerJoin is activated.",
            "optimizer_rewrite_group_by_keys_limit_to_limit_distinct| true| Indicates if the optimizer rule RewriteGroupByKeysLimitToLimitDistinct is activated.",
            "optimizer_rewrite_to_query_then_fetch| true| Indicates if the optimizer rule RewriteToQueryThenFetch is activated.",
            "search_path| doc| Sets the schema search order.",
            "server_version| 14.0| Reports the emulated PostgreSQL version number",
            "server_version_num| 140000| Reports the emulated PostgreSQL version number",
            "standard_conforming_strings| on| Causes '...' strings to treat backslashes literally.",
            "statement_timeout| 0s| The maximum duration of any statement before it gets killed. Infinite/disabled if 0"
        );
    }

    private AbstractStringAssert<?> assertFirstRow() {
        assertThat(response).hasRowCount(1L);
        return assertThat((String) response.rows()[0][0]);
    }
}
