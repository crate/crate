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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.is;

public class PgCatalogITest extends SQLIntegrationTestCase {

    @Before
    public void createRelations() {
        execute("create table doc.t1 (id int primary key, s string, o object as (a int))");
        execute("create view doc.v1 as select id from doc.t1");
    }

    @After
    public void dropView() {
        execute("drop view doc.v1");
    }

    @Test
    public void testPgClassTable() {
        execute("select * from pg_catalog.pg_class where relname in ('t1', 'v1', 'tables', 'nodes') order by relname");
        assertThat(printedTable(response.rows()), is(
            "-1420189195| NULL| 0| 0| 0| 0| false| 0| false| false| true| false| false| false| false| true| false| r| 0| nodes| -458336339| 17| 0| NULL| 0| 0| NULL| p| p| false| 0| 0| -1.0| 0\n" +
            "728874843| NULL| 0| 0| 0| 0| false| 0| false| false| true| false| false| false| false| true| false| r| 0| t1| -2048275947| 3| 0| NULL| 0| 0| NULL| p| p| false| 0| 0| -1.0| 0\n" +
            "-1689918046| NULL| 0| 0| 0| 0| false| 0| false| false| true| false| false| false| false| true| false| r| 0| tables| 204690627| 16| 0| NULL| 0| 0| NULL| p| p| false| 0| 0| -1.0| 0\n" +
            "845171032| NULL| 0| 0| 0| 0| false| 0| false| false| false| false| false| false| false| true| false| v| 0| v1| -2048275947| 1| 0| NULL| 0| 0| NULL| p| p| false| 0| 0| -1.0| 0\n"));
    }

    @Test
    public void testPgNamespaceTable() {
        execute("select * from pg_catalog.pg_namespace order by nspname");
        assertThat(printedTable(response.rows()), is(
            "NULL| blob| 0| -508866815\n" +
            "NULL| doc| 0| -2048275947\n" +
            "NULL| information_schema| 0| 204690627\n" +
            "NULL| pg_catalog| 0| -68025646\n" +
            "NULL| sys| 0| -458336339\n"));
    }

    @Test
    public void testPgAttributeTable() {
        execute("select a.* from pg_catalog.pg_attribute as a join pg_catalog.pg_class as c on a.attrelid = c.oid where c.relname = 't1' order by a.attnum");
        assertThat(printedTable(response.rows()), is(
            "NULL| NULL| false| -1| 0| NULL| false| | 0| false| true| 4| id| 0| false| 1| NULL| 728874843| 0| NULL| 23| -1\n" +
            "NULL| NULL| false| -1| 0| NULL| false| | 0| false| true| -1| s| 0| false| 2| NULL| 728874843| 0| NULL| 1043| -1\n" +
            "NULL| NULL| false| -1| 0| NULL| false| | 0| false| true| -1| o| 0| false| 3| NULL| 728874843| 0| NULL| 114| -1\n" +
            "NULL| NULL| false| -1| 0| NULL| false| | 0| false| true| 4| o['a']| 0| false| 4| NULL| 728874843| 0| NULL| 23| -1\n"));
    }

    @Test
    public void testPgIndexTable() {
        execute("select count(*) from pg_catalog.pg_index");
        assertThat(printedTable(response.rows()), is("23\n"));
    }

    @Test
    public void testPgConstraintTable() {
        execute("select cn.* from pg_constraint cn, pg_class c where cn.conrelid = c.oid and c.relname = 't1'");
        assertThat(printedTable(response.rows()), is(
            "NULL| false| false| NULL| a| NULL| NULL| s| 0| a| 0| 0| true| NULL| t1_pk| -2048275947| true| NULL| NULL| 728874843| NULL| p| 0| true| -874078436\n"));
    }

    @Test
    public void testPgDescriptionTableIsEmpty() {
        execute("select * from pg_description");
        assertThat(printedTable(response.rows()), is(""));
        assertThat(response.cols(), arrayContaining("classoid", "description", "objoid", "objsubid"));
    }

    @Test
    @UseRandomizedSchema(random = false)
    @UseHashJoins(0)
    public void testPgSettingsTable() {
        execute("select name, setting, short_desc, min_val, max_val from pg_catalog.pg_settings");
        assertThat(printedTable(response.rows()), is(
            "enable_hashjoin| false| Considers using the Hash Join instead of the Nested Loop Join implementation.| NULL| NULL\n" +
            "error_on_unknown_object_key| true| Raises or suppresses ObjectKeyUnknownException when querying nonexistent keys to dynamic objects.| NULL| NULL\n" +
            "max_index_keys| 32| Shows the maximum number of index keys.| NULL| NULL\n" +
            "optimizer_deduplicate_order| true| Indicates if the optimizer rule DeduplicateOrder is activated.| NULL| NULL\n" +
            "optimizer_merge_aggregate_and_collect_to_count| true| Indicates if the optimizer rule MergeAggregateAndCollectToCount is activated.| NULL| NULL\n" +
            "optimizer_merge_aggregate_rename_and_collect_to_count| true| Indicates if the optimizer rule MergeAggregateRenameAndCollectToCount is activated.| NULL| NULL\n" +
            "optimizer_merge_filter_and_collect| true| Indicates if the optimizer rule MergeFilterAndCollect is activated.| NULL| NULL\n" +
            "optimizer_merge_filters| true| Indicates if the optimizer rule MergeFilters is activated.| NULL| NULL\n" +
            "optimizer_move_filter_beneath_fetch_or_eval| true| Indicates if the optimizer rule MoveFilterBeneathFetchOrEval is activated.| NULL| NULL\n" +
            "optimizer_move_filter_beneath_group_by| true| Indicates if the optimizer rule MoveFilterBeneathGroupBy is activated.| NULL| NULL\n" +
            "optimizer_move_filter_beneath_hash_join| true| Indicates if the optimizer rule MoveFilterBeneathHashJoin is activated.| NULL| NULL\n" +
            "optimizer_move_filter_beneath_nested_loop| true| Indicates if the optimizer rule MoveFilterBeneathNestedLoop is activated.| NULL| NULL\n" +
            "optimizer_move_filter_beneath_order| true| Indicates if the optimizer rule MoveFilterBeneathOrder is activated.| NULL| NULL\n" +
            "optimizer_move_filter_beneath_project_set| true| Indicates if the optimizer rule MoveFilterBeneathProjectSet is activated.| NULL| NULL\n" +
            "optimizer_move_filter_beneath_rename| true| Indicates if the optimizer rule MoveFilterBeneathRename is activated.| NULL| NULL\n" +
            "optimizer_move_filter_beneath_union| true| Indicates if the optimizer rule MoveFilterBeneathUnion is activated.| NULL| NULL\n" +
            "optimizer_move_filter_beneath_window_agg| true| Indicates if the optimizer rule MoveFilterBeneathWindowAgg is activated.| NULL| NULL\n" +
            "optimizer_move_order_beneath_fetch_or_eval| true| Indicates if the optimizer rule MoveOrderBeneathFetchOrEval is activated.| NULL| NULL\n" +
            "optimizer_move_order_beneath_nested_loop| true| Indicates if the optimizer rule MoveOrderBeneathNestedLoop is activated.| NULL| NULL\n" +
            "optimizer_move_order_beneath_rename| true| Indicates if the optimizer rule MoveOrderBeneathRename is activated.| NULL| NULL\n" +
            "optimizer_move_order_beneath_union| true| Indicates if the optimizer rule MoveOrderBeneathUnion is activated.| NULL| NULL\n" +
            "optimizer_remove_redundant_fetch_or_eval| true| Indicates if the optimizer rule RemoveRedundantFetchOrEval is activated.| NULL| NULL\n" +
            "optimizer_rewrite_collect_to_get| true| Indicates if the optimizer rule RewriteCollectToGet is activated.| NULL| NULL\n" +
            "optimizer_rewrite_filter_on_outer_join_to_inner_join| true| Indicates if the optimizer rule RewriteFilterOnOuterJoinToInnerJoin is activated.| NULL| NULL\n" +
            "optimizer_rewrite_group_by_keys_limit_to_top_n_distinct| true| Indicates if the optimizer rule RewriteGroupByKeysLimitToTopNDistinct is activated.| NULL| NULL\n" +
            "optimizer_rewrite_insert_from_sub_query_to_insert_from_values| true| Indicates if the optimizer rule RewriteInsertFromSubQueryToInsertFromValues is activated.| NULL| NULL\n" +
            "optimizer_rewrite_to_query_then_fetch| true| Indicates if the optimizer rule RewriteToQueryThenFetch is activated.| NULL| NULL\n" +
            "search_path| pg_catalog, doc| Sets the schema search order.| NULL| NULL\n" +
            "server_version| 10.5| Reports the emulated PostgreSQL version number| NULL| NULL\n" +
            "server_version_num| 100500| Reports the emulated PostgreSQL version number| NULL| NULL\n")
        );
    }

    @Test
    public void test_primary_key_in_pg_index() {
        execute("select i.indexrelid, i.indrelid, i.indkey from pg_index i, pg_class c where c.relname = 't1' and c.oid = i.indrelid;");
        assertThat(printedTable(response.rows()), is("-649073482| 728874843| [1]\n"));
    }

    @Test
    public void test_primary_key_in_pg_class() {
        execute("select ct.oid, ct.relkind, ct.relname, ct.relnamespace, ct.relnatts, ct.relpersistence, ct.relreplident, ct.reltuples" +
                " from pg_class ct, (select * from pg_index i, pg_class c where c.relname = 't1' and c.oid = i.indrelid) i" +
                " where ct.oid = i.indexrelid;");
        assertThat(printedTable(response.rows()), is("-649073482| i| t1_pkey| -2048275947| 3| p| p| 0.0\n"));
    }

    @Test
    public void test_pg_proc_return_correct_column_names() {
        execute("select * from pg_proc");
        assertThat(
            response.cols(),
            arrayContainingInAnyOrder(
                "oid", "proname", "pronamespace", "proowner", "prolang",
                "procost", "prorows", "provariadic", "protransform", "proisagg",
                "proiswindow", "prosecdef", "proleakproof", "proisstrict", "proretset",
                "provolatile", "proparallel", "pronargs", "pronargdefaults",
                "prorettype",  "proargtypes", "proallargtypes", "proargmodes",
                "proargnames", "proargdefaults", "protrftypes", "prosrc", "probin",
                "proconfig", "proacl")
        );
    }

    @Test
    public void test_pg_proc_select_variadic_and_non_variadic_functions() {
        execute(
            "SELECT oid, proname, pronamespace, " +
            "            prorows, provariadic, proisagg, " +
            "            proiswindow, proretset, prorettype, " +
            "            proargtypes, proargmodes, prosrc " +
            "FROM pg_proc " +
            "WHERE proname = ANY(['least', 'current_timestamp', 'format', 'array_difference'])");

        // sort by name signature args length
        Arrays.sort(response.rows(), (o1, o2) -> {
            int cmp = ((String) o1[1]).compareTo((String) o2[1]);
            return cmp == 0 ? ((List<?>) o1[9]).size() - ((List<?>) o2[9]).size() : cmp;
        });
        assertThat(printedTable(response.rows()), is(
            "-1329052381| array_difference| -1861355723| 1000.0| 0| false| false| true| 2277| [2277, 2277]| NULL| array_difference\n" +
            "726540318| current_timestamp| -1861355723| 0.0| 0| false| false| false| 1184| []| NULL| current_timestamp\n" +
            "-359449865| current_timestamp| -1861355723| 0.0| 0| false| false| false| 1184| [23]| NULL| current_timestamp\n" +
            "-277796690| format| -1861355723| 0.0| 2276| false| false| false| 1043| [1043, 2276]| [i, v]| format\n" +
            "89277575| least| -1861355723| 0.0| 2276| false| false| false| 2276| [2276]| [v]| least\n"));
    }

    @Test
    public void test_select_field_of_type_regproc_from_pg_type_and_cast_it_to_text_and_int() {
        execute(
            "SELECT typname, typreceive, typreceive::int, typreceive::text " +
            "FROM pg_type " +
            "WHERE typname = 'bool'");
        assertThat(printedTable(response.rows()), is("bool| boolrecv| 994071801| boolrecv\n"));
    }

    @Test
    public void test_join_pg_proc_with_pg_type_on_typreceive_for_bool() {
        execute(
            "SELECT pg_type.typname, pg_type.typreceive, pg_proc.oid " +
            "FROM pg_type " +
            "JOIN pg_proc ON pg_proc.oid = pg_type.typreceive " +
            "WHERE pg_type.typname = 'bool'");
        assertThat(printedTable(response.rows()), is("bool| boolrecv| 994071801\n"));
    }

    @Test
    public void test_jpg_get_function_result() {
        execute("SELECT pg_get_function_result(oid), proname " +
                "FROM pg_proc " +
                "WHERE proname = 'trunc' " +
                "ORDER BY 1, 2 " +
                "LIMIT 10;");
        assertThat(printedTable(response.rows()), is("bigint| trunc\n" +
                                                     "bigint| trunc\n" +
                                                     "double precision| trunc\n" +
                                                     "integer| trunc\n" +
                                                     "integer| trunc\n" +
                                                     "integer| trunc\n" +
                                                     "integer| trunc\n"));
    }

    @Test
    public void test_npgsql_type_lookup_returns_array_typtype_and_elemtypoid() throws Exception {
        execute("""
            select pg_proc.oid from pg_proc
            inner join pg_type on typreceive = pg_proc.oid
            where proname = 'array_recv' and typname = '_int4'
        """);
        assertThat(printedTable(response.rows()), is(
            "556695454\n"
        ));

        execute("""
            SELECT typ.oid, typname, typrelid, typnotnull, relkind, typelem AS elemoid,
                CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
                CASE
                    WHEN proc.proname='array_recv' THEN typ.typelem
                    WHEN typ.typtype='r' THEN rngsubtype
                    WHEN typ.typtype='d' THEN typ.typbasetype
                END AS elemtypoid
            FROM pg_type AS typ
            LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
            LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
            LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)
            where typname in ('_int2', '_int4')
            order by 1, 3, 4, 5
        """);
        assertThat(printedTable(response.rows()), is(
            "1005| _int2| 0| false| NULL| 21| a| 21\n" +
            "1007| _int4| 0| false| NULL| 23| a| 23\n"
        ));
    }

    @Test
    public void test_kepserver_regclass_cast_query() throws Exception {
        execute("select nspname from pg_namespace n, pg_class c where c.relnamespace=n.oid and c.oid='kepware'::regclass");
        assertThat(printedTable(response.rows()), is(
            ""
        ));
    }
}
