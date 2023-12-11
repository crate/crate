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

import static io.crate.testing.Asserts.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.action.sql.Sessions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.view.ViewInfo;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseNewCluster;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;
import io.crate.role.RoleLookup;

public class PgCatalogITest extends IntegTestCase {

    @Before
    public void createRelations() {
        execute("""
            create table doc.t1 (
                id int primary key,
                s string, o object as (a int),
                g generated always as s || '_foo')
            """);
        execute("create view doc.v1 as select id from doc.t1");
    }

    @After
    public void dropViews() {
        Schemas schemas = cluster().getInstance(Schemas.class);
        List<String> fqQuotedViews = new ArrayList<>();
        for (SchemaInfo schema : schemas) {
            for (ViewInfo view : schema.getViews()) {
                fqQuotedViews.add(view.ident().sqlFqn());
            }
        }
        execute("drop view " + String.join(", ", fqQuotedViews));
    }

    @After
    public void dropAllUsers() {
        // clean all created users and roles
        execute("SELECT name FROM sys.users WHERE superuser = FALSE");
        for (Object[] objects : response.rows()) {
            String user = (String) objects[0];
            execute("DROP user \"" + user + "\"");
        }
        execute("SELECT name FROM sys.roles");
        for (Object[] objects : response.rows()) {
            String role = (String) objects[0];
            execute("DROP ROLE \"" + role + "\"");
        }
    }

    @Test
    public void testPgClassTable() {
        execute("select * from pg_catalog.pg_class where relname in ('t1', 'v1', 'tables', 'nodes') order by relname");
        assertThat(response).hasRows(
            "-1420189195| NULL| 0| 0| 0| 0| false| 0| false| false| false| false| false| true| false| r| 0| nodes| -458336339| 18| 0| NULL| 0| 0| NULL| p| p| 0| false| 0| 0| -1.0| 0",
            "728874843| NULL| 0| 0| 0| 0| false| 0| false| false| false| false| false| true| false| r| 0| t1| -2048275947| 4| 0| NULL| 0| 0| NULL| p| p| 0| false| 0| 0| -1.0| 0",
            "-1689918046| NULL| 0| 0| 0| 0| false| 0| false| false| false| false| false| true| false| r| 0| tables| 204690627| 16| 0| NULL| 0| 0| NULL| p| p| 0| false| 0| 0| -1.0| 0",
            "845171032| NULL| 0| 0| 0| 0| false| 0| false| false| false| false| false| true| false| v| 0| v1| -2048275947| 1| 0| NULL| 0| 0| NULL| p| p| 0| false| 0| 0| -1.0| 0");
    }

    @Test
    public void test_pg_class_table_with_pg_get_expr() {
        execute("select pg_get_expr(relpartbound,0) from pg_catalog.pg_class limit 1");
        assertThat(response).hasRows("NULL");
    }

    @Test
    public void testPgNamespaceTable() {
        execute("select * from pg_catalog.pg_namespace order by nspname");
        assertThat(response).hasRows(
            "NULL| blob| 0| -508866815",
            "NULL| doc| 0| -2048275947",
            "NULL| information_schema| 0| 204690627",
            "NULL| pg_catalog| 0| -68025646",
            "NULL| sys| 0| -458336339");
    }

    @Test
    public void testPgNamespaceTable_shows_namespace_if_user_can_see_a_table() {
        execute("create table vip.tbl (x int)");
        execute("create user hoschi");
        execute("grant dql on table doc.t1 to hoschi");

        RoleLookup userLookup = cluster().getInstance(RoleLookup.class);
        Sessions sessions = cluster().getInstance(Sessions.class);
        try (var session = sessions.newSession("doc", userLookup.findUser("hoschi"))) {
            execute("select nspname from pg_catalog.pg_namespace order by nspname", session);
            // shows doc due to table permission, but not vip
            assertThat(response).hasRows(
                "doc",
                "information_schema",
                "pg_catalog"
            );
        }

        execute("create view vip.v1 as select 1");
        execute("grant dql on view vip.v1 to hoschi");
        try (var session = sessions.newSession("doc", userLookup.findUser("hoschi"))) {
            execute("select nspname from pg_catalog.pg_namespace order by nspname", session);
            assertThat(response).hasRows(
                "doc",
                "information_schema",
                "pg_catalog",
                "vip"
            );
        }
    }

    @Test
    public void testPgAttributeTable() {
        execute(
            "select a.* from pg_catalog.pg_attribute as a join pg_catalog.pg_class as c on a.attrelid = c.oid where" +
            " c.relname = 't1' order by a.attnum");
        assertThat(response).hasRows(
            "NULL| NULL| false| -1| 0| NULL| | false| false| | 0| false| true| 4| NULL| id| 0| true| 1| NULL| 728874843| 0| NULL| 23| -1",
            "NULL| NULL| false| -1| 0| NULL| | false| false| | 0| false| true| -1| NULL| s| 0| false| 2| NULL| 728874843| 0| NULL| 1043| -1",
            "NULL| NULL| false| -1| 0| NULL| | false| false| | 0| false| true| -1| NULL| o| 0| false| 3| NULL| 728874843| 0| NULL| 114| -1",
            "NULL| NULL| false| -1| 0| NULL| | false| false| | 0| false| true| 4| NULL| o['a']| 0| false| 4| NULL| 728874843| 0| NULL| 23| -1",
            "NULL| NULL| false| -1| 0| NULL| s| false| false| | 0| false| true| -1| NULL| g| 0| false| 5| NULL| 728874843| 0| NULL| 1043| -1");
    }

    @Test
    public void testPgIndexTable() {
        execute("select count(*) from pg_catalog.pg_index");
        assertThat(response).hasRows("24");
    }

    @Test
    public void test_pg_index_table_with_pg_get_expr() {
        execute("select pg_get_expr(indexprs,0), pg_get_expr(indpred,0) from pg_catalog.pg_index limit 1");
        assertThat(response).hasRows("NULL| NULL");
    }

    @Test
    public void testPgConstraintTable() {
        execute("select cn.* from pg_constraint cn, pg_class c where cn.conrelid = c.oid and c.relname = 't1'");
        assertThat(response).hasRows(
            "NULL| false| false| NULL| a| NULL| NULL| s| 0| a| 0| 0| true| [1]| t1_pk| -2048275947| true| 0| NULL| " +
            "NULL| 728874843| p| 0| true| -874078436");
    }

    @Test
    public void testPgTablesTable() {
        execute("select * from pg_tables WHERE tablename = 't1'");
        assertThat(response).hasRows("false| false| false| false| doc| t1| NULL| NULL");

        execute("select count(*) from pg_tables WHERE tablename = 'v1'");
        assertThat(response).hasRows("0");
    }

    @Test
    public void testPgViewsTable() {
        execute("select * from pg_views WHERE viewname = 'v1'");
        assertThat(response).hasRows(
            new Object[] { "SELECT \"id\"\nFROM \"doc\".\"t1\"\n", "doc", "v1", "crate" }
        );

        execute("select count(*) from pg_views WHERE viewname = 't1'");
        assertThat(response).hasRows("0");
    }

    @Test
    public void testPgDescriptionTableIsEmpty() {
        execute("select * from pg_description");
        assertThat(response).isEmpty();
        assertThat(response).hasColumns("classoid", "description", "objoid", "objsubid");
    }

    @Test
    public void testPgShdescriptionTableIsEmpty() {
        execute("select * from pg_shdescription");
        assertThat(response).isEmpty();
        assertThat(response).hasColumns("classoid", "description", "objoid");
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    @UseJdbc(0)
    @UseRandomizedSchema(random = false)
    @UseHashJoins(0)
    public void testPgSettingsTable() {
        execute("select name, setting, short_desc, min_val, max_val from pg_catalog.pg_settings");
        assertThat(response).hasRows(
            "application_name| NULL| Optional application name. Can be set by a client to identify the application which created the connection| NULL| NULL",
            "datestyle| ISO| Display format for date and time values.| NULL| NULL",
            "enable_hashjoin| false| Considers using the Hash Join instead of the Nested Loop Join implementation.| NULL| NULL",
            "error_on_unknown_object_key| true| Raises or suppresses ObjectKeyUnknownException when querying nonexistent keys to dynamic objects.| NULL| NULL",
            "max_identifier_length| 255| Shows the maximum length of identifiers in bytes.| NULL| NULL",
            "max_index_keys| 32| Shows the maximum number of index keys.| NULL| NULL",
            "memory.operation_limit| 0| Memory limit in bytes for an individual operation. 0 by-passes the operation limit, relying entirely on the global circuit breaker limits| NULL| NULL",
            "optimizer_deduplicate_order| true| Indicates if the optimizer rule DeduplicateOrder is activated.| NULL| NULL",
            "optimizer_eliminate_cross_join| true| Indicates if the optimizer rule EliminateCrossJoin is activated.| NULL| NULL",
            "optimizer_merge_aggregate_and_collect_to_count| true| Indicates if the optimizer rule MergeAggregateAndCollectToCount is activated.| NULL| NULL",
            "optimizer_merge_aggregate_rename_and_collect_to_count| true| Indicates if the optimizer rule MergeAggregateRenameAndCollectToCount is activated.| NULL| NULL",
            "optimizer_merge_filter_and_collect| true| Indicates if the optimizer rule MergeFilterAndCollect is activated.| NULL| NULL",
            "optimizer_merge_filters| true| Indicates if the optimizer rule MergeFilters is activated.| NULL| NULL",
            "optimizer_move_constant_join_conditions_beneath_nested_loop| true| Indicates if the optimizer rule MoveConstantJoinConditionsBeneathNestedLoop is activated.| NULL| NULL",
            "optimizer_move_filter_beneath_correlated_join| true| Indicates if the optimizer rule MoveFilterBeneathCorrelatedJoin is activated.| NULL| NULL",
            "optimizer_move_filter_beneath_eval| true| Indicates if the optimizer rule MoveFilterBeneathEval is activated.| NULL| NULL",
            "optimizer_move_filter_beneath_group_by| true| Indicates if the optimizer rule MoveFilterBeneathGroupBy is activated.| NULL| NULL",
            "optimizer_move_filter_beneath_join| true| Indicates if the optimizer rule MoveFilterBeneathJoin is activated.| NULL| NULL",
            "optimizer_move_filter_beneath_order| true| Indicates if the optimizer rule MoveFilterBeneathOrder is activated.| NULL| NULL",
            "optimizer_move_filter_beneath_project_set| true| Indicates if the optimizer rule MoveFilterBeneathProjectSet is activated.| NULL| NULL",
            "optimizer_move_filter_beneath_rename| true| Indicates if the optimizer rule MoveFilterBeneathRename is activated.| NULL| NULL",
            "optimizer_move_filter_beneath_union| true| Indicates if the optimizer rule MoveFilterBeneathUnion is activated.| NULL| NULL",
            "optimizer_move_filter_beneath_window_agg| true| Indicates if the optimizer rule MoveFilterBeneathWindowAgg is activated.| NULL| NULL",
            "optimizer_move_limit_beneath_eval| true| Indicates if the optimizer rule MoveLimitBeneathEval is activated.| NULL| NULL",
            "optimizer_move_limit_beneath_rename| true| Indicates if the optimizer rule MoveLimitBeneathRename is activated.| NULL| NULL",
            "optimizer_move_order_beneath_fetch_or_eval| true| Indicates if the optimizer rule MoveOrderBeneathFetchOrEval is activated.| NULL| NULL",
            "optimizer_move_order_beneath_nested_loop| true| Indicates if the optimizer rule MoveOrderBeneathNestedLoop is activated.| NULL| NULL",
            "optimizer_move_order_beneath_rename| true| Indicates if the optimizer rule MoveOrderBeneathRename is activated.| NULL| NULL",
            "optimizer_move_order_beneath_union| true| Indicates if the optimizer rule MoveOrderBeneathUnion is activated.| NULL| NULL",
            "optimizer_optimize_collect_where_clause_access| true| Indicates if the optimizer rule OptimizeCollectWhereClauseAccess is activated.| NULL| NULL",
            "optimizer_remove_redundant_fetch_or_eval| true| Indicates if the optimizer rule RemoveRedundantFetchOrEval is activated.| NULL| NULL",
            "optimizer_reorder_hash_join| true| Indicates if the optimizer rule ReorderHashJoin is activated.| NULL| NULL",
            "optimizer_reorder_nested_loop_join| true| Indicates if the optimizer rule ReorderNestedLoopJoin is activated.| NULL| NULL",
            "optimizer_rewrite_filter_on_outer_join_to_inner_join| true| Indicates if the optimizer rule RewriteFilterOnOuterJoinToInnerJoin is activated.| NULL| NULL",
            "optimizer_rewrite_group_by_keys_limit_to_limit_distinct| true| Indicates if the optimizer rule RewriteGroupByKeysLimitToLimitDistinct is activated.| NULL| NULL",
            "optimizer_rewrite_nested_loop_join_to_hash_join| true| Indicates if the optimizer rule RewriteNestedLoopJoinToHashJoin is activated.| NULL| NULL",
            "optimizer_rewrite_to_query_then_fetch| true| Indicates if the optimizer rule RewriteToQueryThenFetch is activated.| NULL| NULL",
            "search_path| doc| Sets the schema search order.| NULL| NULL",
            "server_version| 14.0| Reports the emulated PostgreSQL version number| NULL| NULL",
            "server_version_num| 140000| Reports the emulated PostgreSQL version number| NULL| NULL",
            "standard_conforming_strings| on| Causes '...' strings to treat backslashes literally.| NULL| NULL",
            "statement_timeout| 0s| The maximum duration of any statement before it gets killed. Infinite/disabled if 0| NULL| NULL"
        );
    }

    @Test
    public void test_primary_key_in_pg_index() {
        execute("select i.indexrelid, i.indrelid, i.indkey from pg_index i, pg_class c where c.relname = 't1' and c.oid = i.indrelid;");
        assertThat(response).hasRows("-649073482| 728874843| [1]");
    }

    @Test
    public void test_primary_key_in_pg_class() {
        execute("select ct.oid, ct.relkind, ct.relname, ct.relnamespace, ct.relnatts, ct.relpersistence, ct.relreplident, ct.reltuples" +
                " from pg_class ct, (select * from pg_index i, pg_class c where c.relname = 't1' and c.oid = i.indrelid) i" +
                " where ct.oid = i.indexrelid;");
        assertThat(response).hasRows("-649073482| i| t1_pkey| -2048275947| 4| p| p| 0.0");
    }

    @Test
    public void test_pg_proc_return_correct_column_names() {
        execute("select * from pg_proc");
        assertThat(response).hasColumns(
            "oid", "proacl", "proallargtypes", "proargdefaults", "proargmodes", "proargnames", "proargtypes",
            "probin", "proconfig", "procost", "proisstrict", "prokind", "prolang", "proleakproof", "proname",
            "pronamespace", "pronargdefaults", "pronargs", "proowner", "proparallel", "proretset", "prorettype",
            "prorows", "prosecdef", "prosqlbody", "prosrc", "prosupport", "protrftypes", "provariadic", "provolatile"
        );
    }

    @Test
    public void test_pg_proc_select_variadic_and_non_variadic_functions() {
        execute(
            """
                    SELECT oid, proname, pronamespace, prorows, provariadic, prokind,
                           proretset, prorettype, proargtypes, proargmodes, prosrc
                    FROM pg_proc
                    WHERE proname = ANY(['least', 'current_timestamp', 'format', 'array_difference'])
                """);

        // sort by name signature args length
        Arrays.sort(response.rows(), (o1, o2) -> {
            int cmp = ((String) o1[1]).compareTo((String) o2[1]);
            return cmp == 0 ? ((List<?>) o1[8]).size() - ((List<?>) o2[8]).size() : cmp;
        });
        assertThat(response).hasRows(
            "-1329052381| array_difference| -1861355723| 1000.0| 0| f| true| 2277| [2277, 2277]| NULL| array_difference",
            "726540318| current_timestamp| -1861355723| 0.0| 0| f| false| 1184| []| NULL| current_timestamp",
            "-359449865| current_timestamp| -1861355723| 0.0| 0| f| false| 1184| [23]| NULL| current_timestamp",
            "-277796690| format| -1861355723| 0.0| 2276| f| false| 1043| [1043, 2276]| [i, v]| format",
            "89277575| least| -1861355723| 0.0| 2276| f| false| 2276| [2276]| [v]| least");
    }

    @Test
    public void test_select_field_of_type_regproc_from_pg_type_and_cast_it_to_text_and_int() {
        execute(
            "SELECT typname, typreceive, typreceive::int, typreceive::text " +
            "FROM pg_type " +
            "WHERE typname = 'bool'");
        assertThat(response).hasRows("bool| boolrecv| 994071801| boolrecv");
    }

    @Test
    public void test_join_pg_proc_with_pg_type_on_typreceive_for_bool() {
        execute(
            "SELECT pg_type.typname, pg_type.typreceive, pg_proc.oid " +
            "FROM pg_type " +
            "JOIN pg_proc ON pg_proc.oid = pg_type.typreceive " +
            "WHERE pg_type.typname = 'bool'");
        assertThat(response).hasRows("bool| boolrecv| 994071801");
    }

    @Test
    public void test_jpg_get_function_result() {
        execute("SELECT pg_get_function_result(oid), proname " +
                "FROM pg_proc " +
                "WHERE proname = 'trunc' " +
                "ORDER BY 1, 2 " +
                "LIMIT 10;");
        assertThat(response).hasRows("bigint| trunc",
                                     "bigint| trunc",
                                     "double precision| trunc",
                                     "integer| trunc",
                                     "integer| trunc",
                                     "integer| trunc",
                                     "integer| trunc");
    }

    @Test
    public void test_npgsql_type_lookup_returns_array_typtype_and_elemtypoid() throws Exception {
        execute("""
                    select pg_proc.oid from pg_proc
                    inner join pg_type on typreceive = pg_proc.oid
                    where proname = 'array_recv' and typname = '_int4'
                    """);
        assertThat(response).hasRows("556695454");

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
        assertThat(response).hasRows(
            "1005| _int2| 0| false| NULL| 21| a| 21",
            "1007| _int4| 0| false| NULL| 23| a| 23"
        );
    }

    @Test
    public void test_kepserver_regclass_cast_query() throws Exception {
        execute("select nspname from pg_namespace n, pg_class c where c.relnamespace=n.oid and c.oid='kepware'::regclass");
        assertThat(response).isEmpty();
    }

    @Test
    public void test_pg_constrains_conkey_array_populated() throws Exception {
        execute("CREATE TABLE doc.tbl (" +
            "not_null int not null," +
            "int_col int," +
            "long_col bigint," +
            "checked int CONSTRAINT positive CHECK(checked>0)," +
            "PRIMARY KEY (int_col, long_col)," +
            "CONSTRAINT many_cols_and_functions CHECK(not_null * long_col > int_col))"
        );
        int reloid = OidHash.relationOid(OidHash.Type.TABLE, new RelationName("doc", "tbl"));
        response = execute("select i.conkey, i.conname, i.contype from pg_catalog.pg_constraint i where i.conrelid  = " + reloid + " order by i.conname");
        assertThat(response).hasRows(
            "[2, 1, 3]| many_cols_and_functions| c",
            "[4]| positive| c",
            "[2, 3]| tbl_pk| p"
        );
    }

    @UseRandomizedSchema(random = false)
    @Test
    public void test_pg_class_oid_equals_cast_of_string_to_regclass() {
        execute("CREATE TABLE persons (x INT)");
        execute("SELECT " +
            " '\"persons\"'::regclass::integer oid_from_relname, " +
            " oid " +
            " FROM pg_class WHERE relname ='persons'");
        assertThat(response).hasRows("1726373441| 1726373441");
    }

    @Test
    @UseNewCluster // Dropped column prefix contains OID and must be deterministic.
    public void test_dropped_columns_shown_in_pg_attribute() {
        execute("create table t(a integer, o object AS(oo object AS(a int)))");

        execute("alter table t drop column a");
        execute("alter table t add column a text");

        // Verify that dropping top-level column is reflected in pg_attribute
        // and re-added column with the same name appears as a new entry.
        execute("""
            select attname, attnum, attisdropped
            from pg_attribute
            where attrelid = 't'::regclass
            order by attnum"""
        );

        // Column 'a' has OID 6 because first 5 are taken by the table, created in createRelations().
        String pgAttributeRows = """
            _dropped_6| 1| true
            o| 2| false
            o['oo']| 3| false
            o['oo']['a']| 4| false
            a| 5| false
            """;
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualToIgnoringWhitespace(pgAttributeRows);

        // Drop sub-column which in turn, has children column.
        // Re-add columns with the same names but leaf having different type.
        execute("alter table t drop column o['oo']");
        execute("alter table t add column o['oo'] object AS(a text)");


        // Only top-level columns are shown, children are completely gone.
        // For example, there is no entry with ordinal 4 ==> no entry for dropped column o['oo']['a']|
        execute("""
            select attname, attnum, attisdropped
            from pg_attribute
            where attrelid = 't'::regclass
            order by attnum"""
        );

        pgAttributeRows = """
            _dropped_6| 1| true
            o| 2| false
            _dropped_8| 3| true
            a| 5| false
            o['oo']| 6| false
            o['oo']['a']| 7| false
            """;
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualToIgnoringWhitespace(pgAttributeRows);
    }

    @Test
    public void test_named_primary_key_constraint_names_are_visible() {
        execute("create table t (aa int constraint c_aa primary key)");
        execute("select conname from pg_catalog.pg_constraint where conname = 'c_aa'");
        assertThat(response).hasRows("c_aa");
    }

    @Test
    public void test_pg_roles() {
        execute("CREATE USER \"Arthur\" WITH (password='foo')");
        execute("CREATE USER \"John\"");
        execute("CREATE ROLE \"SuperDooper\"");
        execute("GRANT AL TO \"SuperDooper\"");

        execute("SELECT rolname, rolpassword, rolsuper, rolinherit, rolcreaterole, rolcanlogin, " +
            "rolreplication, rolconnlimit, rolcreatedb, rolvaliduntil, rolbypassrls, rolconfig " +
            "FROM pg_catalog.pg_roles ORDER BY rolname");
        assertThat(response).hasRows(
            "Arthur| ********| false| true| false| true| false| -1| NULL| NULL| NULL| NULL",
            "John| NULL| false| true| false| true| false| -1| NULL| NULL| NULL| NULL",
            "SuperDooper| NULL| false| true| true| false| true| -1| NULL| NULL| NULL| NULL",
            "crate| NULL| true| true| true| true| true| -1| NULL| NULL| NULL| NULL");

        execute("SELECT oid FROM pg_catalog.pg_roles");
        assertThat(response).hasRowCount(4);
        for (int i = 0; i < response.rowCount(); i++) {
            assertThat(response.rows()[i][0]).isNotNull();
        }
    }
}
