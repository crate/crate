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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;

@UseHashJoins(0)
public class CorrelatedSubqueryITest extends IntegTestCase {

    private Properties properties;

    @Before
    public void setup() {
        properties = new Properties();
        properties.setProperty("user", "crate");
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_simple_correlated_subquery() {
        execute("EXPLAIN (COSTS FALSE) SELECT 1, (SELECT t.mountain) FROM sys.summits t");

        // This should use the correlated-join execution path;
        // If we later optimize this query, ensure there is a test for CorrelatedJoin execution
            assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Eval[1, (SELECT mountain FROM (empty_row))]\n" +
            "  └ CorrelatedJoin[1, mountain, (SELECT mountain FROM (empty_row))]\n" +
            "    └ Rename[1, mountain] AS t\n" +
            "      └ Collect[sys.summits | [1, mountain] | true]\n" +
            "    └ SubPlan\n" +
            "      └ Eval[mountain]\n" +
            "        └ Limit[2::bigint;0::bigint]\n" +
            "          └ TableFunction[empty_row | [] | true]\n"
        );
        execute("SELECT 1, (SELECT t.mountain) FROM sys.summits t");
        Comparator<Object[]> compareMountain = Comparator.comparing((Object[] row) -> (String) row[1]);
        List<Object[]> mountains = Stream.of(response.rows())
            .sorted(compareMountain)
            .limit(5)
            .toList();
        assertThat(TestingHelpers.printRows(mountains)).isEqualTo(
            "1| Acherkogel\n" +
            "1| Ackerlspitze\n" +
            "1| Adamello\n" +
            "1| Admonter Reichenstein\n" +
            "1| Aiguille Méridionale d'Arves\n"
        );
    }

    @Test
    public void test_simple_correlated_subquery_with_user_table_as_input() {
        execute("create table tbl (x int)");
        execute("insert into tbl (x) values (1), (2), (3)");
        execute("refresh table tbl");
        execute("SELECT (SELECT t.x) FROM tbl t order by 1 desc");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "3\n" +
            "2\n" +
            "1\n"
        );
    }

    @Test
    public void test_query_fails_if_correlated_subquery_returns_more_than_1_row() {
        String stmt = "SELECT (SELECT t.mountain from sys.summits) FROM sys.summits t";
        assertThatThrownBy(() -> execute(stmt))
            .hasMessageContaining("Subquery returned more than 1 row when it shouldn't.");
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_simple_correlated_subquery_with_order_by() {
        String statement = "SELECT 1, (SELECT t.mountain) FROM sys.summits t order by 2 asc limit 5";
        execute("EXPLAIN (COSTS FALSE)" + statement);

        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Eval[1, (SELECT mountain FROM (empty_row))]\n" +
            "  └ Limit[5::bigint;0]\n" +
            "    └ OrderBy[(SELECT mountain FROM (empty_row)) ASC]\n" +
            "      └ CorrelatedJoin[1, mountain, (SELECT mountain FROM (empty_row))]\n" +
            "        └ Rename[1, mountain] AS t\n" +
            "          └ Collect[sys.summits | [1, mountain] | true]\n" +
            "        └ SubPlan\n" +
            "          └ Eval[mountain]\n" +
            "            └ Limit[2::bigint;0::bigint]\n" +
            "              └ TableFunction[empty_row | [] | true]\n"
        );
        execute(statement);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "1| Acherkogel\n" +
            "1| Ackerlspitze\n" +
            "1| Adamello\n" +
            "1| Admonter Reichenstein\n" +
            "1| Aiguille Méridionale d'Arves\n"
        );
    }

    @Test
    public void test_correlated_subquery_can_use_outer_column_in_where_clause() {
        String statement = """
            SELECT
                mountain,
                (SELECT height FROM sys.summits WHERE summits.mountain = t.mountain limit 1)
            FROM
                sys.summits t
            ORDER BY 1 ASC LIMIT 5
            """;
        execute(statement);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Acherkogel| 3008\n" +
            "Ackerlspitze| 2329\n" +
            "Adamello| 3539\n" +
            "Admonter Reichenstein| 2251\n" +
            "Aiguille Méridionale d'Arves| 3514\n"
        );
    }

    @Test
    public void test_correlated_subquery_within_case_using_outer_column_in_where_clause() {
        String stmt = """
            SELECT
                CASE WHEN t.typtype = 'd' THEN
                    (
                        SELECT
                            CASE WHEN typname = E'date' THEN 91 ELSE 1111 END
                        FROM
                            pg_type
                        WHERE
                            oid = t.typbasetype
                    )
                ELSE
                    NULL
                END AS base_type
            FROM
                pg_catalog.pg_type t
            LIMIT 4
            """;
        execute(stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "NULL\n" +
            "NULL\n" +
            "NULL\n" +
            "NULL\n"
        );
    }

    @Test
    public void test_correlated_subquery_used_in_virtual_table_with_union() {
        String stmt = """
            SELECT
                (SELECT t1.mountain)
            FROM
                sys.summits t1
            UNION ALL
            SELECT
                (SELECT t2.mountain)
            FROM sys.summits t2
            ORDER BY 1 DESC
            LIMIT 4
            """;
        execute(stmt);
        assertThat(response).hasRows(
            "Špik",
            "Špik",
            "Škrlatica",
            "Škrlatica"
        );
    }

    @Test
    public void test_multiple_correlated_subqueries_in_selectlist() {
        String stmt = "SELECT (SELECT t.mountain), (SELECT t.height) FROM sys.summits t order by 2 desc limit 3";
        execute(stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Mont Blanc| 4808\n" +
            "Monte Rosa| 4634\n" +
            "Dom| 4545\n"
        );
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_where_exists_with_correlated_subquery() {
        String stmt = "select x from generate_series(1, 2) as t (x) where exists (select t.x)";
        execute("EXPLAIN (COSTS FALSE)" + stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Eval[x]\n" +
            "  └ Filter[EXISTS (SELECT x FROM (empty_row))]\n" +
            "    └ CorrelatedJoin[x, (SELECT x FROM (empty_row))]\n" +
            "      └ Rename[x] AS t\n" +
            "        └ TableFunction[generate_series | [generate_series] | true]\n" +
            "      └ SubPlan\n" +
            "        └ Eval[x]\n" +
            "          └ Limit[1;0]\n" +
            "            └ TableFunction[empty_row | [] | true]\n");
        execute(stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "1\n" +
            "2\n"
        );
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_can_use_correlated_subquery_in_where_clause() {
        String stmt = "SELECT mountain, region FROM sys.summits t where mountain = (SELECT t.mountain) ORDER BY height desc limit 3";
        execute("EXPLAIN (COSTS FALSE)" + stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Eval[mountain, region]\n" +
            "  └ Limit[3::bigint;0]\n" +
            "    └ OrderBy[height DESC]\n" +
            "      └ Filter[(mountain = (SELECT mountain FROM (empty_row)))]\n" +
            "        └ CorrelatedJoin[mountain, region, height, (SELECT mountain FROM (empty_row))]\n" +
            "          └ Rename[mountain, region, height] AS t\n" +
            "            └ Collect[sys.summits | [mountain, region, height] | true]\n" +
            "          └ SubPlan\n" +
            "            └ Eval[mountain]\n" +
            "              └ Limit[2::bigint;0::bigint]\n" +
            "                └ TableFunction[empty_row | [] | true]\n"
        );
        execute(stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Mont Blanc| Mont Blanc massif\n" +
            "Monte Rosa| Monte Rosa Alps\n" +
            "Dom| Mischabel\n"
        );


        stmt = """
            SELECT
                mountain,
                region
            FROM
                sys.summits t,
                sys.cluster
            WHERE
                mountain = (SELECT t.mountain) ORDER BY height desc limit 3
            """;
        execute(stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Mont Blanc| Mont Blanc massif\n" +
            "Monte Rosa| Monte Rosa Alps\n" +
            "Dom| Mischabel\n"
        );
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_correlated_subquery_without_table_alias_within_join_condition() {
        String stmt = """
            SELECT
                columns.table_name,
                columns.column_name
            FROM
                information_schema.columns
                LEFT JOIN pg_catalog.pg_attribute AS col_attr
                    ON col_attr.attname = columns.column_name
                    AND col_attr.attrelid = (
                        SELECT
                            pg_class.oid
                        FROM
                            pg_catalog.pg_class
                            LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                        WHERE
                            pg_class.relname = columns.table_name
                            AND pg_namespace.nspname = columns.table_schema
                    )

            ORDER BY 1, 2 DESC
            LIMIT 3
            """;
        execute("EXPLAIN (COSTS FALSE)" + stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Eval[table_name, column_name]\n" +
            "  └ Limit[3::bigint;0]\n" +
            "    └ OrderBy[table_name ASC column_name DESC]\n" +
            "      └ Filter[(attrelid = (SELECT oid FROM (pg_catalog.pg_class, pg_catalog.pg_namespace)))]\n" +
            "        └ CorrelatedJoin[table_name, column_name, table_schema, attname, attrelid, (SELECT oid FROM (pg_catalog.pg_class, pg_catalog.pg_namespace))]\n" +
            "          └ NestedLoopJoin[LEFT | (attname = column_name)]\n" +
            "            ├ Collect[information_schema.columns | [table_name, column_name, table_schema] | true]\n" +
            "            └ Rename[attname, attrelid] AS col_attr\n" +
            "              └ Collect[pg_catalog.pg_attribute | [attname, attrelid] | true]\n" +
            "          └ SubPlan\n" +
            "            └ Eval[oid]\n" +
            "              └ Limit[2::bigint;0::bigint]\n" +
            "                └ NestedLoopJoin[INNER | (oid = relnamespace)]\n" +
            "                  ├ Collect[pg_catalog.pg_class | [oid, relnamespace] | (relname = table_name)]\n" +
            "                  └ Collect[pg_catalog.pg_namespace | [oid] | (nspname = table_schema)]\n"
        );
        execute(stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "allocations| table_schema\n" +
            "allocations| table_name\n" +
            "allocations| shard_id\n"
        );

    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_correlated_subquery_without_table_alias_within_join_condition_and_additional_condition() {
        var stmt = """
            SELECT
                columns.table_name,
                columns.column_name
            FROM
                information_schema.columns
                LEFT JOIN pg_catalog.pg_attribute AS col_attr
                    ON col_attr.attname = columns.column_name
                    AND col_attr.attrelid = (
                        SELECT
                            pg_class.oid
                        FROM
                            pg_catalog.pg_class
                            LEFT JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                        WHERE
                            pg_class.relname = columns.table_name
                            AND pg_namespace.nspname = columns.table_schema
                    )
                    AND col_attr.attrelid = (SELECT col_attr.attrelid)

            ORDER BY 1, 2 DESC
            LIMIT 3
            """;
        execute("EXPLAIN (COSTS FALSE)" + stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Eval[table_name, column_name]\n" +
            "  └ Limit[3::bigint;0]\n" +
            "    └ OrderBy[table_name ASC column_name DESC]\n" +
            "      └ Filter[((attrelid = (SELECT oid FROM (pg_catalog.pg_class, pg_catalog.pg_namespace))) AND (attrelid = (SELECT attrelid FROM (empty_row))))]\n" +
            "        └ CorrelatedJoin[table_name, column_name, table_schema, attname, attrelid, (SELECT oid FROM (pg_catalog.pg_class, pg_catalog.pg_namespace)), (SELECT attrelid FROM (empty_row))]\n" +
            "          └ CorrelatedJoin[table_name, column_name, table_schema, attname, attrelid, (SELECT oid FROM (pg_catalog.pg_class, pg_catalog.pg_namespace))]\n" +
            "            └ NestedLoopJoin[LEFT | (attname = column_name)]\n" +
            "              ├ Collect[information_schema.columns | [table_name, column_name, table_schema] | true]\n" +
            "              └ Rename[attname, attrelid] AS col_attr\n" +
            "                └ Collect[pg_catalog.pg_attribute | [attname, attrelid] | true]\n" +
            "            └ SubPlan\n" +
            "              └ Eval[oid]\n" +
            "                └ Limit[2::bigint;0::bigint]\n" +
            "                  └ NestedLoopJoin[INNER | (oid = relnamespace)]\n" +
            "                    ├ Collect[pg_catalog.pg_class | [oid, relnamespace] | (relname = table_name)]\n" +
            "                    └ Collect[pg_catalog.pg_namespace | [oid] | (nspname = table_schema)]\n" +
            "          └ SubPlan\n" +
            "            └ Eval[attrelid]\n" +
            "              └ Limit[2::bigint;0::bigint]\n" +
            "                └ TableFunction[empty_row | [] | true]\n"
        );
        execute(stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "allocations| table_schema\n" +
            "allocations| table_name\n" +
            "allocations| shard_id\n"
        );
    }

    @Test
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(0)
    public void test_can_use_column_in_query_paired_with_correlation_that_is_not_selected() throws Exception {
        execute("CREATE TABLE a (f1 TEXT, f2 TEXT, f3 TEXT)");
        execute("CREATE TABLE b (f1 TEXT, f2 TEXT, f3 TEXT)");
        execute("INSERT INTO a VALUES ('a','b','c')");
        execute("INSERT INTO b VALUES ('a','b','c')");
        execute("refresh table a, b");
        String stmt = "SELECT count(*) FROM a "
            + "WHERE EXISTS (SELECT 1 FROM b where a.f1 = b.f1 and a.f2 = b.f2 and b.f3 ='c') and a.f3 IN ('a','b','c')";
        assertThat(execute("explain (costs false)" + stmt)).hasLines(
            "HashAggregate[count(*)]",
            "  └ Filter[EXISTS (SELECT 1 FROM (doc.b))]",
            "    └ CorrelatedJoin[f1, f2, (SELECT 1 FROM (doc.b))]",
            "      └ Collect[doc.a | [f1, f2] | (f3 = ANY(['a', 'b', 'c']))]",
            "      └ SubPlan",
            "        └ Eval[1]",
            "          └ Limit[1;0]",
            "            └ Collect[doc.b | [1] | (((f1 = f1) AND (f2 = f2)) AND (f3 = 'c'))]"
        );
        assertThat(execute(stmt)).hasRows(
            "1"
        );

        // Execute again with disabled optimizer rule to verify that the query is still working
        try (var conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            Statement statement = conn.createStatement();
            conn.setAutoCommit(false);
            statement.execute("SET SESSION search_path TO 'doc'");
            statement.execute("SET optimizer_move_filter_beneath_correlated_join = false");

            ResultSet result = statement.executeQuery(stmt);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    public void test_correlated_subquery_together_with_join() throws Exception {
        // https://github.com/crate/crate/issues/14671
        execute(
            """
            SELECT
                n.nspname AS schema,
                t.typname AS typename,
                t.oid::int4 AS typeid
            FROM
                pg_type t
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE
                EXISTS (
                    SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem);
            """);
        assertThat(response).hasRowCount(24L);
    }
}
