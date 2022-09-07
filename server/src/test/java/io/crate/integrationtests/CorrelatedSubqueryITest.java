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


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;

public class CorrelatedSubqueryITest extends IntegTestCase {

    @Test
    public void test_simple_correlated_subquery() {
        execute("EXPLAIN SELECT 1, (SELECT t.mountain) FROM sys.summits t");

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

    @Test
    public void test_simple_correlated_subquery_with_order_by() {
        String statement = "SELECT 1, (SELECT t.mountain) FROM sys.summits t order by 2 asc limit 5";
        execute("EXPLAIN " + statement);

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
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Špik\n" +
            "Špik\n" +
            "Škrlatica\n" +
            "Škrlatica\n"
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

    @Test
    public void test_can_use_correlated_subquery_in_where_clause() {
        String stmt = "SELECT mountain, region FROM sys.summits t where mountain = (SELECT t.mountain) ORDER BY height desc limit 3";
        execute("EXPLAIN " + stmt);
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

    @UseJdbc(0)
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
        execute("EXPLAIN " + stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "Eval[table_name, column_name]\n" +
            "  └ Limit[3::bigint;0]\n" +
            "    └ OrderBy[table_name ASC column_name DESC]\n" +
            "      └ NestedLoopJoin[LEFT | ((attname = column_name) AND (attrelid = (SELECT oid FROM (pg_catalog.pg_class, pg_catalog.pg_namespace))))]\n" +
            "        ├ CorrelatedJoin[table_name, column_name, table_schema, (SELECT oid FROM (pg_catalog.pg_class, pg_catalog.pg_namespace))]\n" +
            "        │  └ Collect[information_schema.columns | [table_name, column_name, table_schema] | true]\n" +
            "        │  └ SubPlan\n" +
            "        │    └ Eval[oid]\n" +
            "        │      └ Limit[2::bigint;0::bigint]\n" +
            "        │        └ NestedLoopJoin[INNER | (oid = relnamespace)]\n" +
            "        │          ├ Collect[pg_catalog.pg_class | [oid, relnamespace, relname] | (relname = table_name)]\n" +
            "        │          └ Collect[pg_catalog.pg_namespace | [oid, nspname] | (nspname = table_schema)]\n" +
            "        └ Rename[attname, attrelid] AS col_attr\n" +
            "          └ Collect[pg_catalog.pg_attribute | [attname, attrelid] | true]\n"
        );
        execute(stmt);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            "allocations| table_schema\n" +
            "allocations| table_name\n" +
            "allocations| shard_id\n"
        );

    }

    @UseJdbc(0)
    @Test
    public void test_correlated_subquery_without_table_alias_within_join_condition_failed() {
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
        execute("EXPLAIN " + stmt);
        System.out.println(TestingHelpers.printedTable(response.rows()));
        execute(stmt);
    }
}
