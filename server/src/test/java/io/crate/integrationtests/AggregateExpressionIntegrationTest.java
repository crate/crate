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
import java.util.Map;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.UseJdbc;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class AggregateExpressionIntegrationTest extends IntegTestCase {

    @Test
    public void test_sum_int() throws Exception {
        execute("create table tbl (x int)");
        execute("insert into tbl (x) values (1), (2), (3)");
        execute("refresh table tbl");

        execute("select sum(x) from tbl");
        assertThat(response).hasRows("6");
    }

    @Test
    public void test_min_and_max_by() {
        execute("create table tbl (name text, x int)");
        execute("insert into tbl (name, x) values (?, ?)",
            new Object[][] {
                new Object[] {"foo", 1},
                new Object[] {null, 2},
                new Object[] {"no", null},
                new Object[] {"bar", 2},
                new Object[] {"baz", 3}
            }
        );
        execute("refresh table tbl");

        execute("select max_by(name, x) from tbl");
        assertThat(response).hasRows("baz");
        execute("select min_by(name, x) from tbl");
        assertThat(response).hasRows("foo");
    }

    @Test
    @UseJdbc(0) // Deterministic output format of INTERVAL
    public void test_sum_interval() {
        execute(
            """
                SELECT SUM(col1) FROM (SELECT
                unnest(
                [
                    INTERVAL '6 years 5 mons' YEAR TO MONTH,
                    INTERVAL '4 days 3 hours' DAY TO HOUR,
                    INTERVAL '45.123']) AS col1) AS t
            """);
        assertThat(response).hasRows("P6Y5M4DT3H45.123S");
    }

    @Test
    public void test_numeric_sum_with_on_floating_point_and_long_columns_with_doc_values() {
        execute("CREATE TABLE tbl (x float, y double, z long) " +
                "CLUSTERED INTO 1 SHARDS " +
                "WITH (number_of_replicas=0)");
        execute(
            "INSERT INTO tbl VALUES (?, ?, ?)", new Object[][]{
                new Object[]{1.0d, 1f, 9223372036854775806L},
                new Object[]{1.56d, 2.12f, 3L}});
        execute("refresh table tbl");

        execute("SELECT sum(x::numeric(16, 1))," +
                "       sum(y::numeric(16, 2))," +
                "       sum(z::numeric) " + // overflow
                "FROM tbl");
        assertThat(response).hasRows("2.6| 3.12| 9223372036854775809");
    }

    @Test
    public void test_numeric_agg_with_numeric_cast() {
        execute("CREATE TABLE IF NOT EXISTS t01 (txt TEXT, val DOUBLE PRECISION)");
        execute("INSERT INTO t01 VALUES ('a', 1.0)");
        execute("REFRESH TABLE t01");
        execute(
            """
                SELECT
                    txt,
                SUM(val :: NUMERIC(10, 2)) AS val2
                FROM t01
                GROUP BY txt
            """);
        assertThat(response).hasRows(
            "a| 1.00"
        );
    }

    @Test
    public void test_numeric_avg_with_on_floating_point_and_long_columns_with_doc_values() {
        execute("CREATE TABLE tbl (x float, y double, z long) " +
            "CLUSTERED INTO 1 SHARDS " +
            "WITH (number_of_replicas=0)");
        execute(
            "INSERT INTO tbl VALUES (?, ?, ?)", new Object[][]{
                new Object[]{0.3f, 2.251d, 9223372036854775806L},
                new Object[]{0.7f, 2.251d, 9223372036854775806L}});
        execute("refresh table tbl");

        execute("SELECT avg(x::numeric(16, 1)), " +
                "       avg(y::numeric(16, 2))," +
                "       avg(z::numeric) " + // Handle precision error by casting.
            "FROM tbl");
        assertThat(response).hasRows("0.5| 2.25| 9223372036854775806");
    }

    @Test
    @UseJdbc(0) // For consistent output format
    public void test_interval_avg() {
        execute("CREATE TABLE tbl (ts timestamp) " +
                "CLUSTERED INTO 1 SHARDS " +
                "WITH (number_of_replicas=0)");
        execute(
            "INSERT INTO tbl VALUES (?)", new Object[][]{
                new Object[]{"2022-01-01 00:00:00"},
                new Object[]{"2023-01-01 00:00:00"},
                new Object[]{"2023-02-01 00:00:33.678"},
                new Object[]{"2023-11-22 11:22:33"}});
        execute("refresh table tbl");

        execute("SELECT avg('2023-05-04 22:33:11.123' - ts) FROM tbl");
        assertThat(response).hasRows("P126DT1H42M24.453S");
    }

    @Test
    public void test_filter_in_aggregate_expr_with_group_by() {
        execute("SELECT" +
                "   y, " +
                "   COLLECT_SET(x) FILTER (WHERE x > 3), " +
                "   COLLECT_SET(x) FILTER (WHERE x > 2) " +
                "FROM UNNEST(" +
                "   [1, 3, 4, 3, 5, 4]," +
                "   ['a', 'a', 'a', 'b', 'b', 'b']) as t(x, y) " +
                "GROUP BY y " +
                "ORDER BY y");
        assertThat(response).hasRows(
            "a| [4]| [3, 4]",
            "b| [4, 5]| [3, 4, 5]");
    }

    @Test
    public void test_filter_in_aggregate_expr_with_group_by_column_with_nulls() {
        execute("SELECT" +
                "   y, " +
                "   COLLECT_SET(x) FILTER (WHERE x > 3) " +
                "FROM UNNEST(" +
                "   [1, 4, 3, 5, 4]," +
                "   ['a', 'a', null, null, null]) AS t(x, y) " +
                "GROUP BY y " +
                "ORDER BY y");
        assertThat(response).hasRows(
            "a| [4]",
            "NULL| [4, 5]");
    }

    @Test
    public void test_filter_in_aggregate_expr_for_global_aggregate() {
        execute("SELECT" +
                "   COLLECT_SET(x) FILTER (WHERE x > 3), " +
                "   COLLECT_SET(x) FILTER (WHERE x > 2) " +
                "FROM UNNEST([1, 3, 4, 2, 5, 4]) AS t(x, y)");
        assertThat(response).hasRows("[4, 5]| [3, 4, 5]");
    }

    // grouping by a single numeric value would result in a different
    // code path where the optimized version of the grouping collector is used
    @Test
    public void test_filter_in_aggregate_expr_with_group_by_single_number() {
        execute("SELECT" +
                "   COLLECT_SET(x) FILTER (WHERE x > 1) " +
                "FROM UNNEST(" +
                "   [1, 2, 1, 3]," +
                "   [1, 1, 2, 2]) AS t(x, y) " +
                "GROUP BY y");
        assertThat(response).hasRows(
            "[2]",
            "[3]");
    }

    @Test
    public void test_filter_in_aggregate_expr_with_group_by_single_numeric_column_with_nulls() {
        execute("SELECT" +
                "   COLLECT_SET(x) FILTER (WHERE x > 1) " +
                "FROM UNNEST(" +
                "   [1, 2, 1, 3]," +
                "   [1, 1, null, null]) AS t(x, y) " +
                "GROUP BY y");
        assertThat(response).hasRows(
            "[3]",
            "[2]");
    }

    @Test
    public void test_filter_with_subquery_in_aggregate_expr_for_global_aggregate() {
        execute("SELECT" +
                "   COLLECT_SET(x) FILTER (WHERE x in (SELECT UNNEST([1, 3]))) " +
                "FROM UNNEST([1, 2]) AS t(x)");
        assertThat(response).hasRows("[1]");
    }

    @Test
    public void test_filter_with_subquery_in_aggregate_expr_for_group_by_aggregates() {
        execute("SELECT" +
                "   y, " +
                "   COLLECT_SET(x) FILTER (WHERE x in (SELECT UNNEST([1, 4]))), " +
                "   COLLECT_SET(x) FILTER (WHERE x in (SELECT UNNEST([3, 5]))) " +
                "FROM UNNEST(" +
                "   [1, 3, 4, 3, 5, 4]," +
                "   ['a', 'a', 'a', 'b', 'b', 'b']) as t(x, y) " +
                "GROUP BY y " +
                "ORDER BY y");
        assertThat(response).hasRows(
            "a| [1, 4]| [3]",
            "b| [4]| [3, 5]");
    }

    @Test
    public void test_filter_in_count_star_aggregate_function() {
        execute("CREATE TABLE t (x int)");
        execute("INSERT INTO t VALUES (1), (3), (2), (4)");
        execute("REFRESH TABLE t");

        execute("SELECT COUNT(*) FILTER (WHERE x > 2) FROM t");
        assertThat(response).hasRows("2");
    }

    @Test
    public void test_filter_with_group_by_low_cardinality_text_field() {
        execute("CREATE TABLE t (x TEXT) CLUSTERED INTO 1 SHARDS");
        // has low cardinality ration: CARDINALITY_RATIO_THRESHOLD (0.5) > 2 terms / 5 docs
        // that would result in a code path that uses the optimized group by iterator
        execute("INSERT INTO t VALUES ('a'), ('b'), ('a'), ('b'), ('a')");
        execute("REFRESH TABLE t");

        execute("SELECT x, COUNT(*) FILTER (WHERE x = ANY(['a'])) " +
                "FROM t " +
                "GROUP BY x " +
                "ORDER BY x");
        assertThat(response).hasRows(
            "a| 3",
            "b| 0");
    }

    @Test
    public void test_aggregation_in_order_by_without_having_them_in_the_select_list() throws Exception {
        execute("create table test(id integer, name varchar) clustered into 1 shards with(number_of_replicas=0)");
        execute("insert into test(id, name) values (1, 'a'),(2, 'a'),(3, 'a'),(4, 'b'),(5, 'b'),(6, 'c')");
        execute("refresh table test");

        execute("select name from test group by name order by count(1)");
        assertThat(response).hasRows(
            "c",
            "b",
            "a");

        execute("select name from test group by name order by count(id)");
        assertThat(response).hasRows(
            "c",
            "b",
            "a");

        execute("select name from test group by name order by count(id) asc");
        assertThat(response).hasRows(
            "c",
            "b",
            "a");

        execute("select name from test group by name order by count(id) desc");
        assertThat(response).hasRows(
            "a",
            "b",
            "c");
    }

    public void test_assure_cmp_by_function_call_with_reference_and_literal_does_not_throw_exception() {
        execute("create table tbl (name text, x int);");
        execute("insert into tbl (name, x) values ('foo', 10)");
        execute("refresh table tbl");
        execute("select max_by(name, 1) from tbl;");
        assertThat(response).hasRows("foo");
    }

    @SuppressWarnings("unchecked")
    public void test_topk_agg() {
        execute("create table tbl (l long, l_no_doc_values long storage with(columnstore = false))");
        execute("insert into tbl(l, l_no_doc_values) values (1, 1), (1, 1), (1, 2), (2, 2), (2, 2)");
        execute("refresh table tbl");

        // Use this very verbose style of assertion, since the return type is an Array<Object(UNDEFINED)>,
        // and while HTTP returns Long for the item and frequency values, PG converts them to Integer
        execute("select topk(l) tl from tbl");
        Map<String, Object> resultRows = (Map<String, Object>) response.rows()[0][0];
        assertThat(resultRows).containsOnlyKeys("maximum_error", "frequencies");
        assertThat(resultRows.get("maximum_error")).isNotNull();
        assertThat(resultRows.get("frequencies")).isNotNull();

        execute("select topk(l_no_doc_values) tl from tbl");
        resultRows = (Map<String, Object>) response.rows()[0][0];
        assertThat(resultRows).containsOnlyKeys("maximum_error", "frequencies");
        assertThat(resultRows.get("maximum_error")).isNotNull();
        assertThat(resultRows.get("frequencies")).isNotNull();
    }
}
