/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import org.junit.Test;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.CoreMatchers.is;

public class AggregateExpressionIntegrationTest extends SQLTransportIntegrationTest {

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
        assertThat(printedTable(response.rows()),
                   is("a| [4]| [3, 4]\n" +
                      "b| [4, 5]| [3, 4, 5]\n")
        );
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
        assertThat(printedTable(response.rows()),
                   is("a| [4]\n" +
                      "NULL| [4, 5]\n")
        );
    }

    @Test
    public void test_filter_in_aggregate_expr_for_global_aggregate() {
        execute("SELECT" +
                "   COLLECT_SET(x) FILTER (WHERE x > 3), " +
                "   collect_set(x) FILTER (WHERE x > 2) " +
                "FROM UNNEST([1, 3, 4, 2, 5, 4]) AS t(x, y)");
        assertThat(printedTable(response.rows()), is("[4, 5]| [3, 4, 5]\n"));
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
        assertThat(printedTable(response.rows()),
                   is("[2]\n" +
                      "[3]\n"));
    }

    @Test
    public void test_filter_in_aggregate_expr_with_group_by_single_numeric_column_with_nulls() {
        execute("SELECT" +
                "   COLLECT_SET(x) FILTER (WHERE x > 1) " +
                "FROM unnest(" +
                "   [1, 2, 1, 3]," +
                "   [1, 1, null, null]) AS t(x, y) " +
                "GROUP BY y");
        assertThat(printedTable(response.rows()),
                   is("[2]\n" +
                      "[3]\n"));
    }

    @Test
    public void test_filter_with_subquery_in_aggregate_expr_for_global_aggregate() {
        execute("SELECT" +
                "   COLLECT_SET(x) FILTER (WHERE x in (SELECT UNNEST([1, 3]))) " +
                "FROM UNNEST([1, 2]) AS t(x)");
        assertThat(printedTable(response.rows()),
                   is("[1]\n"));
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
        assertThat(printedTable(response.rows()),
                   is("a| [1, 4]| [3]\n" +
                      "b| [4]| [3, 5]\n"));
    }
}
