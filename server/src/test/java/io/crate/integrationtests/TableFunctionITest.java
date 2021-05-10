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

import io.crate.testing.UseJdbc;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.core.Is.is;

public class TableFunctionITest extends SQLIntegrationTestCase {

    @Test
    public void testSelectFromUnnest() {
        execute("select * from unnest([1, 2], ['Trillian', 'Marvin'])");
        assertThat(response.rowCount(), is(2L));
        assertThat(printedTable(response.rows()), is("" +
                                                     "1| Trillian\n" +
                                                     "2| Marvin\n"));
    }

    @Test
    public void testSelectFromUnnestWithOrderByAndLimit() {
        execute("select * from unnest([1, 2], ['Trillian', 'Marvin']) order by col1 desc limit 1");
        assertThat(response.rowCount(), is(1L));
        assertThat(printedTable(response.rows()), is("2| Marvin\n"));
    }

    @Test
    public void testSelectFromUnnestWithScalarFunction() {
        execute("select substr(col2, 0, 1) from unnest([1, 2], ['Trillian', 'Marvin']) order by col1 limit 1");
        assertThat(printedTable(response.rows()), is("T\n"));
    }

    @Test
    public void testInsertIntoFromSelectUnnest() {
        execute("create table t (id int primary key, name string) with (number_of_replicas = 0)");
        ensureYellow();

        Object[] args = $(List.of(1, 2), List.of("Marvin", "Trillian")); // non-bulk request
        execute("insert into t (select * from unnest(?, ?))", args);
        execute("refresh table t");

        assertThat(printedTable(execute("select * from t order by id").rows()), is("1| Marvin\n2| Trillian\n"));
    }

    @Test
    public void testGroupByFromUnnest() {
        execute("select col1, count(*) from unnest(['Marvin', 'Marvin', 'Trillian']) group by col1 order by 2 desc");
        assertThat(printedTable(response.rows()), is("Marvin| 2\nTrillian| 1\n"));
    }

    @Test
    public void testGlobalAggregationFromUnnest() {
        assertThat(execute("select max(col1) from unnest([1, 2, 3, 4])").rows()[0][0], is(4));
    }

    @Test
    public void testJoinUnnestWithTable() {
        execute("create table t (id int primary key)");
        ensureYellow();
        execute("insert into t (id) values (1)");
        execute("refresh table t");
        assertThat(printedTable(execute("select * from unnest([1, 2]) inner join t on t.id = col1::integer").rows()),
            is("1| 1\n"));
    }

    @Test
    public void testWhereClauseIsEvaluated() {
        execute("select col1 from unnest([1, 2]) where col1 = 2");
        assertThat(printedTable(response.rows()), is("2\n"));
    }

    @Test
    public void testValueExpression() {
        execute("select * from unnest(coalesce([1,2]))");
        assertThat(printedTable(response.rows()), is("1\n2\n"));
    }

    @Test
    public void testUnnestUsedInSelectList() {
        execute("select unnest(col1) * 2, col2 from (select [1, 2, 3, 4], 'foo') tbl (col1, col2) order by 1 desc limit 2");
        assertThat(
            printedTable(response.rows()),
            is("8| foo\n6| foo\n")
        );
    }

    @Test
    public void testAggregationOnResultOfTableFunctionWithinSubQuery() {
        execute("select max(x) from (select unnest([1, 2, 3, 4]) as x) as t");
        assertThat(response.rows()[0][0], is(4));
    }

    @Test
    public void testSelectUnnestAndStandaloneColumnFromUserTable() {
        execute("create table t1 (x int, arr array(string))");
        execute("insert into t1 (x, arr) values (1, ['foo', 'bar'])");
        execute("refresh table t1");
        execute("select x, unnest(arr) from t1");
        assertThat(printedTable(response.rows()), is("" +
                                                     "1| foo\n" +
                                                     "1| bar\n"));
    }

    @Test
    public void testTableFunctionIsAppliedAfterAggregationAndAggregationCanBeAnArgumentToTableFunction() {
        execute("select sum(col1), generate_series(1, sum(col1)) from unnest([1, 2])");
        assertThat(printedTable(response.rows()), is("" +
                                                     "3| 1\n" +
                                                     "3| 2\n" +
                                                     "3| 3\n"));
    }

    @Test
    public void testDistinctIsAppliedAfterTableFunctions() {
        execute("select distinct generate_series(1, 2), col1 from unnest([1, 1]) order by 1 asc");
        assertThat(printedTable(response.rows()),
            is("1| 1\n" +
               "2| 1\n"));
    }

    @Test
    public void testScalarCanBeUsedInFromClause() {
        execute("select * from substr('foo',1,2), array_cat([1], [2, 3])");
        assertThat(printedTable(response.rows()),
            is("fo| [1, 2, 3]\n"));
    }

    @Test
    @UseJdbc(1)
    public void test_srf_with_multiple_columns_in_result_can_be_used_in_selectlist() throws Exception {
        execute("select pg_catalog.pg_get_keywords()");
        var rows = Arrays.asList(response.rows());

        rows.sort(Comparator.comparing((Object[] x) -> {
            Object value = x[0];
            if (value instanceof String) {
                return (Comparable) value;
            } else {
                throw new IllegalArgumentException("Unexpected value " + value);
            }
        }));
        assertThat(rows.get(0)[0], is("(add,R,reserved)"));
    }

    @Test
    public void test_row_type_field_access() throws Exception {
        execute("select (information_schema._pg_expandarray(ARRAY['a', 'b', 'b'])).n");
        assertThat(printedTable(response.rows()),
            is("1\n" +
               "2\n" +
               "3\n")
        );
    }

    @Test
    public void test_unnest_on_top_of_regexp_matches() throws Exception {
        String stmt = """
            SELECT
                unnest(regexp_matches(col1, 'crate_(\\d+.\\d+.\\d+)')) as version
            FROM
                (VALUES ('crate_4.3.1')) as tbl
        """;
        execute("EXPLAIN " + stmt);
        assertThat(printedTable(response.rows()), is(
            "Eval[unnest(regexp_matches(col1, 'crate_(\\d+.\\d+.\\d+)')) AS version]\n" +
            "  └ ProjectSet[unnest(regexp_matches(col1, 'crate_(\\d+.\\d+.\\d+)')), col1]\n" +
            "    └ ProjectSet[regexp_matches(col1, 'crate_(\\d+.\\d+.\\d+)'), col1]\n" +
            "      └ Rename[col1] AS tbl\n" +
            "        └ TableFunction[_values | [col1] | true]\n"
        ));
        execute(stmt);
        assertThat(printedTable(response.rows()), is(
            "4.3.1\n"
        ));
    }
}
