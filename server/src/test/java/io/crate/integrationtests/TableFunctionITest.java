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

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.UseJdbc;

public class TableFunctionITest extends IntegTestCase {

    @Test
    public void testSelectFromUnnest() {
        execute("select * from unnest([1, 2], ['Trillian', 'Marvin'])");
        assertThat(response).hasRows(
            "1| Trillian",
            "2| Marvin"
        );
    }

    @Test
    public void testSelectFromUnnestWithOrderByAndLimit() {
        execute("select * from unnest([1, 2], ['Trillian', 'Marvin']) order by col1 desc limit 1");
        assertThat(response).hasRows("2| Marvin");
    }

    @Test
    public void testSelectFromUnnestWithScalarFunction() {
        execute("select substr(col2, 0, 1) from unnest([1, 2], ['Trillian', 'Marvin']) order by col1 limit 1");
        assertThat(response).hasRows("T");
    }

    @Test
    public void testInsertIntoFromSelectUnnest() {
        execute("create table t (id int primary key, name string) with (number_of_replicas = 0)");
        ensureYellow();

        Object[] args = $(List.of(1, 2), List.of("Marvin", "Trillian")); // non-bulk request
        execute("insert into t (select * from unnest(?, ?))", args);
        execute("refresh table t");

        assertThat(execute("select * from t order by id")).hasRows(
            "1| Marvin",
            "2| Trillian"
        );
    }

    @Test
    public void testGroupByFromUnnest() {
        execute("select unnest, count(*) from unnest(['Marvin', 'Marvin', 'Trillian']) group by unnest order by 2 desc");
        assertThat(response).hasRows(
            "Marvin| 2",
            "Trillian| 1");
    }

    @Test
    public void testGlobalAggregationFromUnnest() {
        assertThat(execute("select max(unnest) from unnest([1, 2, 3, 4])").rows()[0][0]).isEqualTo(4);
    }

    @Test
    public void testJoinUnnestWithTable() {
        execute("create table t (id int primary key)");
        ensureYellow();
        execute("insert into t (id) values (1)");
        execute("refresh table t");
        assertThat(execute("select * from unnest([1, 2]) inner join t on t.id = unnest::integer")).hasRows(
            "1| 1"
        );
    }

    @Test
    public void testWhereClauseIsEvaluated() {
        execute("select unnest from unnest([1, 2]) where unnest = 2");
        assertThat(response).hasRows("2");
    }

    @Test
    public void testValueExpression() {
        execute("select * from unnest(coalesce([1,2]))");
        assertThat(response).hasRows(
            "1",
            "2"
        );
    }

    @Test
    public void testUnnestUsedInSelectList() {
        execute("select unnest(col1) * 2, col2 from (select [1, 2, 3, 4], 'foo') tbl (col1, col2) order by 1 desc limit 2");
        assertThat(response).hasRows(
            "8| foo",
            "6| foo"
        );
    }

    @Test
    public void testAggregationOnResultOfTableFunctionWithinSubQuery() {
        execute("select max(x) from (select unnest([1, 2, 3, 4]) as x) as t");
        assertThat(response.rows()[0][0]).isEqualTo(4);
    }

    @Test
    public void testSelectUnnestAndStandaloneColumnFromUserTable() {
        execute("create table t1 (x int, arr array(string))");
        execute("insert into t1 (x, arr) values (1, ['foo', 'bar'])");
        execute("refresh table t1");
        execute("select x, unnest(arr) from t1");
        assertThat(response).hasRows(
            "1| foo",
            "1| bar"
        );
    }

    @Test
    public void testTableFunctionIsAppliedAfterAggregationAndAggregationCanBeAnArgumentToTableFunction() {
        execute("select sum(unnest), generate_series(1, sum(unnest)) from unnest([1, 2])");
        assertThat(response).hasRows(
            "3| 1",
            "3| 2",
            "3| 3"
        );
    }

    @Test
    public void testDistinctIsAppliedAfterTableFunctions() {
        execute("select distinct generate_series(1, 2), unnest from unnest([1, 1]) order by 1 asc");
        assertThat(response).hasRows(
            "1| 1",
            "2| 1"
        );
    }

    @Test
    public void testScalarCanBeUsedInFromClause() {
        execute("select * from substr('foo',1,2), array_cat([1], [2, 3])");
        assertThat(response).hasRows(
            "fo| [1, 2, 3]"
        );
    }

    @Test
    @UseJdbc(1)
    public void test_srf_with_multiple_columns_in_result_can_be_used_in_selectlist() throws Exception {
        execute("select pg_catalog.pg_get_keywords()");
        var rows = Arrays.asList(response.rows());
        rows.sort(Comparator.comparing((Object[] row) -> (String) row[0]));
        assertThat(rows.get(0)[0]).isEqualTo("(absolute,U,unreserved)");
    }

    @Test
    public void test_row_type_field_access() throws Exception {
        execute("select (information_schema._pg_expandarray(ARRAY['a', 'b', 'b'])).n");
        assertThat(response).hasRows(
            "1",
            "2",
            "3"
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
        execute("EXPLAIN (COSTS FALSE) " + stmt);
        assertThat(response).hasLines(
            "Eval[unnest(regexp_matches(col1, 'crate_(\\d+.\\d+.\\d+)')) AS version]",
            "  └ ProjectSet[unnest(regexp_matches(col1, 'crate_(\\d+.\\d+.\\d+)')), col1]",
            "    └ ProjectSet[regexp_matches(col1, 'crate_(\\d+.\\d+.\\d+)'), col1]",
            "      └ Rename[col1] AS tbl",
            "        └ TableFunction[_values | [col1] | true]"
        );
        execute(stmt);
        assertThat(response).hasRows("4.3.1");
    }

    @Test
    public void testColumnNameWhenTableAliasPresent() throws Exception {
        execute("SELECT * FROM unnest([1, 2]) AS my_func");
        assertThat(response).hasColumns("my_func");
    }

    @Test
    public void testColumnNameWhenBothTableAliasAndColumnAliasPresent() throws Exception {
        execute("SELECT * FROM unnest([1, 2]) AS my_func (col_alias, col_alias2)");
        assertThat(response).hasColumns("col_alias");
    }

    @Test
    public void TestColumnNameWhenTableFunctionReturnMoreThanOneOutput() throws Exception {
        //if the table function returns more than one output, table alias will not be used
        execute("SELECT * FROM unnest([1, 2],['a','b']) AS my_func(col_alias1)");
        assertThat(response).hasColumns("col_alias1", "col2");
    }

}
