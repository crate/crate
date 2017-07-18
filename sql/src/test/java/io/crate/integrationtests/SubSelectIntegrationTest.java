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

import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

public class SubSelectIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void testSubSelectOrderBy() throws Exception {
        setup.setUpCharacters();

        execute("select i, name from (select id as i, name from characters order by name) as ch order by i desc");
        assertThat(printedTable(response.rows()),
            is("4| Arthur\n" +
               "3| Trillian\n" +
               "2| Ford\n" +
               "1| Arthur\n"));
    }

    @Test
    public void testSubSelectWhere() throws Exception {
        setup.setUpCharacters();

        execute("select id, name " +
                "from (select * from characters where female = true) as ch " +
                "where name like 'Arthur'");
        assertThat(printedTable(response.rows()),
            is("4| Arthur\n"));
    }

    @Test
    public void testSubSelectWhereDocKey() throws Exception {
        setup.setUpCharacters();

        execute("select id, name " +
                "from (select * from characters where female = true) as ch " +
                "where id = 4");
        assertThat(printedTable(response.rows()),
            is("4| Arthur\n"));
    }

    @Test
    public void testSubSelectLimitOffset() throws Exception {
        setup.setUpLocations();
        execute("refresh table locations");

        execute("select name " +
                "from (select * from locations order by name limit 10 offset 5) as l " +
                "limit 5 offset 4");
        assertThat(printedTable(response.rows()),
            is("End of the Galaxy\n" +
               "Galactic Sector QQ7 Active J Gamma\n" +
               "North West Ripple\n" +
               "Outer Eastern Rim\n"));
    }

    @Test
    public void testSubSelectOutputs() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        refresh();

        execute("select aa, (xxi + 1) " +
                "from (select (xx + i) as xxi, concat(a, a) as aa " +
                " from (select a, i, (x + x) as xx from t1) as t) as tt " +
                "order by aa");

        assertThat(printedTable(response.rows()),
            is("aa| 9\n" +
               "bb| 14\n" +
               "cc| 20\n" +
               "dd| 30\n"));
    }

    @Test
    public void testReferenceToNestedField() throws Exception {
        setup.groupBySetup();

        execute("select gender, minAge from ( " +
                "  select gender, min(age) as minAge from characters group by gender" +
                ") as ch " +
                "where gender = 'male'");
        assertThat(printedTable(response.rows()),
            is("male| 34\n"));
    }

    @Test
    public void testReferenceToNestedAggregatedField() throws Exception {
        setup.groupBySetup();
        execute("select gender, minAge from ( " +
                "  select gender, min(age) as minAge from characters group by gender" +
                ") as ch " +
                "where (minAge * 2) < 120 order by gender");
        assertThat(printedTable(response.rows()),
            is("female| 32\n" +
               "male| 34\n"));
    }

    @Test
    public void testNestedGroupByAggregation() throws Exception {
        setup.groupBySetup();
        execute("select count(*) from (" +
                "  select min(age) as minAge from characters group by gender) as ch " +
                "group by minAge");
        List<Object[]> rows = Arrays.asList(response.rows());
        Collections.sort(rows, OrderingByPosition.arrayOrdering(0, true, null));
        assertThat(TestingHelpers.printedTable(rows.toArray(new Object[0][])),
            is("1\n1\n"));
    }

    @Test
    public void testOrderingOnNestedAggregation() throws Exception {
        setup.groupBySetup();

        execute("select race, avg(age) as avgAge from ( " +
                "  select * from characters where gender = 'male' order by age) as ch " +
                "group by race");

        List<Object[]> rows = Arrays.asList(response.rows());
        Collections.sort(rows, OrderingByPosition.arrayOrdering(0, true, null));
        assertThat(printedTable(rows.toArray(new Object[0][])),
            is("Android| NULL\n" +
               "Human| 73.0\n" +
               "Vogon| NULL\n"));
    }

    @Test
    public void testFilterOnSubSelectWithJoins() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        execute("create table t2 (a string, i integer, y integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("insert into t2 (a, i, y) values ('aa', 22, 33),('bb', 33, 55),('cc', 55, 77),('dd', 77, 111)");
        refresh();

        execute("select col1, col2 from ( " +
                "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
                "  from t1, t2 where t2.y > 60) as t " +
                "where col1 = 'a' order by col3");

        assertThat(printedTable(response.rows()),
            is("a| 55\n" +
               "a| 77\n"));
    }

    @Test
    public void testNestedSubSelectWithJoins() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        execute("create table t2 (a string, i integer, y integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("insert into t2 (a, i, y) values ('aa', 22, 33),('bb', 33, 55),('cc', 55, 77),('dd', 77, 111)");
        refresh();

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1, t2 where t1.a='a' or t2.a='aa') as t) as tt " +
                "order by aa, xyi");

        assertThat(printedTable(response.rows()),
            is("aaa| 58\n" +
               "abb| 91\n" +
               "acc| 135\n" +
               "add| 191\n" +
               "baa| 60\n" +
               "caa| 62\n" +
               "daa| 66\n"));
    }

    @Test
    public void testNestedSubSelectWithOuterJoins() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        execute("create table t2 (a string, i integer, y integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("insert into t2 (a, i, y) values ('a', 22, 33),('bb', 33, 55),('cc', 55, 77),('dd', 77, 111)");
        refresh();

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1 left join t2 on t1.a = t2.a where t1.a='a') as t) as tt " +
                "order by aa, xyi");

        assertThat(printedTable(response.rows()),
            is("aa| 58\n"));

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1 right join t2 on t1.a = t2.a where t1.a='a' or t2.a in ('aa', 'bb')) as t) as tt " +
                "order by aa, xyi");

        assertThat(printedTable(response.rows()),
            is("aa| 58\n" +
               "bb| NULL\n"));

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1 full join t2 on t1.a = t2.a where t1.a='a' or t2.a in ('aa', 'bb')) as t) as tt " +
                "order by aa, xyi");

        assertThat(printedTable(response.rows()),
            is("aa| 58\n" +
               "bb| NULL\n"));
    }

    @Test
    public void testSingleRowSubselectInWhereClauseOnSysTables() throws Exception {
        assertThat(execute("select 1 where 2 = (select 2)").rowCount(), is(1L));
    }

    @Test
    public void testSingleRowSubSelectInWhereClauseOnDocTables() throws Exception {
        execute("create table t1 (x int)");
        execute("create table t2 (y int)");
        ensureYellow();
        execute("insert into t1 (x) values (1), (2)");
        execute("insert into t2 (y) values (2)");
        execute("refresh table t1, t2");

        execute("select * from t1 where x = (select y from t2)");
        assertThat(printedTable(response.rows()), is("2\n"));
    }

    @Test
    public void testNestedSingleRowSubSelect() throws Exception {
        execute("create table t1 (x int)");
        execute("create table t2 (y int)");
        execute("create table t3 (z int)");
        ensureYellow();
        execute("insert into t1 (x) values (1), (2)");
        execute("insert into t2 (y) values (2), (3)");
        execute("insert into t3 (z) values (2)");
        execute("refresh table t1, t2, t3");

        execute("select * from t1 where x = (select y from t2 where y = (select z from t3))");
        assertThat(printedTable(response.rows()), is("2\n"));
    }

    @Test
    public void testSingleRowSubSelectInGlobalAggregationWhereClause() throws Exception {
        execute("create table t1 (x long)");
        ensureYellow();
        execute("insert into t1 (x) values (1)");
        execute("refresh table t1");

        execute("select sum(x) from t1 where x = (select 1)");
        assertThat(printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testSingleRowSubSelectInGroupByWhereClause() throws Exception {
        execute("create table t1 (x long)");
        ensureYellow();
        execute("insert into t1 (x) values (1)");
        execute("refresh table t1");

        execute("select sum(x), x from t1 where x = (select 1) group by x");
        assertThat(printedTable(response.rows()), is("1| 1\n"));
    }

    @Test
    public void testSingleRowSubselectWithMultipleRowsReturning() throws Exception {
        execute("create table t1 (x long)");
        ensureYellow();
        execute("insert into t1 (x) values (1), (2)");
        execute("refresh table t1");

        expectedException.expectMessage("Subquery returned more than 1 row");
        execute("select name from sys.cluster where 1 = (select x from t1)");
    }

    @Test
    public void testSingleRowSubSelectCanBeUsedInSelectListAndWhereOfPrimaryKeyLookup() throws Exception {
        execute("create table t1 (x int primary key)");
        ensureYellow();
        execute("insert into t1 (x) values (1), (2)");
        execute("refresh table t1");

        execute("select x, (select 'foo') from t1 where x = (select 1)");
        assertThat(printedTable(response.rows()), is("1| foo\n"));
    }

    @Test
    public void testSingleRowSubSelectWorksWithJoins() throws Exception {
        execute("create table t (x long primary key)");
        ensureYellow();
        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");

        execute("select * from t as t1, t as t2 where t1.x = (select 1) order by t2.x");
        assertThat(printedTable(response.rows()), is("1| 1\n1| 2\n"));
    }

    @Test
    public void testSubSelectReturnsNoRowIsHandledAsNullValue() throws Exception {
        execute("select name from sys.cluster where name = (select name from sys.nodes where 1 = 2)");
        assertThat(response.rowCount(), is(0L));

        execute("select name from sys.cluster where (select name from sys.nodes where 1 = 2) is null");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testScalarSubqueryCanBeUsedInGroupByAndHaving() throws Exception {
        execute("select (select 'foo'), count(*) from unnest([1, 2]) group by 1 having count(*) = (select 2)");
        assertThat(printedTable(response.rows()), is("foo| 2\n"));
    }

    @Test
    public void testSubqueryInOrderResultsInAnError() throws Exception {
        execute("create table t (x long primary key)");
        ensureYellow();

        expectedException.expectMessage("Using a non-integer constant in ORDER BY is not supported");
        execute("select x from t order by (select 1)");
    }

    @Test
    public void testGlobalAggregatesOnSimpleSubQuery() throws Exception {
        execute("create table t (x int)");
        ensureYellow();

        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");

        // orderBy and limit in subQuery to prevent rewrite to non-subquery
        execute("select sum(x) from (select x from t order by x limit 1) as t");
        assertThat(printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testGlobalAggregateOnVirtualTableWithGroupBy() throws Exception {
        execute("create table t1 (id int, ts timestamp) with (number_of_replicas = 0)");
        execute("insert into t1 (id, ts) values (1, current_timestamp)");
        execute("refresh table t1");

        execute("select sum(ids) from (select date_trunc('day', ts), count(distinct id) as ids from t1 group by 1) tt");
        assertThat(printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testGlobalAggregationOnNestedSubQueryWithGlobalAggregation() throws Exception {
        execute("create table t (x int)");
        ensureYellow();
        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");

        execute("select sum(x) from (select min(x) as x from (select max(x) as x from t) as t) as t");
        assertThat(printedTable(response.rows()), is("2\n"));
    }

    @Test
    public void testGlobalAggOnJoinSubQueryWithScalarSubQueries() throws Exception {
        execute("select sum(x) from (" +
                "   select t1.col1 as x from unnest([1, 1]) t1, unnest([1, 1]) t2 " +
                "       where t1.col1 = (select 1) " +
                "       order by x limit 3" +
                ") t");
        assertThat(printedTable(response.rows()), is("3\n"));
    }

    @Test
    public void testGlobalAggOnSubQueryWithWhereOnOuterRelation() throws Exception {
        execute("select sum(x) from (select min(col1) as x from unnest([1])) as t where x = 2");
        assertThat(printedTable(response.rows()), is("NULL\n"));
    }

    @Test
    public void testSubQueryInSelectListOnDocTable() throws Exception {
        execute("create table t (x int)");
        ensureYellow();
        execute("insert into t (x) values (1), (1)");
        execute("refresh table t");

        execute("select (select 2), x from t");
        assertThat(printedTable(response.rows()),
            is("2| 1\n" +
               "2| 1\n"));
    }

    @Test
    public void testSimpleSelectOnSubQueryWithOrderByAndLimit() throws Exception {
        execute("select col1 from (" +
                "   select col1 from unnest([1, 2, 3, 4]) order by col1 asc limit 3" +
                ") t order by col1 desc limit 1");
        assertThat(printedTable(response.rows()), is("3\n"));
    }

    @Test
    public void testSimpleSelectOnSubQueryWithFetchPushDown() throws Exception {
        execute("create table t (x int, y int)");
        ensureYellow();
        execute("insert into t (x, y) values (10, 20), (30, 40), (50, 60)");
        execute("refresh table t");

        execute("select x, y from (" +
                "   select x, y from t order by x limit 2) t " +
                "order by y desc limit 1");
        assertThat(printedTable(response.rows()), is("30| 40\n"));
    }

    @Test
    public void testSimpleSelectOnSubQueryWithWhereClause() throws Exception {
        execute("create table t (x int, y int)");
        ensureYellow();
        execute("insert into t (x, y) values (10, 20), (30, 40), (50, 60)");
        execute("refresh table t");

        execute("select x, y from (" +
                "   select x, y from t order by x limit 3) t " +
                "where x = 30 order by y desc limit 2");
        assertThat(printedTable(response.rows()), is("30| 40\n"));
    }

    @Test
    public void testNestedSimpleSubSelectWhichWhereFetchPropagationIsPossible() throws Exception {
        execute("create table t (x int, y int)");
        ensureYellow();
        execute("insert into t (x, y) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
        execute("refresh table t");

        // this re-orders columns and contains scalar functions to handle a more-complex case
        execute("select xx, yy from (" +
                "   select y + y as yy, x + x as xx from (" +
                "       select x, y from t order by x asc limit 4" +
                "   ) tt " +
                "   order by tt.x desc limit 3" +
                ") ttt " +
                "where ttt.xx = 4 or ttt.xx = 6 order by ttt.xx asc limit 2");
        assertThat(printedTable(response.rows()),
            is("4| 40\n" +
               "6| 60\n"));
    }

    @Test
    public void testNestedSimpleSubSelectNoFetchPropagationAsWhereIsOnNonQuerySymbol() throws Exception {
        execute("create table t (x int, y int)");
        ensureYellow();
        execute("insert into t (x, y) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
        execute("refresh table t");

        // this re-orders columns and contains scalar functions to handle a more-complex case
        execute("select xx, yy from (" +
                "   select y + y as yy, x + x as xx from (" +
                "       select x, y from t order by x asc limit 4" +
                "   ) tt " +
                "   order by tt.x desc limit 3" +
                ") ttt " +
                "where ttt.yy = 40 or ttt.yy = 60 order by ttt.xx asc limit 2");
        assertThat(printedTable(response.rows()),
            is("4| 40\n" +
               "6| 60\n"));
    }

    @Test
    public void testCountWithOneRowSubselect() throws Exception {
        setup.setUpLocations();
        execute("refresh table locations");
        execute("select count(*) from locations where position > (select max(position) from locations) - 2");
        assertThat(response.rows()[0][0], is(2L));
    }
}
