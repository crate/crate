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
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.data.Paging;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.metadata.RelationName;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.testing.Asserts;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.types.DataTypes;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SubSelectIntegrationTest extends IntegTestCase {

    private final Setup setup = new Setup(sqlExecutor);

    @Test
    public void testSubSelectOrderBy() throws Exception {
        setup.setUpCharacters();

        execute("select i, name from (select id as i, name from characters order by name) as ch order by i desc");
        assertThat(response).hasRows(
            "4| Arthur",
            "3| Trillian",
            "2| Ford",
            "1| Arthur"
        );
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_sub_select_order_by_and_limit_using_query_then_fetch() throws Exception {
        execute("create table doc.tbl (ord int, name text)");
        execute("insert into doc.tbl (ord, name) values (?, ?)", $$(
            $(4, "Arthur"),
            $(3, "Trillian"),
            $(2, "Ford"),
            $(1, "Arthur")
        ));
        execute("refresh table doc.tbl");
        execute("explain (costs false) select i, name from (select ord as i, name from doc.tbl order by name) as t order by i desc limit 20");
        assertThat(response).hasLines(
            "Rename[i, name] AS t",
            "  └ Fetch[ord AS i, name]",
            "    └ Limit[20::bigint;0]",
            "      └ OrderBy[ord AS i DESC]",
            "        └ Collect[doc.tbl | [_fetchid, ord AS i] | true]"
        );

        execute("select i, name from (select ord as i, name from doc.tbl order by name) as t order by i desc limit 20");
        assertThat(response).hasRows(
            "4| Arthur",
            "3| Trillian",
            "2| Ford",
            "1| Arthur"
        );
    }

    @Test
    public void testSubSelectWhere() throws Exception {
        setup.setUpCharacters();

        execute("select id, name " +
                "from (select * from characters where female = true) as ch " +
                "where name like 'Arthur'");
        assertThat(response).hasRows(
            "4| Arthur");
    }

    @Test
    public void testSubSelectWhereDocKey() throws Exception {
        setup.setUpCharacters();

        execute("select id, name " +
                "from (select * from characters where female = true) as ch " +
                "where id = 4");
        assertThat(response).hasRows(
            "4| Arthur");
    }

    @Test
    public void testSubSelectLimitOffset() throws Exception {
        setup.setUpLocations();
        execute("refresh table locations");

        execute("select name " +
                "from (select * from locations order by name limit 10 offset 5) as l " +
                "limit 5 offset 4");
        assertThat(response).hasRows(
            "End of the Galaxy",
            "Galactic Sector QQ7 Active J Gamma",
            "North West Ripple",
            "Outer Eastern Rim");
    }

    @Test
    public void testSubSelectOutputs() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("refresh table t1");

        execute("select aa, (xxi + 1) " +
                "from (select (xx + i) as xxi, concat(a, a) as aa " +
                " from (select a, i, (x + x) as xx from t1) as t) as tt " +
                "order by aa");

        assertThat(response).hasRows(
            "aa| 9",
            "bb| 14",
            "cc| 20",
            "dd| 30");
    }

    @Test
    public void testReferenceToNestedField() throws Exception {
        setup.groupBySetup();

        execute("select gender, minAge from ( " +
                "  select gender, min(age) as minAge from characters group by gender" +
                ") as ch " +
                "where gender = 'male'");
        assertThat(response).hasRows(
            "male| 34"
        );
    }

    @Test
    public void testReferenceToNestedAggregatedField() throws Exception {
        setup.groupBySetup();
        execute("select gender, minAge from ( " +
                "  select gender, min(age) as minAge from characters group by gender" +
                ") as ch " +
                "where (minAge * 2) < 120 order by gender");
        assertThat(response).hasRows(
            "female| 32",
            "male| 34"
        );
    }

    @Test
    public void testNestedGroupByAggregation() throws Exception {
        setup.groupBySetup();
        execute("select count(*) from (" +
                "  select min(age) as minAge from characters group by gender) as ch " +
                "group by minAge");
        List<Object[]> rows = Arrays.asList(response.rows());
        Collections.sort(rows, OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, true, true));
        assertThat(rows).containsExactly(
            new Object[] { 1L },
            new Object[] { 1L }
        );
    }

    @Test
    public void testOrderingOnNestedAggregation() throws Exception {
        setup.groupBySetup();

        execute("select race, avg(age) as avgAge from ( " +
                "  select * from characters where gender = 'male' order by age) as ch " +
                "group by race");

        List<Object[]> rows = Arrays.asList(response.rows());
        Collections.sort(rows, OrderingByPosition.arrayOrdering(DataTypes.STRING, 0, false, true));
        assertThat(rows).containsExactly(
            new Object[] { "Android", null },
            new Object[] {"Human", 73.0 },
            new Object[] {"Vogon", null }
        );
    }

    @Test
    public void testFilterOnSubSelectWithJoins() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        execute("create table t2 (a string, i integer, y integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("insert into t2 (a, i, y) values ('aa', 22, 33),('bb', 33, 55),('cc', 55, 77),('dd', 77, 111)");
        execute("refresh table t1, t2");

        execute("select col1, col2 from ( " +
                "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
                "  from t1, t2 where t2.y > 60) as t " +
                "where col1 = 'a' order by col3");

        assertThat(response).hasRows(
            "a| 55",
            "a| 77");
    }

    @Test
    public void testNestedSubSelectWithJoins() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        execute("create table t2 (a string, i integer, y integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("insert into t2 (a, i, y) values ('aa', 22, 33),('bb', 33, 55),('cc', 55, 77),('dd', 77, 111)");
        execute("refresh table t1, t2");

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1, t2 where t1.a='a' or t2.a='aa') as t) as tt " +
                "order by aa, xyi");

        assertThat(response).hasRows(
            "aaa| 58",
            "abb| 91",
            "acc| 135",
            "add| 191",
            "baa| 60",
            "caa| 62",
            "daa| 66");
    }

    @Test
    public void testNestedSubSelectWithOuterJoins() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        execute("create table t2 (a string, i integer, y integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("insert into t2 (a, i, y) values ('a', 22, 33),('bb', 33, 55),('cc', 55, 77),('dd', 77, 111)");
        execute("refresh table t1, t2");

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1 left join t2 on t1.a = t2.a where t1.a='a') as t) as tt " +
                "order by aa, xyi");

        assertThat(response).hasRows(
            "aa| 58");

        String stmt =
            "select aa, xyi from (" +
            "  select (xy + i) as xyi, aa from (" +
            "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
            "    from t1 right join t2 on t1.a = t2.a where t1.a='a' or t2.a in ('aa', 'bb')) as t) as tt " +
            "order by aa, xyi";
        assertThat(execute(stmt)).hasRows(
            "aa| 58",
            "bb| NULL"
        );
    }

    @Test
    public void testSingleRowSubselectInWhereClauseOnSysTables() throws Exception {
        assertThat(execute("select 1 where 2 = (select 2)")).hasRowCount(1L);
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
        assertThat(response).hasRows("2");
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
        assertThat(response).hasRows("2");
    }

    @Test
    public void testSingleRowSubSelectInGlobalAggregationWhereClause() throws Exception {
        execute("create table t1 (x long)");
        ensureYellow();
        execute("insert into t1 (x) values (1)");
        execute("refresh table t1");

        execute("select sum(x) from t1 where x = (select 1)");
        assertThat(response).hasRows("1");
    }

    @Test
    public void testSingleRowSubSelectInGroupByWhereClause() throws Exception {
        execute("create table t1 (x long)");
        ensureYellow();
        execute("insert into t1 (x) values (1)");
        execute("refresh table t1");

        execute("select sum(x), x from t1 where x = (select 1) group by x");
        assertThat(response).hasRows("1| 1");
    }

    @Test
    public void testSingleRowSubselectWithMultipleRowsReturning() throws Exception {
        execute("create table t1 (x long)");
        ensureYellow();
        execute("insert into t1 (x) values (1), (2)");
        execute("refresh table t1");

        Asserts.assertSQLError(() -> execute("select name from sys.cluster where 1 = (select x from t1)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Subquery returned more than 1 row");
    }

    @Test
    public void testSingleRowSubSelectCanBeUsedInSelectListAndWhereOfPrimaryKeyLookup() throws Exception {
        execute("create table t1 (x int primary key)");
        ensureYellow();
        execute("insert into t1 (x) values (1), (2)");
        execute("refresh table t1");

        execute("select x, (select 'foo') from t1 where x = (select 1)");
        assertThat(response).hasRows("1| foo");
    }

    @Test
    public void testSingleRowSubSelectWorksWithJoins() throws Exception {
        execute("create table t (x long primary key)");
        ensureYellow();
        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");

        for (TableStats tableStats : cluster().getInstances(TableStats.class)) {
            Map<RelationName, Stats> newStats = new HashMap<>();
            newStats.put(
                new RelationName(sqlExecutor.getCurrentSchema(), "t"),
                new Stats(100, 64, Map.of()));
            tableStats.updateTableStats(newStats);
        }

        // Left table is expected to be one row, due to the single row subselect in the where clause.
        execute("select * from t as t1, t as t2 where t1.x = (select 1) order by t2.x");
        assertThat(response).hasRows(
            "1| 1",
            "1| 2"
        );

        // Left table is expected to be bigger due to the table stats stating it being 100 rows
        execute("select * from t as t2, t as t1 where t1.x = (select 1) order by t2.x");
        assertThat(response).hasRows(
            "1| 1",
            "2| 1"
        );
    }

    @Test
    public void testSubSelectReturnsNoRowIsHandledAsNullValue() throws Exception {
        execute("select name from sys.cluster where name = (select name from sys.nodes where 1 = 2)");
        assertThat(response).hasRowCount(0);

        execute("select name from sys.cluster where (select name from sys.nodes where 1 = 2) is null");
        assertThat(response).hasRowCount(1);
    }

    @Test
    public void testScalarSubqueryCanBeUsedInGroupByAndHaving() throws Exception {
        execute("select (select 'foo'), count(*) from unnest([1, 2]) group by 1 having count(*) = (select 2)");
        assertThat(response).hasRows("foo| 2");
    }

    @Test
    public void testGlobalAggregatesOnSimpleSubQuery() throws Exception {
        execute("create table t (x int)");
        ensureYellow();

        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");

        // orderBy and limit in subQuery to prevent rewrite to non-subquery
        execute("select sum(x) from (select x from t order by x limit 1) as t");
        assertThat(response).hasRows("1");
    }

    @Test
    public void testGlobalAggregateOnVirtualTableWithGroupBy() {
        execute(
            "create table t1 (" +
            "   id int," +
            "   ts timestamp with time zone" +
            ") with (number_of_replicas = 0)");
        execute("insert into t1 (id, ts) values (1, current_timestamp)");
        execute("refresh table t1");

        execute("select sum(ids) from (select date_trunc('day', ts), count(distinct id) as ids from t1 group by 1) tt");
        assertThat(response).hasRows("1");
    }

    @Test
    public void testGlobalAggregationOnNestedSubQueryWithGlobalAggregation() throws Exception {
        execute("create table t (x int)");
        ensureYellow();
        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");

        execute("select sum(x) from (select min(x) as x from (select max(x) as x from t) as t) as t");
        assertThat(response).hasRows("2");
    }

    @Test
    public void testGlobalAggOnJoinSubQueryWithScalarSubQueries() throws Exception {
        execute("select sum(x) from (" +
                "   select t1.t1 as x from unnest([1, 1]) t1, unnest([1, 1]) t2 " +
                "       where t1.t1 = (select 1) " +
                "       order by x limit 3" +
                ") t");
        assertThat(response).hasRows("3");
    }

    @Test
    public void testJoinOnSubQueriesWithLimitAndOffset() {
        execute("create table t1(col1 integer)");
        execute("create table t2(col1 integer)");
        ensureYellow();
        execute("insert into t1(col1) values (1), (2), (2), (3), (3)");
        execute("insert into t2(col1) values (1), (1), (1), (2), (2), (3), (3), (4), (4)");
        execute("refresh table t1, t2");

        execute("select * from" +
                " (select * from t1 order by 1 limit 3 offset 1) t1 inner join " +
                " (select * from t2 order by 1 limit 5 offset 3) t2 " +
                "on t1.col1 = t2.col1 " +
                "order by 1 desc, 2 " +
                "limit 2 offset 1");
        assertThat(response).hasRows(
            "3| 3",
            "2| 2");
    }

    @Test
    public void testJoinOnSubQueriesWithLimitAndOffsetAndPaging() {
        // Test that {@link BatchPagingIterator} can moveToStart() even if iteration hasn't exhausted all rows.
        Paging.PAGE_SIZE = 2;
        execute("create table t1(col1 integer)");
        execute("create table t2(col1 integer)");
        ensureYellow();
        execute("insert into t1(col1) values (1), (2), (2), (3), (3)");
        execute("insert into t2(col1) values (1), (1), (1), (2), (2), (3), (3), (4), (4)");
        execute("refresh table t1, t2");

        execute("select * from" +
                " (select * from t1 order by 1 limit 4 offset 1) t1, " +
                " (select * from t2 order by 1 desc limit 5 offset 1) t2 " +
                "where t1.col1 = t2.col1 " +
                "order by 1 desc, 2 " +
                "limit 4 offset 2");
        assertThat(response).hasRows(
            "3| 3",
            "3| 3",
            "2| 2",
            "2| 2");
    }

    @Test
    public void testJoinWithAggregationsOnSubQueriesWithLimitOffsetAndAggregations() {
        execute("create table t1(col1 integer)");
        execute("create table t2(col1 integer)");
        ensureYellow();
        execute("insert into t1(col1) values (1), (2), (2), (3), (3)");
        execute("insert into t2(col1) values (1), (1), (1), (2), (2), (3), (3), (4), (4)");
        execute("refresh table t1, t2");

        execute("select * from" +
                " (select distinct col1 from t1 order by 1 limit 2 offset 1) t1, " +
                " (select col1, count(*) as cnt from t2 group by col1 order by 2, 1 limit 2 offset 2) t2 " +
                "where t1.col1 = t2.cnt::integer " +
                "order by 1 desc, 2, 3 " +
                "limit 1 offset 1");
        assertThat(response).hasRows("2| 4| 2");
    }

    @Test
    public void testGlobalAggOnSubQueryWithWhereOnOuterRelation() throws Exception {
        execute("select sum(x) from (select min(unnest) as x from unnest([1])) as t where x = 2");
        assertThat(response).hasRows("NULL");
    }

    @Test
    public void testSubQueryInSelectListOnDocTable() throws Exception {
        execute("create table t (x int)");
        ensureYellow();
        execute("insert into t (x) values (1), (1)");
        execute("refresh table t");

        execute("select (select 2), x from t");
        assertThat(response).hasRows(
            "2| 1",
            "2| 1"
        );
    }

    @Test
    public void testSimpleSelectOnSubQueryWithOrderByAndLimit() throws Exception {
        execute("select unnest from (" +
                "   select unnest from unnest([1, 2, 3, 4]) order by unnest asc limit 3" +
                ") t order by unnest desc limit 1");
        assertThat(response).hasRows("3");
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
        assertThat(response).hasRows("30| 40");
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
        assertThat(response).hasRows("30| 40");
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
        assertThat(response).hasRows(
            "4| 40",
            "6| 60");
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
        assertThat(response).hasRows(
            "4| 40",
               "6| 60");
    }

    @Test
    public void testCountWithOneRowSubselect() throws Exception {
        setup.setUpLocations();
        execute("refresh table locations");
        execute("select count(*) from locations where position > (select max(position) from locations) - 2");
        assertThat(response.rows()[0][0]).isEqualTo(2L);
    }

    @Test
    public void testSubqueryExpressionWithInPredicateLeftFieldSymbol() throws Exception {
        setup.setUpCharacters();
        execute("select id, name from characters where id in (select unnest from unnest([1,2,3])) order by id");
        assertThat(response).hasRows(
            "1| Arthur",
            "2| Ford",
            "3| Trillian"
        );
    }

    @Test
    public void testSubqueryExpressionWithInPredicateLeftValueSymbol() throws Exception {
        execute("select 1 in (select unnest from unnest([1,2,3]))");
        assertThat(response).hasRows("true");
    }

    @Test
    public void testSubqueryExpressionWithInPredicateEvaluatesToNull() throws Exception {
        execute("select 1 in (select unnest from unnest([2, cast(null as long)]))");
        assertThat(response).hasRowCount(1L);
        assertThat(response.rows()[0][0]).isNull();

        execute("select NULL in (select unnest from unnest([1,2]))");
        assertThat(response).hasRowCount(1L);
        assertThat(response.rows()[0][0]).isNull();
    }

    @Test
    public void testNestedSubqueryWithAggregatesInMultipleStages() throws Exception {
        setup.setUpJobs();
        setup.setUpEmployees();

        execute(
            "select department, avg(income) from employees" +
            "   where income <= ANY (" +
            "       select avg(min_salary) from jobs" +
            "       where id in (" +
            "           select job_id from job_history where from_ts between '2014-01-01' and '2017-12-31'" +
            "       )" +
            "   )" +
            "   group by department" +
            "   order by avg(income) desc"
        );
        assertThat(response).hasRows(
            "engineering| 5000.0",
            "HR| 0.5"
        );
    }

    @Test
    public void testJoiningSubqueries() throws Exception {
        setup.setUpJobs();
        setup.setUpEmployees();

        execute("select employees.name, employees.department " +
                "from employees, " +
                "     (select jobs.department from jobs) sub " +
                "where employees.department = sub.department " +
                "order by 1, 2");
        assertThat(response).hasRowCount(6L);
        assertThat(response).hasRows(
            "asok| internship",
            "catbert| HR",
            "dilbert| engineering",
            "pointy haired boss| management",
            "ratbert| HR",
            "wally| engineering"
        );
    }

    @Test
    public void testSubqueryWithNestedEquiJoin() throws Exception {
        setup.setUpJobs();
        setup.setUpEmployees();

        execute(
            "select name, income from employees" +
            "   where hired <= ANY (" +
            "       select jh.from_ts from job_history jh" +
            "       join jobs j on jh.job_id = j.id" +
            "       where j.department = 'HR'" +
            "   )" +
            "   and department = 'HR'" +
            "   order by income desc"
        );
        assertThat(response).hasRows(
            "catbert| 9.9999999999E8"
        );
    }

    @Test
    public void testSelectWithTwoInOnSubQueryThatCanBeRewrittenToSemiJoins() throws Exception {
        execute(
            "select * from unnest([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) t1 " +
            "where " +
            "   t1 in (select unnest from unnest([1, 2, 4, 5, 6])) " +
            "   and t1 in (select unnest from unnest([4, 5, 6])) " +
            "order by t1 "
        );
        assertThat(response).hasRows(
            "4",
            "5",
            "6"
        );
    }

    /**
     * Test that results from subQueries are bound to the parent query's where clause
     * BEFORE creating any execution phase (for this test case: before resolving the routing)
     */
    @Test
    public void testWhereSubsSelectAsClusteredByValue() {
        execute("create table t1 (id int, r int) clustered by(r)");
        execute("insert into t1 (id, r) values (1, 1), (2, 2)");
        execute("refresh table t1");
        execute("select id from t1 where r = (select r from t1 where id = 1)");
        assertThat(response).hasRowCount(1L);

        execute("select count(*) from t1 where r = (select r from t1 where id = 1)");
        assertThat(response).hasRowCount(1L);
    }

    @Test
    public void testSubscriptOnSubSelect() {
        execute("create table t1 (a object, c object)");
        execute("insert into t1 (a, c) values ({ b = 1 }, { d = { e = 2 }})");
        execute("refresh table t1");
        execute("select a['b'], c['d']['e'] from (select * from t1) t2");
        assertThat(response).hasRows(
            "1| 2");
    }

    @Test
    public void testSubscriptOnSubSelectFromUnnestWithObjectLiteral() {
        execute("select unnest['b'] from (select * from unnest([{b=1}])) t1");
        assertThat(response).hasRows(
            "1"
        );
    }

    @Test
    public void testOrderByFunctionWithColumnOfSubSelect() {
        execute("create table t1 (id int)");
        execute("insert into t1 (id) values (1), (2)");
        execute("refresh table t1");
        execute("select id + 1 from (select id from t1) tt order by 1 desc");
        assertThat(response).hasRows(
            "3",
            "2");
    }

    @Test
    public void test_quoted_subscript_with_sub_select() {
        execute("create table nested_obj (o object as (a object as (b object as (c int))))");
        execute("insert into nested_obj (o) values ({a = {b = {c = 1}}})");
        execute("refresh table nested_obj");

        execute("select \"o['a']['b']['c']\" from (" +
                "select o, o['a']['b']['c'] from nested_obj" +
                ") nobj");
        assertThat(response).hasRows("1");
    }

    @Test
    public void test_non_recursive_with_query() {
        setup.setUpCharacters();

        execute("WITH ch AS (SELECT * FROM characters WHERE female = true) " +
            "SELECT id, name " +
            "FROM ch " +
            "WHERE name LIKE 'Arthur'");
        assertThat(response).hasRows(
            "4| Arthur");
    }

    @Test
    public void test_exists_with_subquery_returns_result_if_subquery_has_rows() {
        String stmt = "SELECT mountain, height FROM sys.summits WHERE EXISTS (SELECT 1) order by height desc limit 2";
        execute(stmt);
        assertThat(response).hasRows(
            "Mont Blanc| 4808",
            "Monte Rosa| 4634"
        );
        stmt = """
            SELECT mountain FROM sys.summits
            WHERE EXISTS (SELECT 1 FROM sys.cluster WHERE 1 = 2) order by height desc limit 2
            """;
        execute(stmt);
        assertThat(response.rowCount()).isEqualTo(0L);

        stmt = """
            SELECT
                table_name
            FROM
                information_schema.tables t
            WHERE
                EXISTS
                (
                 SELECT 1
                 FROM information_schema.columns c
                 WHERE
                    c.table_schema = t.table_schema AND
                    c.table_name = t.table_name
                )
                LIMIT 15
            """;
        execute(stmt);
        assertThat(response.rowCount()).isEqualTo(15L);
    }
}
