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

import io.crate.data.CollectionBucket;
import io.crate.execution.engine.join.RamBlockSizeCalculator;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.metadata.RelationName;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseRandomizedSchema;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printRows;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class JoinIntegrationTest extends SQLIntegrationTestCase {

    @After
    public void resetStatsAndBreakerSettings() {
        resetTableStats();
        execute("reset global indices");
    }

    @Test
    public void testCrossJoinOrderByOnBothTables() throws Exception {
        createColorsAndSizes();
        execute("select colors.name, sizes.name from colors, sizes order by colors.name, sizes.name");
        assertThat(printedTable(response.rows()), is(
            "blue| large\n" +
            "blue| small\n" +
            "green| large\n" +
            "green| small\n" +
            "red| large\n" +
            "red| small\n"));
    }

    @Test
    public void testCrossJoinOrderByOnOneTableWithLimit() throws Exception {
        createColorsAndSizes();
        execute("select colors.name, sizes.name from colors, sizes order by sizes.name, colors.name limit 4");
        assertThat(printedTable(response.rows()), is("" +
                                                     "blue| large\n" +
                                                     "green| large\n" +
                                                     "red| large\n" +
                                                     "blue| small\n"));
    }

    @Test
    public void testInsertFromCrossJoin() throws Exception {
        createColorsAndSizes();
        execute("create table target (color string, size string)");
        ensureYellow();

        execute("insert into target (color, size) (select colors.name, sizes.name from colors cross join sizes)");
        execute("refresh table target");

        execute("select color, size from target order by size, color limit 4");
        assertThat(printedTable(response.rows()), is("" +
                                                     "blue| large\n" +
                                                     "green| large\n" +
                                                     "red| large\n" +
                                                     "blue| small\n"));
    }

    @Test
    public void testInsertFromInnerJoin() throws Exception {
        execute("create table t1 (x int)");
        execute("create table t2 (y int)");
        execute("create table target (x int, y int)");
        ensureYellow();

        execute("insert into t1 (x) values (1), (2)");
        execute("insert into t2 (y) values (2), (3)");
        execute("refresh table t1, t2");

        execute("insert into target (x, y) (select t1.x, t2.y from t1 inner join t2 on t1.x = t2.y)");
        execute("refresh table target");

        execute("select x, y from target order by x, y");
        assertThat(printedTable(response.rows()), is("2| 2\n"));
    }

    @Test
    public void testInsertFromInnerJoinUsing() throws Exception {
        execute("create table t1 (num int, value string)");
        execute("create table t2 (num int, value string, qty int)");
        ensureYellow();
        execute("insert into t1 (num, value) values (1, 'xxx'), (3, 'yyy'), (5, 'zzz')");
        execute("insert into t2 (num, value, qty) values (1, 'xxx', 12), (3, 'yyy', 7), (5, 'zzz', 0)");
        execute("refresh table t1, t2");
        ensureGreen();
        execute("SELECT * FROM t1 AS rel1 INNER JOIN t2 USING (num, value) ORDER BY rel1.num");
        assertThat(printedTable(response.rows()), is(
            "1| xxx| 1| xxx| 12\n" +
            "3| yyy| 3| yyy| 7\n" +
            "5| zzz| 5| zzz| 0\n"));
        execute("SELECT * FROM (SELECT * FROM t1) AS rel1 JOIN (SELECT * FROM t2) AS rel2 USING (num, value) ORDER BY rel1.num;");
        assertThat(printedTable(response.rows()), is(
            "1| xxx| 1| xxx| 12\n" +
            "3| yyy| 3| yyy| 7\n" +
            "5| zzz| 5| zzz| 0\n"));
    }

    @Test
    public void testJoinOnEmptyPartitionedTablesWithAndWithoutJoinCondition() throws Exception {
        execute("create table foo (id long) partitioned by (id)");
        execute("create table bar (id long) partitioned by (id)");
        ensureYellow();
        execute("select * from foo f, bar b where f.id = b.id");
        assertThat(printedTable(response.rows()), is(""));

        execute("select * from foo f, bar b");
        assertThat(printedTable(response.rows()), is(""));
    }

    @Test
    public void testCrossJoinJoinUnordered() throws Exception {
        execute("create table employees (size float, name string) clustered by (size) into 1 shards");
        execute("create table offices (height float, name string) clustered by (height) into 1 shards");
        execute("insert into employees (size, name) values (1.5, 'Trillian')");
        execute("insert into offices (height, name) values (1.5, 'Hobbit House')");
        execute("refresh table employees, offices");

        // which employee fits in which office?
        execute("select employees.name, offices.name from employees, offices limit 1");
        assertThat(response.rows().length, is(1));
    }

    @Test
    public void testCrossJoinWithFunction() throws Exception {
        execute("create table t1 (price float)");
        execute("create table t2 (price float)");
        ensureYellow();
        execute("insert into t1 (price) values (20.3), (15.0)");
        execute("insert into t2 (price) values (28.3)");
        execute("refresh table t1, t2");

        execute("select round(t1.price * t2.price) as total_price from t1, t2 order by total_price");
        assertThat(printedTable(response.rows()), is("425\n574\n"));
    }

    @Test
    public void test_cross_join_with_order_by_alias_and_order() throws Exception {
        execute("create table t1 (price int)");
        execute("create table t2 (price int)");
        ensureYellow();
        execute("insert into t1 (price) values (1)");
        execute("insert into t2 (price) values (2)");
        execute("refresh table t1, t2");

        execute("select t1.price as total_price from t1 cross join t2 order by total_price limit 10");
        assertThat(printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testOrderByWithMixedRelationOrder() throws Exception {
        execute("create table t1 (price float)");
        execute("create table t2 (price float, name string)");
        ensureYellow();
        execute("insert into t1 (price) values (20.3), (15.0)");
        execute("insert into t2 (price, name) values (28.3, 'foobar'), (40.1, 'bar')");
        execute("refresh table t1, t2");

        execute("select t2.price, t1.price, name from t1, t2 order by t2.price, t1.price, t2.name");
        assertThat(printedTable(response.rows()), is("" +
                                                     "28.3| 15.0| foobar\n" +
                                                     "28.3| 20.3| foobar\n" +
                                                     "40.1| 15.0| bar\n" +
                                                     "40.1| 20.3| bar\n"));
    }

    @Test
    public void testOrderByNoneSelectedField() throws Exception {
        execute("create table colors (name string)");
        execute("create table articles (price float, name string)");
        ensureYellow();
        execute("insert into colors (name) values ('black'), ('grey')");
        execute("insert into articles (price, name) values (28.3, 'towel'), (40.1, 'cheese')");
        execute("refresh table colors, articles");

        execute("select colors.name, articles.name from colors, articles order by articles.price, colors.name, articles.name");
        assertThat(printedTable(response.rows()), is("" +
                                                     "black| towel\n" +
                                                     "grey| towel\n" +
                                                     "black| cheese\n" +
                                                     "grey| cheese\n"));

    }

    @Test
    public void testCrossJoinWithoutLimitAndOrderByAndCrossJoinSyntax() throws Exception {
        createColorsAndSizes();
        execute("select colors.name, sizes.name from colors cross join sizes");
        assertThat(response.rowCount(), is(6L));

        List<Object[]> rows = Arrays.asList(response.rows());
        Collections.sort(rows, OrderingByPosition.arrayOrdering(
            new int[]{0, 1}, new boolean[]{false, false}, new boolean[]{false, false}));
        assertThat(printRows(rows), is(
            "blue| large\n" +
            "blue| small\n" +
            "green| large\n" +
            "green| small\n" +
            "red| large\n" +
            "red| small\n"
        ));
    }

    @Test
    public void testOutputFromOnlyOneTable() throws Exception {
        createColorsAndSizes();
        execute("select colors.name from colors, sizes order by colors.name");
        assertThat(response.rowCount(), is(6L));
        assertThat(printedTable(response.rows()), is("" +
                                                     "blue\n" +
                                                     "blue\n" +
                                                     "green\n" +
                                                     "green\n" +
                                                     "red\n" +
                                                     "red\n"));
    }

    @Test
    public void testCrossJoinWithSysTable() throws Exception {
        execute("create table t (name string) clustered into 3 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t values ('foo'), ('bar')");
        execute("refresh table t");

        execute("select shards.id, t.name from sys.shards, t where shards.table_name = 't' order by shards.id, t.name");
        assertThat(response.rowCount(), is(6L));
        assertThat(printedTable(response.rows()), is("" +
                                                     "0| bar\n" +
                                                     "0| foo\n" +
                                                     "1| bar\n" +
                                                     "1| foo\n" +
                                                     "2| bar\n" +
                                                     "2| foo\n"));
    }

    @Test
    public void testJoinOnSysTables() throws Exception {
        execute("select column_policy, column_name from information_schema.tables, information_schema.columns " +
                "where " +
                "tables.table_schema = 'sys' " +
                "and tables.table_name = 'shards' " +
                "and tables.table_schema = columns.table_schema " +
                "and tables.table_name = columns.table_name " +
                "order by columns.column_name " +
                "limit 5");
        assertThat(response.rowCount(), is(5L));
        assertThat(printedTable(response.rows()),
            is("strict| blob_path\n" +
               "strict| closed\n" +
               "strict| flush_stats\n" +
               "strict| flush_stats['count']\n" +
               "strict| flush_stats['periodic_count']\n"));
    }

    @Test
    public void testCrossJoinSysTablesOnly() throws Exception {
        execute("create table t (name string) clustered into 3 shards with (number_of_replicas = 0)");
        ensureYellow();

        execute("select s1.id, s2.id, s1.table_name from sys.shards s1, sys.shards s2 order by s1.id asc, s2.id desc");
        assertThat(response.rowCount(), is(9L));
        assertThat(printedTable(response.rows()), is("" +
                                                     "0| 2| t\n" +
                                                     "0| 1| t\n" +
                                                     "0| 0| t\n" +
                                                     "1| 2| t\n" +
                                                     "1| 1| t\n" +
                                                     "1| 0| t\n" +
                                                     "2| 2| t\n" +
                                                     "2| 1| t\n" +
                                                     "2| 0| t\n"));

        execute("select s1.id, s2.id, s1.table_name from sys.shards s1, sys.shards s2");
        assertThat(response.rowCount(), is(9L));

        List<Object[]> rows = Arrays.asList(response.rows());
        Collections.sort(rows, OrderingByPosition.arrayOrdering(
            new int[]{0, 1}, new boolean[]{false, true}, new boolean[]{false, true}));

        assertThat(printedTable(new CollectionBucket(rows)),
            is("" +
               "0| 2| t\n" +
               "0| 1| t\n" +
               "0| 0| t\n" +
               "1| 2| t\n" +
               "1| 1| t\n" +
               "1| 0| t\n" +
               "2| 2| t\n" +
               "2| 1| t\n" +
               "2| 0| t\n"));
    }

    @Test
    public void testJoinUsingSubscriptInQuerySpec() {
        execute("create table t1 (id int, a object as (b int))");
        execute("create table t2 (id int)");
        execute("insert into t1 (id, a) values (1, {b=1})");
        execute("insert into t2 (id) values (1)");
        refresh();
        execute("select t.id, tt.id from t1 as t, t2 as tt where tt.id = t.a['b']");
        assertThat(printedTable(response.rows()), is("1| 1\n"));
    }

    @Test
    public void testCrossJoinFromInformationSchemaTable() throws Exception {
        // sys table with doc granularity on single node
        execute("select * from information_schema.schemata t1, information_schema.schemata t2 " +
                "order by t1.schema_name, t2.schema_name");
        assertThat(response.rowCount(), is(25L));
        assertThat(printedTable(response.rows()),
            is("" +
               "blob| blob\n" +
               "blob| doc\n" +
               "blob| information_schema\n" +
               "blob| pg_catalog\n" +
               "blob| sys\n" +
               "doc| blob\n" +
               "doc| doc\n" +
               "doc| information_schema\n" +
               "doc| pg_catalog\n" +
               "doc| sys\n" +
               "information_schema| blob\n" +
               "information_schema| doc\n" +
               "information_schema| information_schema\n" +
               "information_schema| pg_catalog\n" +
               "information_schema| sys\n" +
               "pg_catalog| blob\n" +
               "pg_catalog| doc\n" +
               "pg_catalog| information_schema\n" +
               "pg_catalog| pg_catalog\n" +
               "pg_catalog| sys\n" +
               "sys| blob\n" +
               "sys| doc\n" +
               "sys| information_schema\n" +
               "sys| pg_catalog\n" +
               "sys| sys\n"));
    }

    @Test
    public void testSelfJoin() throws Exception {
        execute("create table t (x int)");
        ensureYellow();
        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");
        execute("select * from t as t1, t as t2");
        assertThat(response.rowCount(), is(4L));
        assertThat(Arrays.asList(response.rows()), containsInAnyOrder(new Object[]{1, 1},
            new Object[]{1, 2},
            new Object[]{2, 1},
            new Object[]{2, 2}));
    }

    @Test
    public void testSelfJoinWithOrder() throws Exception {
        execute("create table t (x int)");
        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");
        execute("select * from t as t1, t as t2 order by t1.x, t2.x");
        assertThat(printedTable(response.rows()), is("1| 1\n" +
                                                     "1| 2\n" +
                                                     "2| 1\n" +
                                                     "2| 2\n"));
    }

    @Test
    public void test_self_join_with_order_and_limit_is_executed_with_qtf() throws Exception {
        execute("create table doc.t (x int, y int)");
        execute("insert into doc.t (x, y) values (1, 10), (2, 20)");
        execute("refresh table doc.t");
        execute("explain select * from doc.t as t1, doc.t as t2 order by t1.x, t2.x limit 3");
        assertThat(printedTable(response.rows()), is(
            "Fetch[x, y, x, y]\n" +
            "  └ Limit[3::bigint;0]\n" +
            "    └ OrderBy[x ASC x ASC]\n" +
            "      └ NestedLoopJoin[CROSS]\n" +
            "        ├ Rename[t1._fetchid, x] AS t1\n" +
            "        │  └ Collect[doc.t | [_fetchid, x] | true]\n" +
            "        └ Rename[t2._fetchid, x] AS t2\n" +
            "          └ Collect[doc.t | [_fetchid, x] | true]\n"
        ));
        execute("select * from doc.t as t1, doc.t as t2 order by t1.x, t2.x limit 3");
        assertThat(printedTable(response.rows()), is(
            "1| 10| 1| 10\n" +
            "1| 10| 2| 20\n" +
            "2| 20| 1| 10\n"
        ));
    }

    @Test
    public void testFilteredSelfJoin() throws Exception {
        execute("create table employees (salary float, name string)");
        ensureYellow();
        execute("insert into employees (salary, name) values (600, 'Trillian'), (200, 'Ford Perfect'), (800, 'Douglas Adams')");
        execute("refresh table employees");

        execute("select more.name, less.name, (more.salary - less.salary) from employees as more, employees as less " +
                "where more.salary > less.salary " +
                "order by more.salary desc, less.salary desc");
        assertThat(printedTable(response.rows()), is("Douglas Adams| Trillian| 200.0\n" +
                                                     "Douglas Adams| Ford Perfect| 600.0\n" +
                                                     "Trillian| Ford Perfect| 400.0\n"));
    }

    @Test
    public void testFilteredSelfJoinWithFilterOnBothRelations() {
        execute("create table test(id long primary key, num long, txt string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test(id, num, txt) values(1, 1, '1111'), (2, 2, '2222'), (3, 1, '2222'), (4, 2, '2222')");
        execute("refresh table test");

        execute("select t1.id, t2.id from test as t1 inner join test as t2 on t1.num = t2.num " +
                "where t1.txt = '1111' and t2.txt='2222'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("1| 3\n"));
    }

    @Test
    public void testFilteredJoin() throws Exception {
        execute("create table employees (size float, name string)");
        execute("create table offices (height float, name string)");
        ensureYellow();
        execute("insert into employees (size, name) values (1.5, 'Trillian'), (1.3, 'Ford Perfect'), (1.96, 'Douglas Adams')");
        execute("insert into offices (height, name) values (1.5, 'Hobbit House'), (1.6, 'Entresol'), (2.0, 'Chief Office')");
        execute("refresh table employees, offices");

        // which employee fits in which office?
        execute("select employees.name, offices.name from employees inner join offices on size < height " +
                "where size < height order by height - size limit 3");
        assertThat(printedTable(response.rows()), is("" +
                                                     "Douglas Adams| Chief Office\n" +
                                                     "Trillian| Entresol\n" +
                                                     "Ford Perfect| Hobbit House\n"));
    }

    @Test
    public void testFetchWithoutOrder() throws Exception {
        createColorsAndSizes();
        execute("select colors.name, sizes.name from colors, sizes limit 3");
        assertThat(response.rowCount(), is(3L));
    }

    @Test
    public void testJoinWithFunctionInOutputAndOrderBy() throws Exception {
        createColorsAndSizes();
        execute("select substr(colors.name, 0, 1), sizes.name from colors, sizes order by colors.name, sizes.name limit 3");
        assertThat(printedTable(response.rows()),
            is("b| large\n" +
               "b| small\n" +
               "g| large\n"));
    }

    private void createColorsAndSizes() {
        execute("create table colors (name string) ");
        execute("create table sizes (name string) ");
        ensureYellow();

        execute("insert into colors (name) values (?)", new Object[][]{
            new Object[]{"red"},
            new Object[]{"blue"},
            new Object[]{"green"}
        });
        execute("insert into sizes (name) values (?)", new Object[][]{
            new Object[]{"small"},
            new Object[]{"large"},
        });
        execute("refresh table colors, sizes");
    }

    @Test
    public void testJoinTableWithEmptyRouting() throws Exception {
        // no shards in sys.shards -> empty routing
        execute("SELECT s.id, n.id, n.name FROM sys.shards s, sys.nodes n");
        assertThat(response.cols(), arrayContaining("id", "id", "name"));
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testFilteredJoinWithPartitionsAndSelectFromOnlyOneTable() throws Exception {
        execute("create table users ( " +
                "id int primary key, " +
                "name string, " +
                "gender string primary key" +
                ") partitioned by (gender) with (number_of_replicas = 0)");
        execute("create table events ( " +
                "name string, " +
                "user_id int)");
        ensureYellow();

        execute("insert into users (id, name, gender) values " +
                "(1, 'Arthur', 'male'), " +
                "(2, 'Trillian', 'female'), " +
                "(3, 'Marvin', 'android'), " +
                "(4, 'Slartibartfast', 'male')");

        execute("insert into events (name, user_id) values ('a', 1), ('a', 2), ('b', 1)");
        execute("refresh table users, events");
        ensureYellow(); // wait for shards of new partitions

        execute("select users.* from users join events on users.id = events.user_id order by users.id");
    }

    @Test
    public void testJoinWithFilterAndJoinCriteriaNotInOutputs() throws Exception {
        execute("create table t_left (id long primary key, temp float, ref_id int) clustered into 2 shards with (number_of_replicas = 0)");
        execute("create table t_right (id int primary key, name string) clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t_left (id, temp, ref_id) values (1, 23.2, 1), (2, 20.8, 1), (3, 19.7, 1), (4, -0.5, 2), (5, -1.2, 2), (6, 0.2, 2)");
        execute("refresh table t_left");

        execute("insert into t_right (id, name) values (1, 'San Francisco'), (2, 'Vienna')");
        execute("refresh table t_right");


        execute("select temp, name from t_left inner join t_right on t_left.ref_id = t_right.id order by temp");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("-1.2| Vienna\n" +
               "-0.5| Vienna\n" +
               "0.2| Vienna\n" +
               "19.7| San Francisco\n" +
               "20.8| San Francisco\n" +
               "23.2| San Francisco\n"));
    }

    @Test
    public void test3TableCrossJoin() throws Exception {
        execute("create table t1 (x int)");
        execute("create table t2 (x int)");
        execute("create table t3 (x int)");
        ensureYellow();
        execute("insert into t1 (x) values (1)");
        execute("insert into t2 (x) values (2)");
        execute("insert into t3 (x) values (3)");
        execute("refresh table t1, t2, t3");

        execute("select * from t1, t2, t3");
        assertThat(response.rowCount(), is(1L));
        assertThat(TestingHelpers.printedTable(response.rows()), is("1| 2| 3\n"));
    }

    @Test
    public void test3TableJoinWithJoinFilters() throws Exception {
        execute("create table users (id int primary key, name string) with (number_of_replicas = 0)");
        execute("create table events (id int primary key, name string) with (number_of_replicas = 0)");
        execute("create table logs (user_id int, event_id int) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into users (id, name) values (1, 'Arthur'), (2, 'Trillian')");

        execute("insert into events (id, name) values (1, 'Earth destroyed')");
        execute("insert into events (id, name) values (2, 'Hitch hiking on a vogon ship')");
        execute("insert into events (id, name) values (3, 'Meeting Arthur')");

        execute("insert into logs (user_id, event_id) values (1, 1), (1, 2), (2, 3)");

        execute("refresh table users, events, logs");
        execute("select users.name, events.name " +
                "from users " +
                "join logs on users.id = logs.user_id " +
                "join events on events.id = logs.event_id " +
                "order by users.name, events.id");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("Arthur| Earth destroyed\n" +
               "Arthur| Hitch hiking on a vogon ship\n" +
               "Trillian| Meeting Arthur\n"));
    }

    @Test
    public void testFetchArrayAndAnalyzedColumnsWithJoin() throws Exception {
        execute("create table t1 (id int primary key, text string index using fulltext)");
        execute("create table t2 (id int primary key, tags array(string))");
        ensureYellow();
        execute("insert into t1 (id, text) values (1, 'Hello World')");
        execute("insert into t2 (id, tags) values (1, ['foo', 'bar'])");
        execute("refresh table t1, t2");

        execute("select text, tags from t1 join t2 on t1.id = t2.id");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("Hello World| [foo, bar]\n"));
    }

    @Test
    public void test3TableJoinWithFunctionOrderBy() throws Exception {
        execute("create table t1 (x integer)");
        execute("create table t2 (y integer)");
        execute("create table t3 (z integer)");
        ensureYellow();
        execute("insert into t1 (x) values (1)");
        execute("insert into t2 (y) values (2)");
        execute("insert into t3 (z) values (3)");
        execute("refresh table t1, t2, t3");
        execute("select x+y+z from t1,t2,t3 order by x,y,z");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("6\n"));
    }

    @Test
    public void testOrderByExpressionWithMultiRelationSymbol() throws Exception {
        execute("create table t1 (x integer)");
        execute("create table t2 (y integer)");
        execute("create table t3 (z integer)");
        ensureYellow();
        execute("insert into t1 (x) values (3), (1)");
        execute("insert into t2 (y) values (4), (2)");
        execute("insert into t3 (z) values (5), (6)");
        execute("refresh table t1, t2, t3");
        execute("select x,y,z from t1,t2,t3 order by x-y+z, x+y");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("1| 4| 5\n" +
               "1| 4| 6\n" +
               "1| 2| 5\n" +
               "3| 4| 5\n" +
               "1| 2| 6\n" +
               "3| 4| 6\n" +
               "3| 2| 5\n" +
               "3| 2| 6\n"));
    }

    @Test
    public void testSimpleOrderByNonUniqueValues() throws Exception {
        execute("create table t1 (a integer)");
        execute("create table t2 (x integer)");
        ensureYellow();
        execute("insert into t1 (a) values (1), (1), (2), (2)");
        execute("insert into t2 (x) values (1), (2)");
        execute("refresh table t1, t2");
        execute("select a, x from t1, t2 order by a, x");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("1| 1\n" +
               "1| 1\n" +
               "1| 2\n" +
               "1| 2\n" +
               "2| 1\n" +
               "2| 1\n" +
               "2| 2\n" +
               "2| 2\n"));
    }

    @Test
    public void testJoinOnInformationSchema() throws Exception {
        execute("create table t (id string, name string)");
        ensureYellow();
        execute("insert into t (id, name) values ('0-1', 'Marvin')");
        execute("refresh table t");
        execute("select * from t inner join information_schema.tables on t.id = tables.number_of_replicas");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testJoinWithIndexMissingExceptions() throws Throwable {
        execute("create table t1 (x int)");
        execute("create table t2 (x int)");
        ensureYellow();
        execute("insert into t1 (x) values (1)");
        execute("insert into t2 (x) values (2)");
        execute("refresh table t1, t2");

        PlanForNode plan = plan("select * from t1, t2 where t1.x = t2.x");
        execute("drop table t2");

        assertThrowsMatches(() -> execute(plan).getResult(), instanceOf(IndexNotFoundException.class));
    }

    @Test
    public void testAggOnJoin() throws Exception {
        execute("create table t1 (x int)");
        ensureYellow();
        execute("insert into t1 (x) values (1), (2)");
        execute("refresh table t1");

        execute("select sum(t1.x) from t1, t1 as t2");
        assertThat(TestingHelpers.printedTable(response.rows()), is("6\n"));
    }

    @Test
    public void testAggOnJoinWithScalarAfterAggregation() throws Exception {
        execute("select sum(t1.col1) * 2 from unnest([1, 2]) t1, unnest([3, 4]) t2");
        assertThat(TestingHelpers.printedTable(response.rows()), is("12\n"));
    }

    @Test
    public void testAggOnJoinWithHaving() throws Exception {
        execute("select sum(t1.col1) from unnest([1, 2]) t1, unnest([3, 4]) t2 having sum(t1.col1) > 8");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testAggOnJoinWithLimit() throws Exception {
        execute("select " +
                "   sum(t1.col1) " +
                "from unnest([1, 2]) t1, unnest([3, 4]) t2 " +
                "limit 0");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testLimitIsAppliedPostJoin() throws Exception {
        execute("select " +
                "   sum(t1.col1) " +
                "from unnest([1, 1]) t1, unnest([1, 1]) t2 " +
                "limit 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("4\n"));
    }

    @Test
    public void testJoinOnAggWithOrderBy() throws Exception {
        execute("select sum(t1.col1) from unnest([1, 1]) t1, unnest([1, 1]) t2 order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("4\n"));
    }

    @Test
    public void testFailureOfJoinDownstream() throws Exception {
        // provoke an exception when the NL emits a row, must bubble up and NL must stop
        assertThrowsMatches(() -> execute("select cast(R.col2 || ' ' || L.col2 as integer)" +
                                   "   from " +
                                   "       unnest(['hello', 'world'], [1, 2]) L " +
                                   "   inner join " +
                                   "       unnest(['world', 'hello'], [1, 2]) R " +
                                   "   on l.col1 = r.col1 " +
                                   "where r.col1 > 1"),
                     isSQLError(is("Cannot cast value `world` to type `integer`"), INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void testGlobalAggregateMultiTableJoin() throws Exception {
        execute("create table t1 (id int primary key, t2 int, val double)");
        execute("create table t2 (id int primary key, t3 int)");
        execute("create table t3 (id int primary key)");
        ensureYellow();

        execute("insert into t3 (id) values (1), (2)");
        execute("insert into t2 (id, t3) values (1, 1), (2, 1), (3, 2), (3, 4)");
        execute("insert into t1 (id, t2, val) values (1, 1, 0.12), (2, 2, 1.23), (3, 3, 2.34), (4, 4, 3.45)");

        refresh();
        execute("select sum(t1.val), avg(t2.id), min(t3.id) from t1 inner join t2 on t1.t2 = t2.id inner join t3 on t2.t3 = t3.id");
        assertThat(TestingHelpers.printedTable(response.rows()), is("3.69| 2.0| 1\n"));
    }

    @Test
    public void testJoinWithWhereOnPartitionColumnThatDoesNotMatch() throws Exception {
        execute("create table t (id int, p int) clustered into 1 shards partitioned by (p)");
        execute("insert into t (id, p) values (1, 1), (2, 2)");
        ensureYellow();

        // regression test:
        // whereClause with query on partitioned column becomes a noMatch after normalization on collector
        // which leads to using RowsBatchIterator.empty() which always had a columnSize of 0
        execute("select * from t as t1 inner join t as t2 on t1.id = t2.id where t2.p = 2");
    }

    @Test
    public void testJoinOnSimpleVirtualTables() throws Exception {
        execute("select * from " +
                "   (select col1 as x from unnest([1, 2, 3])) t1, " +
                "   (select max(col1) as y from unnest([4])) t2 " +
                "order by t1.x");
        assertThat(printedTable(response.rows()),
            is("1| 4\n" +
               "2| 4\n" +
               "3| 4\n"));
    }

    @Test
    public void testJoinOnComplexVirtualTable() throws Exception {
        execute("create table t1 (x int)");
        ensureYellow();
        execute("insert into t1 (x) values (1), (2), (3), (4)");
        execute("refresh table t1");

        execute("select * from " +
                "   (select x from " +
                "       (select x from t1 order by x asc limit 4) tt1 " +
                "   order by tt1.x desc limit 2 " +
                "   ) ttt1, " +
                "   (select col1 as y from unnest([10])) tt2 ");
        assertThat(printedTable(response.rows()),
            is("4| 10\n" +
               "3| 10\n"));

        execute("select * from " +
                "   (select x from " +
                "       (select x from t1 order by x asc limit 4) tt1 " +
                "   order by tt1.x desc limit 2 " +
                "   ) ttt1, " +
                "   (select max(y) as y from " +
                "       (select min(col1) as y from unnest([10])) tt2 " +
                "   ) ttt2 ");
        assertThat(printedTable(response.rows()),
            is("4| 10\n" +
               "3| 10\n"));
    }

    @Test
    public void testJoinOnVirtualTableWithQTF() throws Exception {
        execute("create table customers (" +
                "id long," +
                "name string," +
                "country string," +
                "company_id long" +
                ")");
        ensureYellow();
        execute("insert into customers (id, name, country, company_id) values(1, 'Marios', 'Greece', 1) ");
        execute("refresh table customers");

        execute("create table orders (" +
                "id long," +
                "customer_id long," +
                "price float" +
                ")");
        ensureYellow();
        execute("insert into orders(id, customer_id, price) values (1,1,20.0), (2,1,10.0), (3,1,30.0), (4,1,40.0), (5,1,50.0)");
        execute("refresh table orders");

        String stmt = "SELECT t1.company_id, t1.country, t1.id, t1.name, t2.customer_id, t2.id, t2.price FROM" +
                      "  customers t1, " +
                      "  (SELECT * FROM (SELECT * from orders order by price desc limit 4) t ORDER BY price limit 3) t2 " +
                      "WHERE t2.customer_id = t1.id " +
                      "order by price limit 3 offset 1";

        execute(stmt);
        assertThat(printedTable(response.rows()),
            is("1| Greece| 1| Marios| 1| 3| 30.0\n" +
               "1| Greece| 1| Marios| 1| 4| 40.0\n"));
    }

    @Test
    public void testJoinOnVirtualTableWithSingleRowSubselect() throws Exception {
        execute("SELECT\n" +
                "        (select min(t1.x) from\n" +
                "            (select col1 as x from unnest([1, 2, 3])) t1,\n" +
                "            (select * from unnest([1, 2, 3])) t2\n" +
                "        ) as min_col1,\n" +
                "        *\n" +
                "    FROM\n" +
                "        unnest([1]) tt1," +
                "        unnest([2]) tt2");
        assertThat(printedTable(response.rows()), is("1| 1| 2\n"));
    }

    @Test
    @UseHashJoins(1)
    public void testInnerEquiJoinUsingHashJoin() {
        execute("create table t1 (a integer)");
        execute("create table t2 (x integer)");
        ensureYellow();
        execute("insert into t1 (a) values (0), (0), (1), (2), (4)");
        execute("insert into t2 (x) values (1), (3), (3), (4), (4)");
        execute("refresh table t1, t2");

        long memoryLimit = 6 * 1024 * 1024;
        double overhead = 1.0d;
        execute("set global \"indices.breaker.query.limit\" = '" + memoryLimit + "b', " +
                "\"indices.breaker.query.overhead\" = " + overhead);
        CircuitBreaker queryCircuitBreaker = internalCluster().getInstance(CircuitBreakerService.class).getBreaker(HierarchyCircuitBreakerService.QUERY);
        randomiseAndConfigureJoinBlockSize("t1", 5L, queryCircuitBreaker);
        randomiseAndConfigureJoinBlockSize("t2", 5L, queryCircuitBreaker);

        execute("select a, x from t1 join t2 on t1.a + 1 = t2.x order by a, x");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("0| 1\n" +
               "0| 1\n" +
               "2| 3\n" +
               "2| 3\n"));
    }

    private void resetTableStats() {
        for (TableStats tableStats : internalCluster().getInstances(TableStats.class)) {
            tableStats.updateTableStats(new HashMap<>());
        }
    }

    @Test
    public void testJoinWithLargerRightBranch() throws Exception {
        execute("create table t1 (a integer)");
        execute("create table t2 (x integer)");
        ensureYellow();
        execute("insert into t1 (a) values (0), (1), (1), (2)");
        execute("insert into t2 (x) values (1), (3), (4), (4), (5), (6)");
        execute("refresh table t1, t2");

        Iterable<TableStats> tableStatsOnAllNodes = internalCluster().getInstances(TableStats.class);
        for (TableStats tableStats : tableStatsOnAllNodes) {
            Map<RelationName, Stats> newStats = new HashMap<>();
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t1"), new Stats(4L, 16L, Map.of()));
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t2"), new Stats(6L, 24L, Map.of()));
            tableStats.updateTableStats(newStats);
        }

        execute("select a, x from t1 join t2 on t1.a + 1 = t2.x + 1 order by a, x");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("1| 1\n" +
               "1| 1\n"));
    }

    /**
     * some implementations will apply the branch reordering optimisation, whilst others might not.
     * either way, all join implementations should yield the same results
     */
    @Test
    public void testJoinBranchReorderingOnMultipleTables() throws Exception {
        execute("create table t1 (a integer)");
        execute("create table t2 (x integer)");
        execute("create table t3 (y integer)");
        ensureYellow();
        execute("insert into t1 (a) values (0), (1)");
        execute("insert into t2 (x) values (0), (1), (2)");
        execute("insert into t3 (y) values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)");
        execute("refresh table t1, t2, t3");

        Iterable<TableStats> tableStatsOnAllNodes = internalCluster().getInstances(TableStats.class);
        for (TableStats tableStats : tableStatsOnAllNodes) {
            Map<RelationName, Stats> newStats = new HashMap<>();
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t1"), new Stats(2L, 8L, Map.of()));
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t2"), new Stats(3L, 12L, Map.of()));
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t3"), new Stats(10L, 40L, Map.of()));
            tableStats.updateTableStats(newStats);
        }

        execute("select a, x, y from t1 join t2 on t1.a = t2.x join t3 on t3.y = t2.x where t1.a < t2.x + 1 " +
                "and t2.x < t3.y + 1 order by a, x, y");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("0| 0| 0\n" +
               "1| 1| 1\n"));
    }

    @Test
    public void test_block_NestedLoop_or_HashJoin__with_group_by_on_right_side() {
        execute("create table t1 (x integer)");
        execute("insert into t1 (x) values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)");
        execute("refresh table t1");

        long memoryLimit = 6 * 1024 * 1024;
        double overhead = 1.0d;
        execute("set global \"indices.breaker.query.limit\" = '" + memoryLimit + "b', " +
                "\"indices.breaker.query.overhead\" = " + overhead);
        CircuitBreaker queryCircuitBreaker = internalCluster().getInstance(CircuitBreakerService.class).getBreaker(HierarchyCircuitBreakerService.QUERY);
        randomiseAndConfigureJoinBlockSize("t1", 10L, queryCircuitBreaker);

        execute("select x from t1 left_rel JOIN (select x x2, count(x) from t1 group by x2) right_rel " +
                "ON left_rel.x = right_rel.x2 order by left_rel.x");

        assertThat(TestingHelpers.printedTable(response.rows()),
            is("0\n" +
               "1\n" +
               "2\n" +
               "3\n" +
               "4\n" +
               "5\n" +
               "6\n" +
               "7\n" +
               "8\n" +
               "9\n"));
    }

    private void randomiseAndConfigureJoinBlockSize(String relationName, long rowsCount, CircuitBreaker circuitBreaker) {
        long availableMemory = circuitBreaker.getLimit() - circuitBreaker.getUsed();
        // We're randomising the table size we configure in the stats in a way such that the number of rows that fit
        // in memory is sometimes less than the row count of the table (ie. multiple blocks are created) and sometimes
        // the entire table fits in memory (ie. one block is used)
        long tableSizeInBytes = new Random().nextInt(3 * (int) availableMemory);
        long rowSizeBytes = tableSizeInBytes / rowsCount;

        for (TableStats tableStats : internalCluster().getInstances(TableStats.class)) {
            Map<RelationName, Stats> newStats = new HashMap<>();
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), relationName), new Stats(rowsCount, tableSizeInBytes, Map.of()));
            tableStats.updateTableStats(newStats);
        }

        RamBlockSizeCalculator ramBlockSizeCalculator = new RamBlockSizeCalculator(
            500_000,
            circuitBreaker,
            rowSizeBytes,
            rowsCount
        );
        logger.info("\n\tThe block size for relation {}, total size {} bytes, with row count {} and row size {} bytes, " +
                    "if it would be used in a block join algorithm, would be {}",
            relationName, tableSizeInBytes, rowsCount, rowSizeBytes, ramBlockSizeCalculator.getAsInt());
    }

    @Test
    public void testInnerJoinWithPushDownOptimizations() {
        execute("CREATE TABLE t1 (id INTEGER)");
        execute("CREATE TABLE t2 (id INTEGER, name STRING, id_t1 INTEGER)");

        execute("INSERT INTO t1 (id) VALUES (1), (2)");
        execute("INSERT INTO t2 (id, name, id_t1) VALUES (1, 'A', 1), (2, 'B', 2), (3, 'C', 2)");
        execute("REFRESH TABLE t1, t2");

        assertThat(printedTable(execute(
            "SELECT t1.id, t2.id FROM t2 INNER JOIN t1 ON t1.id = t2.id_t1 ORDER BY lower(t2.name)").rows()),
            is("1| 1\n" +
               "2| 2\n" +
               "2| 3\n")
        );

        assertThat(printedTable(execute(
            "SELECT t1.id, t2.id, t2.name FROM t2 INNER JOIN t1 ON t1.id = t2.id_t1 ORDER BY lower(t2.name)").rows()),
            is("1| 1| A\n" +
               "2| 2| B\n" +
               "2| 3| C\n"));

        assertThat(printedTable(execute(
            "SELECT t1.id, t2.id, lower(t2.name) FROM t2 INNER JOIN t1 ON t1.id = t2.id_t1 ORDER BY lower(t2.name)").rows()),
            is("1| 1| a\n" +
               "2| 2| b\n" +
               "2| 3| c\n")
        );
    }

    @Test
    public void testInnerJoinOnPreSortedRightRelation() {
        execute("CREATE TABLE t1 (x int) with (number_of_replicas = 0)");
        execute("insert into t1 (x) values (1) ");
        execute("refresh table t1");
        // regression test; the repeat requirement wasn't set correctly for the right side
        assertThat(
            printedTable(execute(
                "select * from (select * from t1 order by x) t1 " +
                "join (select * from t1 order by x) t2 on t1.x=t2.x").rows()),
            is("1| 1\n")
        );
    }

    @Test
    public void test_join_with_and_false_in_where_clause_returns_empty_result() {
        String stmt = "SELECT n.* " +
                      "FROM " +
                      "   pg_catalog.pg_namespace n," +
                      "   pg_catalog.pg_class c " +
                      "WHERE " +
                      "   n.nspname LIKE E'sys' " +
                      "   AND c.relnamespace = n.oid " +
                      "   AND (false)";
        execute(stmt);
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void test_many_table_join_with_filter_pushdown() throws Exception {
        // regression this; optimization rule resulted in a endless loop
        String stmt = ""
            + "SELECT\n"
            + "   *\n"
            + "FROM\n"
            + "    pg_catalog.pg_namespace pkn,\n"
            + "    pg_catalog.pg_class pkc,\n"
            + "    pg_catalog.pg_attribute pka,\n"
            + "    pg_catalog.pg_namespace fkn,\n"
            + "    pg_catalog.pg_class fkc,\n"
            + "    pg_catalog.pg_attribute fka,\n"
            + "    pg_catalog.pg_constraint con,\n"
            + "    pg_catalog.generate_series(1, 32) pos (n),\n"
            + "    pg_catalog.pg_class pkic\n"
            + "WHERE\n"
            + "    pkn.oid = pkc.relnamespace\n"
            + "    AND pkc.oid = pka.attrelid\n"
            + "    AND pka.attnum = con.confkey[pos.n]\n"
            + "    AND con.confrelid = pkc.oid\n"
            + "    AND fkn.oid = fkc.relnamespace\n"
            + "    AND fkc.oid = fka.attrelid\n"
            + "    AND fka.attnum = con.conkey[pos.n]\n"
            + "    AND con.conrelid = fkc.oid\n"
            + "    AND con.contype = 'f'\n"
            + "    AND pkic.relkind = 'i'\n"
            + "    AND pkic.oid = con.conindid\n"
            + "    AND pkn.nspname = E'sys'\n"
            + "    AND fkn.nspname = E'sys'\n"
            + "    AND pkc.relname = E'jobs'\n"
            + "    AND fkc.relname = E'jobs_log'\n"
            + "ORDER BY\n"
            + "    fkn.nspname,\n"
            + "    fkc.relname,\n"
            + "    con.conname,\n"
            + "    pos.n\n";
        execute(stmt);
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void test_group_by_on_cross_join_on_system_tables() throws Exception {
        String stmt = "SELECT c.name as name, max(h.severity) as severity " +
            "FROM sys.health h, sys.cluster c " +
            "GROUP BY 1";
        assertThat(printedTable(execute(stmt).rows()), is(
            ""
        ));
    }


    @Test
    @UseHashJoins(value = 1.0)
    @UseRandomizedSchema(random = false)
    public void test_nested_join_with_primary_key_lookup() throws Exception {
        // Tests for a bug where the "requires scroll" property wasn't inherited correctly.
        // This led to a primary-key lookup operation which didn't support `moveToStart`
        execute("create table t1 (id int primary key, a int)");
        execute("create table t2 (id int primary key, b int)");
        execute("create table t3 (id int primary key, c int)");
        execute("create table t4 (id int primary key, d int)");
        execute("insert into t1 (id, a) values (1, 1), (2, 10)");
        execute("insert into t2 (id, b) values (1, 2), (2, 20)");
        execute("insert into t3 (id, c) values (1, 2), (3, 30)");
        execute("insert into t4 (id, d) values (1, 3), (4, 40)");

        execute("refresh table t1, t2, t3");
        execute("analyze");
        String stmt = """
            SELECT
                *
            FROM
                t1
                JOIN t2 on t2.id = t1.id
                JOIN t3 on t3.id = t2.id
                LEFT OUTER JOIN t4 on t4.id = t3.id
            WHERE
                t2.id = 1 OR t2.id = 2
        """;
        execute("EXPLAIN " + stmt);
        // ensure that the query is using the execution plan we want to test
        // This should prevent from the test case becoming invalid
        assertThat(printedTable(response.rows()), is(
            "Eval[id, a, id, b, id, c, id, d]\n" +
            "  └ NestedLoopJoin[LEFT | (id = id)]\n" +
            "    ├ HashJoin[(id = id)]\n" +
            "    │  ├ HashJoin[(id = id)]\n" +
            "    │  │  ├ Get[doc.t2 | id, b | DocKeys{1; 2} | ((id = 1) OR (id = 2))]\n" +
            "    │  │  └ Collect[doc.t1 | [id, a] | true]\n" +
            "    │  └ Collect[doc.t3 | [id, c] | true]\n" +
            "    └ Collect[doc.t4 | [id, d] | true]\n"
        ));
        execute(stmt);
    }

    @Test
    @UseHashJoins(value = 0.0)
    @UseRandomizedSchema(random = false)
    public void test_nested_join_with_primary_key_lookup_on_each_join() throws Exception {
        // Tests for a bug where join operations got stuck because the Paging.PAGE_SIZE was set to 0 which
        // resulted in intermediate requests for just 1 record. This causes the join operations to get stuck.
        execute("create table t1 (id int primary key, a int)");
        execute("create table t2 (id int primary key, b int) clustered into 1 shards");
        execute("create table t3 (id int primary key, c int) clustered into 1 shards");
        execute("insert into t1 (id, a) values (1, 1), (2, 10)");
        execute("insert into t2 (id, b) values (1, 2), (2, 20)");
        Object[][] bulkArgs = new Object[10][];
        for (int i = 0; i < 10; i++) {
            bulkArgs[i] = new Object[] { i, i * 10 };
        }
        execute("insert into t3 (id, c) values (?, ?)", bulkArgs);
        execute("refresh table t1, t2, t3");
        execute("analyze");
        String stmt = """
            SELECT
                *
            FROM
                t1
                JOIN t2 on t2.id = t1.id
                LEFT OUTER JOIN t3 on t3.id = t2.id
            WHERE
                t2.id = 1
                AND t3.id = 1
        """;
        execute("EXPLAIN " + stmt);
        // ensure that the query is using the execution plan we want to test
        // This should prevent from the test case becoming invalid
        assertThat(printedTable(response.rows()), is(
            "Eval[id, a, id, b, id, c]\n" +
            "  └ NestedLoopJoin[INNER | (id = id)]\n" +
            "    ├ NestedLoopJoin[INNER | (id = id)]\n" +
            "    │  ├ Get[doc.t2 | id, b | DocKeys{1} | (id = 1)]\n" +
            "    │  └ Collect[doc.t1 | [id, a] | true]\n" +
            "    └ Get[doc.t3 | id, c | DocKeys{1} | (id = 1)]\n"));
        execute(stmt);
    }


    @Test
    @UseHashJoins(1)
    public void test_inner_join_on_empty_system_tables() throws Exception {
        String stmt = """
            SELECT
                shards.id,
                table_name,
                schema_name,
                partition_ident,
                state,
                nodes.id AS node_id,
                nodes.name AS node_name
            FROM
                sys.shards
                INNER JOIN sys.nodes AS nodes ON shards.node['id'] = nodes.id
            ORDER BY
                node_id,
                shards.id
            """;
        execute("EXPLAIN " + stmt);
        assertThat(printedTable(response.rows()), is(
            "Eval[id, table_name, schema_name, partition_ident, state, id AS node_id, name AS node_name]\n" +
            "  └ OrderBy[id AS node_id ASC id ASC]\n" +
            "    └ HashJoin[(node['id'] = id)]\n" +
            "      ├ Collect[sys.shards | [id, table_name, schema_name, partition_ident, state, node['id']] | true]\n" +
            "      └ Rename[id, name] AS nodes\n" +
            "        └ Collect[sys.nodes | [id, name] | true]\n"
        ));
        execute(stmt);
        assertThat(response.rowCount(), is(0L));
    }

    /**
     * https://github.com/crate/crate/issues/11404
     */
    @Test
    @UseHashJoins(1)
    public void test_constant_join_criteria_handling() throws Exception {
        execute("create table doc.t1 (a integer, b text)");
        execute("create table doc.t2 (a integer, c text)");
        execute("explain select doc.t1.*, doc.t2.c from doc.t1 join doc.t2 on doc.t1.a = doc.t2.a and doc.t2.c = 'abc'");
        assertThat(printedTable(response.rows()),
                   is("Eval[a, b, c]\n" +
                      "  └ HashJoin[(a = a)]\n" +
                      "    ├ Collect[doc.t1 | [a, b] | true]\n" +
                      "    └ Collect[doc.t2 | [c, a] | (c = 'abc')]\n"));
    }
}
