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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.execution.engine.join.RamBlockSizeCalculator;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.metadata.RelationName;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.testing.Asserts;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;
import io.crate.types.DataTypes;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class JoinIntegrationTest extends IntegTestCase {

    @After
    public void resetStatsAndBreakerSettings() {
        resetTableStats();
        execute("reset global indices");
    }

    @Test
    public void testCrossJoinOrderByOnBothTables() throws Exception {
        createColorsAndSizes();
        execute("select colors.name, sizes.name from colors, sizes order by colors.name, sizes.name");
        assertThat(response).hasRows(
            "blue| large",
            "blue| small",
            "green| large",
            "green| small",
            "red| large",
            "red| small");
    }

    @Test
    public void testCrossJoinOrderByOnOneTableWithLimit() throws Exception {
        createColorsAndSizes();
        execute("select colors.name, sizes.name from colors, sizes order by sizes.name, colors.name limit 4");
        assertThat(response).hasRows(
            "blue| large",
            "green| large",
            "red| large",
            "blue| small");
    }

    @Test
    public void testInsertFromCrossJoin() throws Exception {
        createColorsAndSizes();
        execute("create table target (color string, size string)");
        ensureYellow();

        execute("insert into target (color, size) (select colors.name, sizes.name from colors cross join sizes)");
        execute("refresh table target");

        execute("select color, size from target order by size, color limit 4");
        assertThat(response).hasRows(
            "blue| large",
            "green| large",
            "red| large",
            "blue| small");
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
        assertThat(response).hasRows("2| 2");
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
        assertThat(response).hasRows(
            "1| xxx| 1| xxx| 12",
            "3| yyy| 3| yyy| 7",
            "5| zzz| 5| zzz| 0");
        execute("SELECT * FROM (SELECT * FROM t1) AS rel1 JOIN (SELECT * FROM t2) AS rel2 USING (num, value) ORDER BY rel1.num;");
        assertThat(response).hasRows(
            "1| xxx| 1| xxx| 12",
            "3| yyy| 3| yyy| 7",
            "5| zzz| 5| zzz| 0");
    }

    @Test
    public void testJoinOnEmptyPartitionedTablesWithAndWithoutJoinCondition() throws Exception {
        execute("create table foo (id long) partitioned by (id)");
        execute("create table bar (id long) partitioned by (id)");
        ensureYellow();
        execute("select * from foo f, bar b where f.id = b.id");
        assertThat(response).isEmpty();

        execute("select * from foo f, bar b");
        assertThat(response).isEmpty();
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
        assertThat(response).hasRowCount(1L);
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
        assertThat(response).hasRows("425","574");
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
        assertThat(response).hasRows("1");
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
        assertThat(response).hasRows(
                                                     "28.3| 15.0| foobar",
                                                     "28.3| 20.3| foobar",
                                                     "40.1| 15.0| bar",
                                                     "40.1| 20.3| bar");
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
        assertThat(response).hasRows(
            "black| towel",
            "grey| towel",
            "black| cheese",
            "grey| cheese");

    }

    @Test
    public void testCrossJoinWithoutLimitAndOrderByAndCrossJoinSyntax() throws Exception {
        createColorsAndSizes();
        execute("select colors.name, sizes.name from colors cross join sizes");
        assertThat(response).hasRowCount(6L);

        List<Object[]> rows = Arrays.asList(response.rows());
        rows.sort(OrderingByPosition.arrayOrdering(
            List.of(DataTypes.STRING, DataTypes.STRING),
            new int[]{0, 1},
            new boolean[]{false, false},
            new boolean[]{false, false}));
        assertThat(response).hasRows(
            "blue| large",
            "blue| small",
            "green| large",
            "green| small",
            "red| large",
            "red| small"
        );
    }

    @Test
    public void testOutputFromOnlyOneTable() throws Exception {
        createColorsAndSizes();
        execute("select colors.name from colors, sizes order by colors.name");
        assertThat(response).hasRowCount(6L);
        assertThat(response).hasRows(
            "blue",
            "blue",
            "green",
            "green",
            "red",
            "red");
    }

    @Test
    public void testCrossJoinWithSysTable() throws Exception {
        execute("create table t (name string) clustered into 3 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t values ('foo'), ('bar')");
        execute("refresh table t");

        execute("select shards.id, t.name from sys.shards, t where shards.table_name = 't' order by shards.id, t.name");
        assertThat(response).hasRowCount(6L);
        assertThat(response).hasRows(
            "0| bar",
            "0| foo",
            "1| bar",
            "1| foo",
            "2| bar",
            "2| foo");
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
        assertThat(response).hasRowCount(5L);
        assertThat(response).hasRows(
            "strict| blob_path",
               "strict| closed",
               "strict| flush_stats",
               "strict| flush_stats['count']",
               "strict| flush_stats['periodic_count']");
    }

    @Test
    public void testCrossJoinSysTablesOnly() throws Exception {
        execute("create table t (name string) clustered into 3 shards with (number_of_replicas = 0)");
        ensureYellow();

        execute("select s1.id, s2.id, s1.table_name from sys.shards s1, sys.shards s2 order by s1.id asc, s2.id desc");
        assertThat(response).hasRowCount(9L);

        assertThat(response).hasRows(
            "0| 2| t",
            "0| 1| t",
            "0| 0| t",
            "1| 2| t",
            "1| 1| t",
            "1| 0| t",
            "2| 2| t",
            "2| 1| t",
            "2| 0| t");

        execute("select s1.id, s2.id, s1.table_name from sys.shards s1, sys.shards s2");
        assertThat(response).hasRowCount(9L);

        List<Object[]> rows = Arrays.asList(response.rows());
        rows.sort(OrderingByPosition.arrayOrdering(
            List.of(DataTypes.INTEGER, DataTypes.INTEGER),
            new int[]{0, 1},
            new boolean[]{false, true},
            new boolean[]{false, true}
        ));

        assertThat(rows).containsExactly(
            new Object[][]{
                {0, 2, "t"},
                {0, 1, "t"},
                {0, 0, "t"},
                {1, 2, "t"},
                {1, 1, "t"},
                {1, 0, "t"},
                {2, 2, "t"},
                {2, 1, "t"},
                {2, 0, "t"}}
        );
    }

    @Test
    public void testJoinUsingSubscriptInQuerySpec() {
        execute("create table t1 (id int, a object as (b int))");
        execute("create table t2 (id int)");
        execute("insert into t1 (id, a) values (1, {b=1})");
        execute("insert into t2 (id) values (1)");
        refresh();
        execute("select t.id, tt.id from t1 as t, t2 as tt where tt.id = t.a['b']");
        assertThat(response).hasRows("1| 1");
    }

    @Test
    public void testCrossJoinFromInformationSchemaTable() throws Exception {
        // sys table with doc granularity on single node
        execute("select * from information_schema.schemata t1, information_schema.schemata t2 " +
                "order by t1.schema_name, t2.schema_name");
        assertThat(response).hasRowCount(25L);
        assertThat(response).hasRows(
               "blob| blob",
               "blob| doc",
               "blob| information_schema",
               "blob| pg_catalog",
               "blob| sys",
               "doc| blob",
               "doc| doc",
               "doc| information_schema",
               "doc| pg_catalog",
               "doc| sys",
               "information_schema| blob",
               "information_schema| doc",
               "information_schema| information_schema",
               "information_schema| pg_catalog",
               "information_schema| sys",
               "pg_catalog| blob",
               "pg_catalog| doc",
               "pg_catalog| information_schema",
               "pg_catalog| pg_catalog",
               "pg_catalog| sys",
               "sys| blob",
               "sys| doc",
               "sys| information_schema",
               "sys| pg_catalog",
               "sys| sys");
    }

    @Test
    public void testSelfJoin() throws Exception {
        execute("create table t (x int)");
        ensureYellow();
        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");
        execute("select * from t as t1, t as t2");
        assertThat(response).hasRowCount(4L);
        assertThat(response).hasRowsInAnyOrder(
            new Object[]{1, 1},
            new Object[]{1, 2},
            new Object[]{2, 1},
            new Object[]{2, 2});
    }

    @Test
    public void testSelfJoinWithOrder() throws Exception {
        execute("create table t (x int)");
        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");
        execute("select * from t as t1, t as t2 order by t1.x, t2.x");
        assertThat(response).hasRows(
            "1| 1",
            "1| 2",
            "2| 1",
            "2| 2");
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_self_join_with_order_and_limit_is_executed_with_qtf() throws Exception {
        execute("create table doc.t (x int, y int)");
        execute("insert into doc.t (x, y) values (1, 10), (2, 20)");
        execute("refresh table doc.t");
        execute("explain (costs false) select * from doc.t as t1, doc.t as t2 order by t1.x, t2.x limit 3");
        assertThat(response).hasLines(
            "Fetch[x, y, x, y]",
            "  └ Limit[3::bigint;0]",
            "    └ OrderBy[x ASC x ASC]",
            "      └ NestedLoopJoin[CROSS]",
            "        ├ Rename[t1._fetchid, x] AS t1",
            "        │  └ Collect[doc.t | [_fetchid, x] | true]",
            "        └ Rename[t2._fetchid, x] AS t2",
            "          └ Collect[doc.t | [_fetchid, x] | true]"
        );
        execute("select * from doc.t as t1, doc.t as t2 order by t1.x, t2.x limit 3");
        assertThat(response).hasRows(
            "1| 10| 1| 10",
            "1| 10| 2| 20",
            "2| 20| 1| 10"
        );
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
        assertThat(response).hasRows("Douglas Adams| Trillian| 200.0",
                                                     "Douglas Adams| Ford Perfect| 600.0",
                                                     "Trillian| Ford Perfect| 400.0");
    }

    @Test
    public void testFilteredSelfJoinWithFilterOnBothRelations() {
        execute("create table test(id long primary key, num long, txt string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test(id, num, txt) values(1, 1, '1111'), (2, 2, '2222'), (3, 1, '2222'), (4, 2, '2222')");
        execute("refresh table test");

        execute("select t1.id, t2.id from test as t1 inner join test as t2 on t1.num = t2.num " +
                "where t1.txt = '1111' and t2.txt='2222'");
        assertThat(response).hasRows("1| 3");
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
        assertThat(response).hasRows(
            "Douglas Adams| Chief Office",
            "Trillian| Entresol",
            "Ford Perfect| Hobbit House");
    }

    @Test
    public void testFetchWithoutOrder() throws Exception {
        createColorsAndSizes();
        execute("select colors.name, sizes.name from colors, sizes limit 3");
        assertThat(response).hasRowCount(3L);
    }

    @Test
    public void testJoinWithFunctionInOutputAndOrderBy() throws Exception {
        createColorsAndSizes();
        execute("select substr(colors.name, 0, 1), sizes.name from colors, sizes order by colors.name, sizes.name limit 3");
        assertThat(response).hasRows(
            "b| large",
            "b| small",
            "g| large");
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
        assertThat(response).hasColumns("id", "id", "name");
        assertThat(response).hasRowCount(0L);
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
        assertThat(response).hasRows(
            "-1.2| Vienna",
            "-0.5| Vienna",
            "0.2| Vienna",
            "19.7| San Francisco",
            "20.8| San Francisco",
            "23.2| San Francisco");
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
        assertThat(response).hasRowCount(1L);
        assertThat(response).hasRows("1| 2| 3");
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
        assertThat(response).hasRows(
            "Arthur| Earth destroyed",
            "Arthur| Hitch hiking on a vogon ship",
            "Trillian| Meeting Arthur");
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
        assertThat(response).hasRows(
            "Hello World| [foo, bar]");
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
        assertThat(response).hasRows("6");
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
        assertThat(response).hasRows(
            "1| 4| 5",
            "1| 4| 6",
            "1| 2| 5",
            "3| 4| 5",
            "1| 2| 6",
            "3| 4| 6",
            "3| 2| 5",
            "3| 2| 6");
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
        assertThat(response).hasRows(
            "1| 1",
            "1| 1",
            "1| 2",
            "1| 2",
            "2| 1",
            "2| 1",
            "2| 2",
            "2| 2");
    }

    @Test
    public void testJoinOnInformationSchema() throws Exception {
        execute("create table t (id string, name string)");
        ensureYellow();
        execute("insert into t (id, name) values ('0-1', 'Marvin')");
        execute("refresh table t");
        execute("select * from t inner join information_schema.tables on t.id = tables.number_of_replicas");
        assertThat(response).hasRowCount(1L);
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

        assertThatThrownBy(() -> execute(plan).getResult())
            .isExactlyInstanceOf(IndexNotFoundException.class);
    }

    @Test
    public void testAggOnJoin() throws Exception {
        execute("create table t1 (x int)");
        ensureYellow();
        execute("insert into t1 (x) values (1), (2)");
        execute("refresh table t1");

        execute("select sum(t1.x) from t1, t1 as t2");
        assertThat(response).hasRows("6");
    }

    @Test
    public void testAggOnJoinWithScalarAfterAggregation() throws Exception {
        execute("select sum(t1.t1) * 2 from unnest([1, 2]) t1, unnest([3, 4]) t2");
        assertThat(response).hasRows("12");
    }

    @Test
    public void testAggOnJoinWithHaving() throws Exception {
        execute("select sum(t1.t1) from unnest([1, 2]) t1, unnest([3, 4]) t2 having sum(t1.t1) > 8");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testAggOnJoinWithLimit() throws Exception {
        execute("select " +
                "   sum(t1.t1) " +
                "from unnest([1, 2]) t1, unnest([3, 4]) t2 " +
                "limit 0");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testLimitIsAppliedPostJoin() throws Exception {
        execute("select " +
                "   sum(t1.t1) " +
                "from unnest([1, 1]) t1, unnest([1, 1]) t2 " +
                "limit 1");
        assertThat(response).hasRows("4");
    }

    @Test
    public void testJoinOnAggWithOrderBy() throws Exception {
        execute("select sum(t1.t1) from unnest([1, 1]) t1, unnest([1, 1]) t2 order by 1");
        assertThat(response).hasRows("4");
    }

    @Test
    public void testFailureOfJoinDownstream() throws Exception {
        // provoke an exception when the NL emits a row, must bubble up and NL must stop
        Asserts.assertSQLError(() -> execute("select cast(R.col2 || ' ' || L.col2 as integer)" +
                                   "   from " +
                                   "       unnest(['hello', 'world'], [1, 2]) L " +
                                   "   inner join " +
                                   "       unnest(['world', 'hello'], [1, 2]) R " +
                                   "   on l.col1 = r.col1 " +
                                   "where r.col1 > 1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot cast value `world` to type `integer`");
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
        assertThat(response).hasRows("3.69| 2.0| 1");
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
                "   (select unnest as x from unnest([1, 2, 3])) t1, " +
                "   (select max(unnest) as y from unnest([4])) t2 " +
                "order by t1.x");
        assertThat(response).hasRows(
            "1| 4",
            "2| 4",
            "3| 4");
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
                "   (select unnest as y from unnest([10])) tt2 ");
        assertThat(response).hasRows(
            "4| 10",
            "3| 10");

        execute("select * from " +
                "   (select x from " +
                "       (select x from t1 order by x asc limit 4) tt1 " +
                "   order by tt1.x desc limit 2 " +
                "   ) ttt1, " +
                "   (select max(y) as y from " +
                "       (select min(unnest) as y from unnest([10])) tt2 " +
                "   ) ttt2 ");
        assertThat(response).hasRows(
            "4| 10",
            "3| 10");
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

        String stmt = """
            SELECT t1.company_id, t1.country, t1.id, t1.name, t2.customer_id, t2.id, t2.price
            FROM customers t1,
                 (SELECT * FROM (SELECT * from orders order by price desc limit 4) t ORDER BY price limit 3) t2
            WHERE t2.customer_id = t1.id order by price limit 3 offset 1""";

        execute(stmt);
        assertThat(response).hasRows(
            "1| Greece| 1| Marios| 1| 3| 30.0",
            "1| Greece| 1| Marios| 1| 4| 40.0");
    }

    @Test
    public void testJoinOnVirtualTableWithSingleRowSubselect() throws Exception {
        execute(
            """
                SELECT
                  (select min(t1.x) from
                    (select unnest as x from unnest([1, 2, 3])) t1,
                    (select * from unnest([1, 2, 3])) t2
                  ) as min_col1,
                  *
                FROM
                  unnest([1]) tt1,        unnest([2]) tt2""");
        assertThat(response).hasRows("1| 1| 2");
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
        execute("set global \"indices.breaker.query.limit\" = '" + memoryLimit + "b'");
        CircuitBreaker queryCircuitBreaker = cluster().getInstance(CircuitBreakerService.class).getBreaker(HierarchyCircuitBreakerService.QUERY);
        randomiseAndConfigureJoinBlockSize("t1", 5L, queryCircuitBreaker);
        randomiseAndConfigureJoinBlockSize("t2", 5L, queryCircuitBreaker);

        execute("select a, x from t1 join t2 on t1.a + 1 = t2.x order by a, x");
        assertThat(response).hasRows(
            "0| 1",
            "0| 1",
            "2| 3",
            "2| 3");
    }

    private void resetTableStats() {
        for (TableStats tableStats : cluster().getInstances(TableStats.class)) {
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

        Iterable<TableStats> tableStatsOnAllNodes = cluster().getInstances(TableStats.class);
        for (TableStats tableStats : tableStatsOnAllNodes) {
            Map<RelationName, Stats> newStats = new HashMap<>();
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t1"), new Stats(4L, 16L, Map.of()));
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t2"), new Stats(6L, 24L, Map.of()));
            tableStats.updateTableStats(newStats);
        }

        execute("select a, x from t1 join t2 on t1.a + 1 = t2.x + 1 order by a, x");
        assertThat(response).hasRows(
            "1| 1",
            "1| 1");
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

        Iterable<TableStats> tableStatsOnAllNodes = cluster().getInstances(TableStats.class);
        for (TableStats tableStats : tableStatsOnAllNodes) {
            Map<RelationName, Stats> newStats = new HashMap<>();
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t1"), new Stats(2L, 8L, Map.of()));
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t2"), new Stats(3L, 12L, Map.of()));
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), "t3"), new Stats(10L, 40L, Map.of()));
            tableStats.updateTableStats(newStats);
        }

        execute("select a, x, y from t1 join t2 on t1.a = t2.x join t3 on t3.y = t2.x where t1.a < t2.x + 1 " +
                "and t2.x < t3.y + 1 order by a, x, y");
        assertThat(response).hasRows(
            "0| 0| 0",
            "1| 1| 1");
    }

    @Test
    public void test_block_NestedLoop_or_HashJoin__with_group_by_on_right_side() {
        execute("create table t1 (x integer)");
        execute("insert into t1 (x) values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)");
        execute("refresh table t1");

        long memoryLimit = 6 * 1024 * 1024;
        execute("set global \"indices.breaker.query.limit\" = '" + memoryLimit + "b'");
        CircuitBreaker queryCircuitBreaker = cluster().getInstance(CircuitBreakerService.class).getBreaker(HierarchyCircuitBreakerService.QUERY);
        randomiseAndConfigureJoinBlockSize("t1", 10L, queryCircuitBreaker);

        execute("select x from t1 left_rel JOIN (select x x2, count(x) from t1 group by x2) right_rel " +
                "ON left_rel.x = right_rel.x2 order by left_rel.x");

        assertThat(response).hasRows(
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9");
    }

    private void randomiseAndConfigureJoinBlockSize(String relationName, long rowsCount, CircuitBreaker circuitBreaker) {
        long availableMemory = circuitBreaker.getLimit() - circuitBreaker.getUsed();
        // We're randomising the table size we configure in the stats in a way such that the number of rows that fit
        // in memory is sometimes less than the row count of the table (ie. multiple blocks are created) and sometimes
        // the entire table fits in memory (ie. one block is used)
        long tableSizeInBytes = new Random().nextInt(3 * (int) availableMemory);
        long rowSizeBytes = tableSizeInBytes / rowsCount;

        for (TableStats tableStats : cluster().getInstances(TableStats.class)) {
            Map<RelationName, Stats> newStats = new HashMap<>();
            newStats.put(new RelationName(sqlExecutor.getCurrentSchema(), relationName), new Stats(rowsCount, tableSizeInBytes, Map.of()));
            tableStats.updateTableStats(newStats);
        }

        RamBlockSizeCalculator ramBlockSizeCalculator = new RamBlockSizeCalculator(
            500_000,
            circuitBreaker,
            rowSizeBytes
        );
        logger.info("\n\tThe block size for relation {}, total size {} bytes, with row count {} and row size {} bytes, " +
                    "if it would be used in a block join algorithm, would be {}",
            relationName, tableSizeInBytes, rowsCount, rowSizeBytes, ramBlockSizeCalculator.applyAsInt(-1));
    }

    @Test
    public void testInnerJoinWithPushDownOptimizations() {
        execute("CREATE TABLE t1 (id INTEGER)");
        execute("CREATE TABLE t2 (id INTEGER, name STRING, id_t1 INTEGER)");

        execute("INSERT INTO t1 (id) VALUES (1), (2)");
        execute("INSERT INTO t2 (id, name, id_t1) VALUES (1, 'A', 1), (2, 'B', 2), (3, 'C', 2)");
        execute("REFRESH TABLE t1, t2");

        execute("SELECT t1.id, t2.id FROM t2 INNER JOIN t1 ON t1.id = t2.id_t1 ORDER BY lower(t2.name)");
        assertThat(response).hasRows(
            "1| 1",
            "2| 2",
            "2| 3");

        execute("SELECT t1.id, t2.id, t2.name FROM t2 INNER JOIN t1 ON t1.id = t2.id_t1 ORDER BY lower(t2.name)");
        assertThat(response).hasRows(
            "1| 1| A",
            "2| 2| B",
            "2| 3| C");

        execute("SELECT t1.id, t2.id, lower(t2.name) FROM t2 INNER JOIN t1 ON t1.id = t2.id_t1 ORDER BY lower(t2.name)");
        assertThat(response).hasRows(
            "1| 1| a",
            "2| 2| b",
            "2| 3| c");
    }

    @Test
    public void testInnerJoinOnPreSortedRightRelation() {
        execute("CREATE TABLE t1 (x int) with (number_of_replicas = 0)");
        execute("insert into t1 (x) values (1) ");
        execute("refresh table t1");
        // regression test; the repeat requirement wasn't set correctly for the right side
        execute(
            "select * from (select * from t1 order by x) t1 " +
            "join (select * from t1 order by x) t2 on t1.x=t2.x");
        assertThat(response).hasRows("1| 1");
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
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void test_many_table_join_with_filter_pushdown() throws Exception {
        // regression this; optimization rule resulted in a endless loop
        String stmt = """
            SELECT
               *
            FROM
                pg_catalog.pg_namespace pkn,
                pg_catalog.pg_class pkc,
                pg_catalog.pg_attribute pka,
                pg_catalog.pg_namespace fkn,
                pg_catalog.pg_class fkc,
                pg_catalog.pg_attribute fka,
                pg_catalog.pg_constraint con,
                pg_catalog.generate_series(1, 32) pos (n),
                pg_catalog.pg_class pkic
            WHERE
                pkn.oid = pkc.relnamespace
                AND pkc.oid = pka.attrelid
                AND pka.attnum = con.confkey[pos.n]
                AND con.confrelid = pkc.oid
                AND fkn.oid = fkc.relnamespace
                AND fkc.oid = fka.attrelid
                AND fka.attnum = con.conkey[pos.n]
                AND con.conrelid = fkc.oid
                AND con.contype = 'f'
                AND pkic.relkind = 'i'
                AND pkic.oid = con.conindid
                AND pkn.nspname = E'sys'
                AND fkn.nspname = E'sys'
                AND pkc.relname = E'jobs'
                AND fkc.relname = E'jobs_log'
            ORDER BY
                fkn.nspname,
                fkc.relname,
                con.conname,
                pos.n
            """;
        execute(stmt);
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void test_group_by_on_cross_join_on_system_tables() throws Exception {
        String stmt = """
            SELECT c.name as name, max(h.severity) as severity
            FROM sys.health h, sys.cluster c
            GROUP BY 1""";
        execute(stmt);
        assertThat(response).isEmpty();
    }

    @Test
    @UseHashJoins(value = 1.0)
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(0)
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
        execute("insert into t4 (id, d) values (1, 3)");

        execute("refresh table t1, t2, t3, t4");
        execute("analyze");

        String stmt =
            """
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
        execute("EXPLAIN (COSTS FALSE)" + stmt);
        // ensure that the query is using the execution plan we want to test
        // This should prevent from the test case becoming invalid
        assertThat(response).hasLines(
                "NestedLoopJoin[LEFT | (id = id)]",
                "  ├ Eval[id, a, id, b, id, c]",
                "  │  └ HashJoin[(id = id)]",
                "  │    ├ Collect[doc.t3 | [id, c] | true]",
                "  │    └ HashJoin[(id = id)]",
                "  │      ├ Collect[doc.t1 | [id, a] | true]",
                "  │      └ Get[doc.t2 | id, b | DocKeys{1; 2} | ((id = 1) OR (id = 2))]",
                "  └ Collect[doc.t4 | [id, d] | true]"
        );
        execute(stmt);
    }

    @Test
    @UseHashJoins(value = 0.0)
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(0)
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
            bulkArgs[i] = new Object[] {i, i * 10};
        }
        execute("insert into t3 (id, c) values (?, ?)", bulkArgs);
        execute("refresh table t1, t2, t3");
        execute("analyze");
        String stmt =
            """
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
        execute("EXPLAIN (COSTS FALSE)" + stmt);
        // ensure that the query is using the execution plan we want to test
        // This should prevent from the test case becoming invalid
        assertThat(response).hasLines(
            "NestedLoopJoin[INNER | (id = id)]",
                "  ├ NestedLoopJoin[INNER | (id = id)]",
                "  │  ├ Collect[doc.t1 | [id, a] | true]",
                "  │  └ Get[doc.t2 | id, b | DocKeys{1} | (id = 1)]",
                "  └ Get[doc.t3 | id, c | DocKeys{1} | (id = 1)]");
        execute(stmt);
    }

    @Test
    @UseHashJoins(1)
    @UseRandomizedOptimizerRules(0)
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
        execute("EXPLAIN (COSTS FALSE)" + stmt);
        assertThat(response).hasLines(
            "Eval[id, table_name, schema_name, partition_ident, state, id AS node_id, name AS node_name]",
            "  └ OrderBy[id ASC id ASC]",
            "    └ HashJoin[(node['id'] = id)]",
            "      ├ Collect[sys.shards | [id, table_name, schema_name, partition_ident, state, node['id']] | true]",
            "      └ Rename[id, name] AS nodes",
            "        └ Collect[sys.nodes | [id, name] | true]"
        );
        execute(stmt);
        assertThat(response).hasRowCount(0L);
    }

    @Test
    @UseHashJoins(1)
    @UseRandomizedOptimizerRules(0)
    public void test_joins_with_constant_conditions_with_unions_and_renames() {
        execute("CREATE TABLE doc.t1 (id TEXT, name TEXT, PRIMARY KEY (id))");
        execute("CREATE TABLE doc.t2 (id TEXT, name TEXT, PRIMARY KEY (id))");
        execute("CREATE TABLE doc.t3 (id TEXT, name TEXT, PRIMARY KEY (id))");

        execute("insert into doc.t1 values ('1', 'a')");
        execute("insert into doc.t2 values ('1', 'b')");
        execute("insert into doc.t3 values ('1', 'c')");

        execute("refresh table doc.t1");
        execute("refresh table doc.t2");
        execute("refresh table doc.t3");

        var stmt = """
                select x.name, y.name
                from (select name from doc.t1 where name = 'a' union select name from doc.t3 where name = 'c') x
                join
                (select name from doc.t1 where name = 'a' union select name from doc.t2 where name = 'b') y
                on x.name = y.name and x.name != 'constant-condition'
            """;

        execute("EXPLAIN (COSTS FALSE)" + stmt);

        assertThat(response).hasLines(
                "HashJoin[(name = name)]",
                "  ├ Rename[name] AS x",
                "  │  └ GroupHashAggregate[name]",
                "  │    └ Union[name]",
                "  │      ├ Collect[doc.t1 | [name] | ((NOT (name = 'constant-condition')) AND (name = 'a'))]",
                "  │      └ Collect[doc.t3 | [name] | ((NOT (name = 'constant-condition')) AND (name = 'c'))]",
                "  └ Rename[name] AS y",
                "    └ GroupHashAggregate[name]",
                "      └ Union[name]",
                "        ├ Collect[doc.t1 | [name] | (name = 'a')]",
                "        └ Collect[doc.t2 | [name] | (name = 'b')]"
        );
        execute(stmt);
        assertThat(response).hasRows("a| a");
    }


    /**
     * https://github.com/crate/crate/issues/12357
     */
    @Test
    @UseHashJoins(1)
    @UseRandomizedOptimizerRules(0)
    public void test_alias_in_left_join_condition() {
        execute("CREATE TABLE doc.t1 (id TEXT, name TEXT, subscription_id TEXT, PRIMARY KEY (id))");
        execute("CREATE TABLE doc.t2 (kind TEXT, cluster_id TEXT, PRIMARY KEY (kind, cluster_id))");
        execute("CREATE TABLE doc.t3 (id TEXT PRIMARY KEY, reference TEXT)");

        execute("insert into doc.t1 values ('1', 'foo', '2')");
        execute("insert into doc.t2 values ('bar', '1')");
        execute("insert into doc.t3 values ('2', 'bazinga')");

        execute("refresh table doc.t1");
        execute("refresh table doc.t2");
        execute("refresh table doc.t3");

        var stmt = """
                SELECT doc.t3.id, doc.t3.reference
                FROM doc.t3
                JOIN doc.t1 ON doc.t1.subscription_id = doc.t3.id
                JOIN doc.t2 ON doc.t2.cluster_id = doc.t1.id
                AND doc.t2.kind = 'bar'
                LEFT OUTER JOIN doc.t2 AS temp ON temp.cluster_id = doc.t1.id
                AND temp.kind = 'bar'
                WHERE doc.t3.reference = 'bazinga'
            """;

        execute("EXPLAIN (COSTS FALSE)" + stmt);
        assertThat(response).hasLines(
                "Eval[id, reference]",
                "  └ NestedLoopJoin[LEFT | ((cluster_id = id) AND (kind = 'bar'))]",
                "    ├ HashJoin[(cluster_id = id)]",
                "    │  ├ HashJoin[(subscription_id = id)]",
                "    │  │  ├ Collect[doc.t3 | [id, reference] | (reference = 'bazinga')]",
                "    │  │  └ Collect[doc.t1 | [subscription_id, id] | true]",
                "    │  └ Collect[doc.t2 | [cluster_id] | (kind = 'bar')]",
                "    └ Rename[cluster_id, kind] AS temp",
                "      └ Collect[doc.t2 | [cluster_id, kind] | true]"
        );

        execute(stmt);
        assertThat(response).hasRows("2| bazinga");
    }

    /**
     * Test that a NestedLoop join as the left side of a lower join works even when both join operations
     * are running on the same node and thus the NestedLoop join will be repeated.
     * Tests for a bug where the "requires scroll" property wasn't inherited correctly.
     * See {@url https://github.com/crate/crate/issues/13689} and {@url https://github.com/crate/crate/issues/13361}.
     */
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_nested_loop_join_works_as_the_left_side_of_another_join() {
        execute("CREATE TABLE t1 (x int) CLUSTERED INTO 3 SHARDS");
        execute("CREATE TABLE t2 (y int) CLUSTERED INTO 3 SHARDS");

        execute("INSERT INTO t1 VALUES (1), (2)");
        execute("INSERT INTO t2 VALUES (1)");
        refresh();

        var stmt =
            """
            WITH combined as (
                SELECT t1.x
                FROM t1
                LEFT JOIN t2 ON t1.x = t2.y
            ),
            generated AS (
                SELECT *
                FROM UNNEST([1, 2, 3]) z
            )
            SELECT *
            FROM generated d
            LEFT JOIN combined c ON d.z = c.x
            ORDER BY 1, 2;
            """;

        // Ensure that the query is using the execution plan we want to test
        // This should prevent the test case from becoming invalid
        execute("EXPLAIN (COSTS FALSE)" + stmt);
        assertThat(response).hasLines(
                   "OrderBy[z ASC x ASC]",
                   "  └ NestedLoopJoin[LEFT | (z = x)]",
                   "    ├ Rename[z] AS d",
                   "    │  └ Rename[z] AS generated",
                   "    │    └ Rename[z] AS z",
                   "    │      └ TableFunction[unnest | [unnest] | true]",
                   "    └ Rename[x] AS c",
                   "      └ Rename[x] AS combined",
                   "        └ Eval[x]",
                   "          └ NestedLoopJoin[LEFT | (x = y)]",
                   "            ├ Collect[doc.t1 | [x] | true]",
                   "            └ Collect[doc.t2 | [y] | true]"
        );

        execute(stmt);
        assertThat(response).hasRows(
                "1| 1",
                "2| 2",
                "3| NULL"
        );
    }

    /*
     * https://github.com/crate/crate/issues/13503
     */
    @Test
    @UseHashJoins(1)
    @UseRandomizedOptimizerRules(0)
    public void test_nested_joins() {
        execute("CREATE TABLE doc.j1 (x INT)");
        execute("CREATE TABLE doc.j2 (x INT)");
        execute("CREATE TABLE doc.j3 (x INT)");

        execute("insert into doc.j1(x) values (1),(2),(3)");
        execute("insert into doc.j2(x) values (1),(2),(3)");
        execute("insert into doc.j3(x) values (1),(2),(3)");

        execute("refresh table doc.j1, doc.j2, doc.j3");

        var stmt = """
            SELECT *
              FROM doc.j1
              JOIN (doc.j2 JOIN doc.j3 ON doc.j2.x = doc.j3.x)
               ON doc.j1.x = doc.j2.x
            ORDER BY doc.j1.x;
            """;

        execute("explain (costs false)" + stmt);
        assertThat(response).hasLines(
                "Eval[x, x, x]",
                "  └ OrderBy[x ASC]",
                "    └ HashJoin[(x = x)]",
                "      ├ HashJoin[(x = x)]",
                "      │  ├ Collect[doc.j2 | [x] | true]",
                "      │  └ Collect[doc.j3 | [x] | true]",
                "      └ Collect[doc.j1 | [x] | true]"
        );

        execute(stmt);
        assertThat(response).hasRows("1| 1| 1",
                                     "2| 2| 2",
                                     "3| 3| 3");

    }

    @Test
    @UseHashJoins(1)
    @UseRandomizedOptimizerRules(0)
    public void test_join_using_on_nested_join() throws Exception {
        execute("CREATE TABLE doc.j1 (x INT)");
        execute("CREATE TABLE doc.j2 (x INT)");
        execute("CREATE TABLE doc.j3 (z INT)");

        execute("insert into doc.j1(x) values (1),(2),(3)");
        execute("insert into doc.j2(x) values (1),(2),(3)");
        execute("insert into doc.j3(z) values (1),(2),(3)");

        execute("refresh table doc.j1, doc.j2, doc.j3");

        var stmt = """
            SELECT *
                FROM (doc.j2 JOIN doc.j3 ON doc.j2.x = doc.j3.z)
                JOIN doc.J1
                USING(x)
                ORDER BY doc.j1.x
            """;

        execute("explain (costs false)" + stmt);
        assertThat(response).hasLines(
                "OrderBy[x ASC]",
                "  └ HashJoin[(x = x)]",
                "    ├ HashJoin[(x = z)]",
                "    │  ├ Collect[doc.j2 | [x] | true]",
                "    │  └ Collect[doc.j3 | [z] | true]",
                "    └ Collect[doc.j1 | [x] | true]"
        );

        execute(stmt);
        assertThat(response).hasRows(
            "1| 1| 1",
            "2| 2| 2",
            "3| 3| 3");
    }

    @Test
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(value = 0)
    public void test_cross_join_on_top_of_fetch() throws Exception {
        execute("create table tt1 (a int, b int)");
        execute("create table tt2 (a int, b int, c int)");
        execute("insert into tt1 (a, b) SELECT a, a FROM generate_series(1, 100, 1) as g (a)");
        execute("insert into tt2 (a, b, c) SELECT a, a, a FROM generate_series(1, 100, 1) as g (a)");
        execute("refresh table tt1, tt2");
        execute("analyze");

        String stmt = "SELECT * FROM (select a from tt1 order by b desc limit 1) i, tt2 WHERE c >= 50";
        assertThat(execute("explain (costs false) " + stmt)).hasLines(
            "Eval[a, a, b, c]",
            "  └ NestedLoopJoin[CROSS]",
            "    ├ Collect[doc.tt2 | [a, b, c] | (c >= 50)]",
            "    └ Rename[a] AS i",
            "      └ Eval[a]",
            "        └ Fetch[a, b]",
            "          └ Limit[1::bigint;0]",
            "            └ OrderBy[b DESC]",
            "              └ Collect[doc.tt1 | [_fetchid, b] | true]"
        );
        assertThat(execute(stmt)).hasRowCount(51);
    }

    /**
     * Verifies a bug in the HashJoinPhase building code resulting in hashing symbols being in the wrong order
     * and such it's generated hash-code won't match anymore.
     *
     * https://github.com/crate/crate/issues/14583
     */
    @UseJdbc(1)
    @UseHashJoins(1)
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_ensure_hash_symbols_match_after_hash_join_is_reordered() {
        execute("create table doc.t1(a int, b int)");
        execute("create table doc.t2(c int, d int)");
        execute("create table doc.t3(e int, f int)");

        execute("insert into doc.t1(a,b) values(1,2)");
        execute("insert into doc.t2(c,d) values (1,3),(5,6)");
        execute("insert into doc.t3(e,f) values (3,2)");
        refresh();
        execute("analyze");

        var stmt = "SELECT t3.e FROM t1 JOIN t3 ON t1.b = t3.f JOIN t2 ON t1.a = t2.c WHERE t2.d =t3.e";
        assertThat(execute("explain " + stmt)).hasLines(
                "Eval[e] (rows=0)",
                "  └ Eval[b, a, e, f, c, d] (rows=0)",
                "    └ HashJoin[((a = c) AND (d = e))] (rows=0)",
                "      ├ Collect[doc.t2 | [c, d] | true] (rows=2)",
                "      └ HashJoin[(b = f)] (rows=1)",
                "        ├ Collect[doc.t1 | [b, a] | true] (rows=1)",
                "        └ Collect[doc.t3 | [e, f] | true] (rows=1)"
        );

        execute(stmt);
        assertThat(response).hasRows("3");
    }

    @Test
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    public void test_eliminate_cross_join() throws Exception {
        execute("create table t1 (x int)");
        execute("create table t2 (y int)");
        execute("create table t3 (z int)");

        String stmt = "SELECT * FROM t1 CROSS JOIN t2 INNER JOIN t3 ON t1.x = t3.z AND t3.z = t2.y;";
        execute("explain (costs false) " + stmt);

        assertThat(response).hasLines(
            "Eval[x, y, z]",
            "  └ HashJoin[(z = y)]",
            "    ├ HashJoin[(x = z)]",
            "    │  ├ Collect[doc.t1 | [x] | true]",
            "    │  └ Collect[doc.t3 | [z] | true]",
            "    └ Collect[doc.t2 | [y] | true]"
        );
    }

    @Test
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    public void test_eliminate_cross_join_with_filter() throws Exception {
        execute("create table t1 (x int)");
        execute("create table t2 (y int)");
        execute("create table t3 (z int)");

        String stmt = "SELECT * FROM t1 CROSS JOIN t2 INNER JOIN t3 ON t1.x = t3.z AND t3.z = t2.y WHERE t1.x > 1";
        execute("explain (costs false) " + stmt);

        assertThat(response).hasLines(
            "Eval[x, y, z]",
            "  └ HashJoin[(z = y)]",
            "    ├ HashJoin[(x = z)]",
            "    │  ├ Collect[doc.t1 | [x] | (x > 1)]",
            "    │  └ Collect[doc.t3 | [z] | true]",
            "    └ Collect[doc.t2 | [y] | true]"
        );
    }

    /**
     * https://github.com/crate/crate/issues/14961
     */
    @Test
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    public void test_number_of_hash_symbol_match_lhs_rhs_with_nested_joins_with_three_tuples() throws Exception {
        execute("create table t1 (a int, x int)");
        execute("create table t2 (b int, y int)");
        execute("create table t3 (c int)");

        execute("insert into t1 values (1,2)");
        execute("insert into t2 values (1,2)");
        execute("insert into t3 values (1)");
        execute("refresh table t1, t2, t3");

        var stmt = "SELECT * FROM t1,t2, t3 WHERE t3.c = t1.a AND t3.c = t2.b AND t1.a = t2.b and t1.x = t2.y";
        execute("explain (costs false) " + stmt);

        assertThat(response).hasLines(
            "Eval[a, x, b, y, c]",
                "  └ HashJoin[((c = b) AND ((a = b) AND (x = y)))]",
                "    ├ HashJoin[(c = a)]",
                "    │  ├ Collect[doc.t3 | [c] | true]",
                "    │  └ Collect[doc.t1 | [a, x] | true]",
                "    └ Collect[doc.t2 | [b, y] | true]"
        );

        execute(stmt);
        assertThat(response).hasRows("1| 2| 1| 2| 1");
    }

}
