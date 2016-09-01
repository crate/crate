/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.core.collections.CollectionBucket;
import io.crate.exceptions.Exceptions;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.printRows;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
@UseJdbc
public class JoinIntegrationTest extends SQLTransportIntegrationTest {

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
    public void testJoinOnEmptyPartitionedTables() throws Exception {
        execute("create table foo (id long) partitioned by (id)");
        execute("create table bar (id long) partitioned by (id)");
        ensureYellow();
        execute("select * from foo f, bar b where f.id = b.id");
        assertThat(printedTable(response.rows()), is(""));
    }

    @Test
    public void testCrossJoinJoinUnordered() throws Exception {
        execute("create table employees (size float, name string) clustered by (size) into 1 shards");
        execute("create table offices (height float, name string) clustered by (height) into 1 shards");
        ensureYellow();
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
        assertThat(printedTable(response.rows()), is("424\n574\n"));
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
                new int[] {0, 1}, new boolean[]{false, false}, new Boolean[]{null, null}).reverse());
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
                "tables.schema_name = 'sys' " +
                "and tables.table_name = 'shards' " +
                "and tables.schema_name = columns.schema_name " +
                "and tables.table_name = columns.table_name " +
                "order by columns.column_name " +
                "limit 4");
        assertThat(response.rowCount(), is(4L));
        assertThat(printedTable(response.rows()),
                is("strict| id\n" +
                   "strict| num_docs\n" +
                   "strict| orphan_partition\n" +
                   "strict| partition_ident\n"));
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
                new int[] { 0, 1}, new boolean[] { false, true }, new Boolean[] { null, null }).reverse());

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
        execute("insert into t (x) values (1)");
        execute("refresh table t");
        execute("select * from t as t1, t as t2");
    }

    @Test
    public void testFilteredSelfJoin() throws Exception {
        execute("create table employees (salary float, name string)");
        ensureYellow();
        execute("insert into employees (salary, name) values (600, 'Trillian'), (200, 'Ford Perfect'), (800, 'Douglas Adams')");
        execute("refresh table employees");

        execute("select more.name, less.name, (more.salary - less.salary) from employees as more, employees as less " +
                "where more.salary > less.salary "+
                "order by more.salary desc, less.salary desc");
        assertThat(printedTable(response.rows()), is("" +
                "Douglas Adams| Trillian| 200.0\n" +
                "Douglas Adams| Ford Perfect| 600.0\n" +
                "Trillian| Ford Perfect| 400.0\n"));
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
        execute("create table t_left (id long primary key, temp float, ref_id int) clustered into 2 shards with (number_of_replicas = 1)");
        execute("create table t_right (id int primary key, name string) clustered into 2 shards with (number_of_replicas = 1)");
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
    public void testFetchArrayAndAnalyedColumnsWithJoin() throws Exception {
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
    public void testJoinOnInformationSchema() throws Exception {
        execute("create table t (id int, name string) with (number_of_replicas = 1)");
        ensureYellow();
        execute("insert into t (id, name) values (1, 'Marvin')");
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

        expectedException.expect(IndexNotFoundException.class);
        try {
            execute(plan).resultFuture().get(1, TimeUnit.SECONDS);
        } catch (Throwable t) {
            throw Exceptions.unwrap(t);
        }
    }
}
