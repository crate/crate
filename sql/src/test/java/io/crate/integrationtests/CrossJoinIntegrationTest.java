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
import io.crate.operation.projectors.sorting.OrderingByPosition;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.crate.testing.TestingHelpers.printRows;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.core.Is.is;

@ElasticsearchIntegrationTest.ClusterScope(minNumDataNodes = 2)
public class CrossJoinIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        assertThat(response.rowCount(), is(16L));
        assertThat(printedTable(response.rows()), is("" +
                "blob| blob\n" +
                "blob| doc\n" +
                "blob| information_schema\n" +
                "blob| sys\n" +
                "doc| blob\n" +
                "doc| doc\n" +
                "doc| information_schema\n" +
                "doc| sys\n" +
                "information_schema| blob\n" +
                "information_schema| doc\n" +
                "information_schema| information_schema\n" +
                "information_schema| sys\n" +
                "sys| blob\n" +
                "sys| doc\n" +
                "sys| information_schema\n" +
                "sys| sys\n"));
    }

    @Test
    public void testFilteredSelfJoin() throws Exception {
        execute("create table employees (salary float, name string)");
        ensureYellow();
        execute("insert into employees (salary, name) values (600, 'Trillian'), (200, 'Ford Perfect'), (800, 'Douglas Adams')");
        execute("refresh table employees");

        // TODO: once fetch is supported for cross joins, reset query to:
        // select more.name, less.name, (more.salary - less.salary) from employees as more, employees as less where more.salary > less.salary
        //        order by more.salary desc, less.salary desc
        execute("select more.name, less.name, (more.salary - less.salary) from employees as more, employees as less where more.salary > less.salary "+
                "order by more.salary desc, less.salary desc, more.name, less.name");
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
        execute("SELECT s.id, n.name FROM sys.shards s, sys.nodes n");
        assertThat(response.cols(), arrayContaining("s.id", "n.name"));
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

        execute("select users.* from users join events on users.id = events.user_id order by users.id");
    }
}
