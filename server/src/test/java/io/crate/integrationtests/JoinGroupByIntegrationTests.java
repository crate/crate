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
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.testing.TestingHelpers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;

public class JoinGroupByIntegrationTests extends SQLIntegrationTestCase {

    @Before
    public void initTestData() throws Exception {
        createColorsAndFruits();
    }

    /**
     * Create some sample data.
     */
    private void createColorsAndFruits() {
        execute("create table colors (id integer, name string)");
        execute("create table fruits (id integer, price float, name string)");
        ensureYellow();

        execute("insert into colors (id, name) values (1, 'red'), (2, 'yellow')");
        execute("insert into fruits (id, price, name) values (1, 1.9, 'apple'), (2, 0.8, 'banana'), (2, 0.5, 'lemon')");
        execute("refresh table colors, fruits");
    }

    @Test
    public void testJoinWithAggregationGroupBy() throws Exception {
        execute(
            "select colors.name, count(colors.name) " +
            "from colors, fruits " +
            "group by colors.name " +
            "order by colors.name DESC"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("yellow| 3\nred| 3\n"));
    }

    @Test
    public void testJoinWithGroupByLimitAndOffset() throws Exception {
        execute(
            "select colors.name " +
            "from colors, fruits " +
            "group by colors.name " +
            "order by colors.name DESC " +
            "limit 1 offset 1"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("red\n"));
    }

    @Test
    public void testJoinWithGroupByAndHaving() throws Exception {
        execute(
            "select count(fruits.name), colors.name " +
            "from colors, fruits " +
            "where colors.id = fruits.id " +
            "group by colors.name " +
            "having count(colors.name) = 1"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("1| red\n"));
    }

    @Test
    public void testJoinWithGroupByAndWhere() throws Exception {
        execute(
            "select colors.name " +
            "from colors, fruits " +
            "where colors.name='red' " +
            "group by colors.name"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("red\n"));
    }

    @Test
    public void testJoinWithAggregationScalarFunctionWithGroupBy() throws Exception {
        execute(
            "select fruits.name, max(colors.name), max(fruits.price * fruits.price + 10) + 10, max(fruits.name), count(colors.name) " +
            "from colors, fruits " +
            "group by fruits.name " +
            "order by name " +
            "limit 1 offset 1"
        );

        assertThat(
            TestingHelpers.printedTable(response.rows()),
            is("banana| yellow| 20.64| banana| 2\n")
        );
    }

    @Test
    public void testDistributedJoinWithAggregationScalarFunctionWithGroupBy() throws Exception {
        execute(
            "select fruits.name, max(colors.name), max(fruits.price * fruits.price + 10) + 10, max(fruits.name), count(colors.name) " +
            "from colors, fruits " +
            "where colors.id = fruits.id " +
            "group by fruits.name " +
            "having count(colors.id) > 0 " +
            "order by fruits.name " +
            "limit 1 offset 1"
        );

        assertThat(
            TestingHelpers.printedTable(response.rows()),
            is("banana| yellow| 20.64| banana| 1\n")
        );
    }

    @Test
    public void testHavingWithGroupBy() throws Exception {
        execute(
            "select fruits.name as name, price " +
            "from colors, fruits " +
            "group by fruits.name, price " +
            "having count(colors.name) > 0 " +
            "order by name, price"
        );

        assertThat(
            TestingHelpers.printedTable(response.rows()),
            is("apple| 1.9\n" +
               "banana| 0.8\n" +
               "lemon| 0.5\n")
        );
    }

    @Test
    public void testHavingWithGroupByAndFunction() throws Exception {
        execute(
            "select fruits.name as fruit_name, count(colors.name) as color_name_count, price " +
            "from colors, fruits " +
            "group by fruit_name, price " +
            "having abs(price) > 0.5 " +
            "order by fruit_name, price"
        );

        assertThat(
            TestingHelpers.printedTable(response.rows()),
            is("apple| 2| 1.9\n" +
               "banana| 2| 0.8\n")
        );
    }

    @Test
    public void testSelectWithJoinGroupByAndHaving() throws Exception {
        execute(
            "select fruits.name " +
            "from colors, fruits " +
            "where colors.id = fruits.id " +
            "group by fruits.name " +
            "having count(colors.id) > 0 " +
            "order by fruits.name " +
            "limit 1 offset 1"
        );

        assertThat(
            TestingHelpers.printedTable(response.rows()),
            is("banana\n")
        );
    }

    @Test
    public void testSelectWithAggregationJoinGroupByAndHaving() throws Exception {
        execute(
            "select fruits.name, max(colors.name), max(fruits.price * fruits.price + 10) + 10 " +
            "from colors, fruits " +
            "where colors.id = fruits.id " +
            "group by fruits.name " +
            "having count(colors.id) > 0 " +
            "order by fruits.name " +
            "limit 1 offset 1"
        );

        assertThat(
            TestingHelpers.printedTable(response.rows()),
            is("banana| yellow| 20.64\n")
        );
    }

    @Test
    public void testDistributedSelectWithJoinAndGroupBy() throws Exception {
        execute(
            "select max(colors.name), fruits.price * 10 " +
            "from colors, fruits " +
            "where colors.id = fruits.id " +
            "group by fruits.price * 10"
        );

        List<Object[]> rows = Arrays.asList(response.rows());
        rows.sort(OrderingByPosition.arrayOrdering(1, false, false));
        assertThat(
            TestingHelpers.printedTable(new CollectionBucket(rows)),
            is("yellow| 5.0\n" +
               "yellow| 8.0\n" +
               "red| 19.0\n")
        );
    }

    @Test
    public void testSelectWithJoinAndGroupBy() throws Exception {
        execute(
            "select max(colors.name), fruits.price * 10 " +
            "from colors, fruits " +
            "group by fruits.price * 10 "
        );

        List<Object[]> rows = Arrays.asList(response.rows());
        rows.sort(OrderingByPosition.arrayOrdering(1, false, false));

        assertThat(
            TestingHelpers.printedTable(new CollectionBucket(rows)),
            is("yellow| 5.0\n" +
               "yellow| 8.0\n" +
               "yellow| 19.0\n")
        );
    }

    @Test
    public void testSelectWithJoinGroupByAndOrderBy() throws Exception {
        execute(
            "select max(colors.name), fruits.price * 10 " +
            "from colors, fruits " +
            "group by fruits.price * 10 " +
            "order by fruits.price * 10"
        );

        assertThat(
            TestingHelpers.printedTable(response.rows()),
            is("yellow| 5.0\n" +
               "yellow| 8.0\n" +
               "yellow| 19.0\n")
        );
    }
}
