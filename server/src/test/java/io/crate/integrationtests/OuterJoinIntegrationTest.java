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


import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

public class OuterJoinIntegrationTest extends IntegTestCase {

    @Before
    public void setupTestData() {
        execute("create table employees (id integer, name string, office_id integer, profession_id integer)");
        execute("create table offices (id integer, name string, size integer)");
        execute("create table professions (id integer, name string)");
        ensureYellow();
        execute("insert into employees (id, name, office_id, profession_id) values" +
                " (1, 'Trillian', 2, 3), (2, 'Ford Perfect', 4, 2), (3, 'Douglas Adams', 3, 1)");
        execute("insert into offices (id, name, size) values (1, 'Hobbit House', 10), (2, 'Entresol', 70), (3, 'Chief Office', 150)");
        execute("insert into professions (id, name) values (1, 'Writer'), (2, 'Traveler'), (3, 'Commander'), (4, 'Janitor')");
        execute("refresh table employees, offices, professions");
    }

    @Test
    public void testLeftOuterJoin() {
        // which employee works in which office?
        execute("select persons.name, offices.name from" +
                " employees as persons left join offices on office_id = offices.id" +
                " order by persons.id");
        assertThat(response).hasRows(
            "Trillian| Entresol",
            "Ford Perfect| NULL",
            "Douglas Adams| Chief Office");
    }

    @Test
    public void testLeftOuterJoinOrderOnOuterTable() {
        // which employee works in which office?
        execute("select persons.name, offices.name from" +
                " employees as persons left join offices on office_id = offices.id" +
                " order by offices.name nulls first");
        assertThat(response).hasRows(
            "Ford Perfect| NULL",
            "Douglas Adams| Chief Office",
            "Trillian| Entresol");
    }

    @Test
    public void test3TableLeftOuterJoin() {
        execute(
            "select professions.name, employees.name, offices.name from" +
            " professions left join employees on profession_id = professions.id" +
            " left join offices on office_id = offices.id" +
            " order by professions.id");
        assertThat(response).hasRows(
            "Writer| Douglas Adams| Chief Office",
            "Traveler| Ford Perfect| NULL",
            "Commander| Trillian| Entresol",
            "Janitor| NULL| NULL");
    }

    @Test
    public void test3TableLeftOuterJoinOrderByOuterTable() {
        execute(
            "select professions.name, employees.name, offices.name from" +
            " professions left join employees on profession_id = professions.id" +
            " left join offices on office_id = offices.id" +
            " order by offices.name nulls first, professions.id nulls first");
        assertThat(response).hasRows(
            "Traveler| Ford Perfect| NULL",
            "Janitor| NULL| NULL",
            "Writer| Douglas Adams| Chief Office",
            "Commander| Trillian| Entresol");
    }

    @Test
    public void testRightOuterJoin() {
        execute("select offices.name, persons.name from" +
                " employees as persons right join offices on office_id = offices.id" +
                " order by offices.id");
        assertThat(response).hasRows(
            "Hobbit House| NULL",
            "Entresol| Trillian",
            "Chief Office| Douglas Adams");
    }

    @Test
    public void test3TableLeftAndRightOuterJoin() {
        execute(
            "select professions.name, employees.name, offices.name from" +
            " offices left join employees on office_id = offices.id" +
            " right join professions on profession_id = professions.id" +
            " order by professions.id");
        assertThat(response).hasRows(
            "Writer| Douglas Adams| Chief Office",
            "Traveler| NULL| NULL",
            "Commander| Trillian| Entresol",
            "Janitor| NULL| NULL");
    }

    @Test
    public void testFullOuterJoin() {
        execute("select persons.name, offices.name from" +
                " offices full join employees as persons on office_id = offices.id" +
                " order by offices.id");
        assertThat(response).hasRows(
            "NULL| Hobbit House",
            "Trillian| Entresol",
            "Douglas Adams| Chief Office",
            "Ford Perfect| NULL");
    }

    @Test
    public void testFullOuterJoinWithFilters() {
        // It's rewritten to an Inner Join because of the filtering condition in where clause
        execute("select persons.name, offices.name from" +
                " offices full join employees as persons on office_id = offices.id" +
                " where offices.name='Entresol' and persons.name='Trillian' " +
                " order by offices.id");
        assertThat(response).hasRows("Trillian| Entresol");
    }

    @Test
    public void test_filter_will_be_applied_after_outer_join() {
        execute("create table t1 (id int, is_match int)");
        execute("create table t2 (id int, t1_id int)");
        execute("""
            insert into t1 (id, is_match) values
            (1, 0),
            (2, 1),
            (36, 1)""");
        execute("""
            insert into t2 (id, t1_id) values
            (1, 1),
            (2, 2),
            (3, null)""");   // this row must be filtered out after the full join
        execute("refresh table t1, t2");

        execute("SELECT t2.id, t2.t1_id, t1.id, t1.is_match " +
                "FROM t2 " +
                "FULL OUTER JOIN t1 ON (t1.id = t2.t1_id) " +
                "WHERE (t1.is_match = 1) ");
        assertThat(response).hasRows(
            "2| 2| 2| 1",
            "NULL| NULL| 36| 1");
    }

    @Test
    public void testOuterJoinWithFunctionsInOrderBy() {
        execute("select coalesce(persons.name, ''), coalesce(offices.name, '') from" +
                " offices full join employees as persons on office_id = offices.id" +
                " order by 1, 2");
        assertThat(response).hasRows(
            "| Hobbit House",
            "Douglas Adams| Chief Office",
            "Ford Perfect| ",
            "Trillian| Entresol");
    }

    @Test
    public void testLeftJoinWithFilterOnInner() {
        execute("select employees.name, offices.name from" +
                " employees left join offices on office_id = offices.id" +
                " where employees.id < 3" +
                " order by offices.id");
        assertThat(response).hasRows(
            "Trillian| Entresol",
            "Ford Perfect| NULL");
    }

    @Test
    public void testLeftJoinWithFilterOnOuter() {
        // It's rewritten to an Inner Join because of the filtering condition in where clause
        execute("select employees.name, offices.name from" +
                " employees left join offices on office_id = offices.id" +
                " where offices.size > 100" +
                " order by offices.id");
        assertThat(response).hasRows("Douglas Adams| Chief Office");
    }

    @Test
    public void testLeftJoinWithCoalesceOnOuter() throws Exception {
        // coalesce causes a NULL row which is emitted from the join to become a match
        execute("select employees.name, offices.name from" +
                " employees left join offices on office_id = offices.id" +
                " where coalesce(offices.size, cast(110 as integer)) > 100" +
                " order by offices.id");
        assertThat(response).hasRows(
            "Douglas Adams| Chief Office",
            "Ford Perfect| NULL");
    }
}
