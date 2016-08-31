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

import io.crate.testing.UseJdbc;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.core.Is.is;

@UseJdbc
public class OuterJoinIntegrationTest extends SQLTransportIntegrationTest {

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
    public void testLeftOuterJoin() throws Exception {
        // which employee works in which office?
        execute("select persons.name, offices.name from" +
                " employees as persons left join offices on office_id = offices.id" +
                " order by offices.id");
        assertThat(printedTable(response.rows()), is("Trillian| Entresol\n" +
                                                     "Douglas Adams| Chief Office\n" +
                                                     "Ford Perfect| NULL\n"));
    }

    public void testLeftOuterJoinOrderOnOuterTable() throws Exception {
        // which employee works in which office?
        execute("select persons.name, offices.name from" +
                " employees as persons left join offices on office_id = offices.id" +
                " order by offices.name nulls first");
        assertThat(printedTable(response.rows()), is("Ford Perfect| NULL\n" +
                                                     "Douglas Adams| Chief Office\n" +
                                                     "Trillian| Entresol\n"));
    }

    @Test
    public void test3TableLeftOuterJoin() throws Exception {
        execute(
            "select professions.name, employees.name, offices.name from" +
            " professions left join employees on profession_id = professions.id" +
            " left join offices on office_id = offices.id" +
            " order by professions.id");
        assertThat(printedTable(response.rows()), is("Writer| Douglas Adams| Chief Office\n" +
                                                     "Traveler| Ford Perfect| NULL\n" +
                                                     "Commander| Trillian| Entresol\n" +
                                                     "Janitor| NULL| NULL\n"));
    }

    @Test
    public void test3TableLeftOuterJoinOrderByOuterTable() throws Exception {
        execute(
            "select professions.name, employees.name, offices.name from" +
            " professions left join employees on profession_id = professions.id" +
            " left join offices on office_id = offices.id" +
            " order by offices.name nulls first, professions.id nulls first");
        assertThat(printedTable(response.rows()), is("Traveler| Ford Perfect| NULL\n" +
                                                     "Janitor| NULL| NULL\n" +
                                                     "Writer| Douglas Adams| Chief Office\n" +
                                                     "Commander| Trillian| Entresol\n"));
    }

    @Test
    public void testRightOuterJoin() throws Exception {
        execute("select offices.name, persons.name from" +
                " employees as persons right join offices on office_id = offices.id" +
                " order by offices.id");
        assertThat(printedTable(response.rows()), is("Hobbit House| NULL\n" +
                                                     "Entresol| Trillian\n" +
                                                     "Chief Office| Douglas Adams\n"));
    }

    @Test
    public void test3TableLeftAndRightOuterJoin() throws Exception {
        execute(
            "select professions.name, employees.name, offices.name from" +
            " offices left join employees on office_id = offices.id" +
            " right join professions on profession_id = professions.id" +
            " order by professions.id");
        assertThat(printedTable(response.rows()), is("Writer| Douglas Adams| Chief Office\n" +
                                                     "Traveler| NULL| NULL\n" +
                                                     "Commander| Trillian| Entresol\n" +
                                                     "Janitor| NULL| NULL\n"));
    }

    @Test
    public void testFullOuterJoin() throws Exception {
        execute("select persons.name, offices.name from" +
                " offices full join employees as persons on office_id = offices.id" +
                " order by offices.id");
        assertThat(printedTable(response.rows()), is("NULL| Hobbit House\n" +
                                                     "Trillian| Entresol\n" +
                                                     "Douglas Adams| Chief Office\n" +
                                                     "Ford Perfect| NULL\n"));
    }

    @Test
    public void testOuterJoinWithFunctionsInOrderBy() throws Exception {
        execute("select coalesce(persons.name, ''), coalesce(offices.name, '') from" +
                " offices full join employees as persons on office_id = offices.id" +
                " order by 1, 2");
        assertThat(printedTable(response.rows()), is("| Hobbit House\n" +
                                                     "Douglas Adams| Chief Office\n" +
                                                     "Ford Perfect| \n" +
                                                     "Trillian| Entresol\n"));
    }

    @Test
    public void testLeftJoinWithFilterOnInner() throws Exception {
        execute("select employees.name, offices.name from" +
                " employees left join offices on office_id = offices.id" +
                " where employees.id < 3" +
                " order by offices.id");
        assertThat(printedTable(response.rows()), is("Trillian| Entresol\n" +
                                                     "Ford Perfect| NULL\n"));
    }

    @Test
    public void testLeftJoinWithFilterOnOuter() throws Exception {
        execute("select employees.name, offices.name from" +
                " employees left join offices on office_id = offices.id" +
                " where offices.size > 100" +
                " order by offices.id");
        assertThat(printedTable(response.rows()), is("Douglas Adams| Chief Office\n"));
    }
}
