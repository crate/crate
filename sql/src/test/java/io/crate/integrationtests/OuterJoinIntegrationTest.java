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
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
@UseJdbc
public class OuterJoinIntegrationTest extends SQLTransportIntegrationTest {

    private void setupOuterJoinTestData() {
        execute("create table employees (id integer, name string, office_id integer, profession_id integer)");
        execute("create table offices (id integer, name string)");
        execute("create table professions (id integer, name string)");
        ensureYellow();
        execute("insert into employees (id, name, office_id, profession_id) values" +
                " (1, 'Trillian', 2, 3), (2, 'Ford Perfect', 4, 2), (3, 'Douglas Adams', 3, 1)");
        execute("insert into offices (id, name) values (1, 'Hobbit House'), (2, 'Entresol'), (3, 'Chief Office')");
        execute("insert into professions (id, name) values (1, 'Writer'), (2, 'Traveler'), (3, 'Commander'), (4, 'Janitor')");
        execute("refresh table employees, offices, professions");
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
        setupOuterJoinTestData();

        // which employee works in which office?
        execute("select persons.name, offices.name from" +
                " employees as persons left join offices on office_id = offices.id" +
                " order by offices.id");
        assertThat(printedTable(response.rows()), is("Trillian| Entresol\n" +
                                                     "Douglas Adams| Chief Office\n" +
                                                     "Ford Perfect| NULL\n"));
    }

    @Test
    public void test3TableLeftOuterJoin() throws Exception {
        setupOuterJoinTestData();
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
    public void testRightOuterJoin() throws Exception {
        setupOuterJoinTestData();

        // which employee works in which office?
        execute("select persons.name, offices.name from" +
                " offices right join employees as persons on office_id = offices.id" +
                " order by offices.id");
        assertThat(printedTable(response.rows()), is("Trillian| Entresol\n" +
                                                     "Douglas Adams| Chief Office\n" +
                                                     "Ford Perfect| NULL\n"));
    }

    @Test
    public void test3TableRightOuterJoin() throws Exception {
        setupOuterJoinTestData();
        execute(
            "select professions.name, employees.name, offices.name from" +
            " offices right join employees on profession_id = professions.id" +
            " right join professions on office_id = offices.id" +
            " order by professions.id");
        assertThat(printedTable(response.rows()), is("Writer| Douglas Adams| Chief Office\n" +
                                                     "Traveler| Ford Perfect| NULL\n" +
                                                     "Commander| Trillian| Entresol\n" +
                                                     "Janitor| NULL| NULL\n"));
    }

    @Test
    public void testFullOuterJoin() throws Exception {
        setupOuterJoinTestData();

        execute("select persons.name, offices.name from" +
                " offices full join employees as persons on office_id = offices.id" +
                " order by offices.id");
        assertThat(printedTable(response.rows()), is("NULL| Hobbit House\n" +
                                                     "Trillian| Entresol\n" +
                                                     "Douglas Adams| Chief Office\n" +
                                                     "Ford Perfect| NULL\n"));
    }
}
