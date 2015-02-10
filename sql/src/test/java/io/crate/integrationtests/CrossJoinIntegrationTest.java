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

import io.crate.action.sql.SQLActionException;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class CrossJoinIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testSelectSubscript() throws Exception {
        setup.setUpCharacters();
        setup.setUpLocations();
        execute("select female, race['name'] from characters, locations");
        assertThat(response.rowCount(), is(52L));
    }

    @Test
    public void testSimpleThreeTableJoin() throws Exception {
        setup.setUpBooks();
        setup.setUpCharacters();
        setup.setUpLocations();
        execute("select title, characters.name, locations.name from books, characters, locations");
        assertThat(response.rowCount(), is(156L));
    }

    @Test
    public void testCrossJoinWithFunction() throws Exception {
        setup.setUpEmployees();
        setup.setUpBooks();
        execute("select income * 2 from employees, books limit 1");
    }

    @Test
    public void testAmbiguousAlias() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The table or alias 'a' is specified more than once");
        setup.setUpBooks();
        execute("select a.title, a.title from books as a CROSS JOIN books as a");
    }

    @Test
    public void testAmbiguousTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The table or alias 'books' is specified more than once");
        setup.setUpBooks();
        execute("select title, title from books, books, books");
    }

    @Test
    public void testColumnOrder() throws Exception {
        setup.setUpBooks();
        setup.setUpEmployees();
        setup.setUpLocations();
        execute("select title, locations.date, age from employees, books, locations");
        assertArrayEquals(new String[]{"title", "locations.date", "age"}, response.cols());
        Object[] row = response.rows()[0];
        assertThat(row[0], is(instanceOf(String.class)));
        assertThat(row[1], is(instanceOf(Long.class)));
        assertThat(row[2], is(instanceOf(Integer.class)));
    }

    @Test
    public void testJoinWithEmptyTable() throws Exception {
        setup.setUpBooks();
        execute("create table empty (id int primary key) clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();

        execute("select title, author from books CROSS JOIN empty");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testJoinTwoEmptyTables() throws Exception {
        execute("create table empty1 (id int primary key) clustered into 1 shards with (number_of_replicas=0)");
        execute("create table empty2 (id int primary key) clustered into 1 shards with (number_of_replicas=0)");
        ensureGreen();

        execute("select * from empty1 CROSS JOIN empty2");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testJoinOffset() throws Exception {
        setup.setUpCharacters();
        setup.setUpLocations();
        execute("select * from locations, characters offset 50");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testJoinHighLimit() throws Exception {
        setup.setUpCharacters();
        setup.setUpLocations();
        execute("select * from locations, characters limit 300");
        assertThat(response.rowCount(), is(52L));
    }

    @Test
    public void testJoinHighLimitAndOffset() throws Exception {
        setup.setUpCharacters();
        setup.setUpLocations();
        execute("select * from locations, characters limit 300 offset 200");
        assertThat(response.rowCount(), is(52L));
    }

    @Test
    public void testTwoTableCrossJoinWithOrderByTestThatOrderingIsCorrect() throws Exception {
        execute("create table colors (color string) clustered into 3 shards with (number_of_replicas = 0)");
        execute("create table sizes (size string) clustered into 2 shards with (number_of_replicas = 0)");
        ensureGreen();

        execute("insert into colors (color) values (?)", new Object[][] {
                new Object[] { "green" },
                new Object[] { "red" },
                new Object[] { "blue" },
        });
        execute("insert into sizes (size) values (?)", new Object[][] {
                new Object[] { "small" },
                new Object[] { "medium" },
                new Object[] { "large" },
        });
        execute("refresh table colors");
        execute("refresh table sizes");
        execute("select color, size from colors cross join sizes order by color, size");
        assertThat(response.rowCount(), is(9L));
        String expected = "blue| large\n" +
                "blue| medium\n" +
                "blue| small\n" +
                "green| large\n" +
                "green| medium\n" +
                "green| small\n" +
                "red| large\n" +
                "red| medium\n" +
                "red| small\n";
        assertThat(TestingHelpers.printedTable(response.rows()), is(expected));
    }
}