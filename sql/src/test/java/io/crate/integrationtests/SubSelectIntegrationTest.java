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

import io.crate.action.sql.SQLActionException;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class SubSelectIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void testSubSelectOrderBy() throws Exception {
        setup.setUpCharacters();

        execute("select i, name from (select id as i, name from characters order by name) as ch order by i desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("4| Arthur\n" +
               "1| Arthur\n" +
               "2| Ford\n" +
               "3| Trillian\n"));
    }

    @Test
    public void testSubSelectWhere() throws Exception {
        setup.setUpCharacters();

        execute("select id, name " +
                "from (select * from characters where female = true) as ch " +
                "where name like 'Arthur'");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("4| Arthur\n"));
    }

    @Test
    public void testSubSelectWhereDocKey() throws Exception {
        setup.setUpCharacters();

        execute("select id, name " +
                "from (select * from characters where female = true) as ch " +
                "where id = 4");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("4| Arthur\n"));
    }

    @Test
    public void testSubSelectLimitOffset() throws Exception {
        setup.setUpLocations();
        execute("refresh table locations");

        execute("select name " +
                "from (select * from locations order by name limit 10 offset 5) as l " +
                "limit 5 offset 4");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("End of the Galaxy\n" +
               "Galactic Sector QQ7 Active J Gamma\n" +
               "North West Ripple\n" +
               "Outer Eastern Rim\n"));
    }

    @Test
    public void testSubSelectOutputs() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        refresh();

        execute("select aa, (xxi + 1) " +
                "from (select (xx + i) as xxi, concat(a, a) as aa " +
                " from (select a, i, (x + x) as xx from t1) as t) as tt " +
                "order by aa");

        assertThat(TestingHelpers.printedTable(response.rows()),
            is("aa| 9\n" +
               "bb| 14\n" +
               "cc| 20\n" +
               "dd| 30\n"));
    }

    @Test
    public void testReferenceToNestedField() throws Exception {
        setup.groupBySetup();

        execute("select gender, minAge from ( " +
                "  select gender, min(age) as minAge from characters group by gender" +
                ") as ch " +
                "where gender = 'male'");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("male| 34\n"));
    }

    @Test
    public void testReferenceToNestedAggregatedField() throws Exception {
        setup.groupBySetup();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("complex sub selects are not supported");
        execute("select gender, minAge from ( " +
                "  select gender, min(age) as minAge from characters group by gender" +
                ") as ch " +
                "where (minAge * 2) < 120");
    }

    @Test
    public void testNestedGroupByAggregation() throws Exception {
        setup.groupBySetup();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("complex sub selects are not supported");
        execute("select count(*) from (" +
                "  select min(age) as minAge from characters group by gender) as ch " +
                "group by minAge");
    }

    @Test
    public void testOrderingOnNestedAggregation() throws Exception {
        setup.groupBySetup();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("complex sub selects are not supported");
        execute("select race, avg(age) as avgAge from ( " +
                "  select * from characters where gender = 'male' order by age) as ch " +
                "group by race");
    }

    @Test
    public void testFilterOnSubSelectWithJoins() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        execute("create table t2 (a string, i integer, y integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("insert into t2 (a, i, y) values ('aa', 22, 33),('bb', 33, 55),('cc', 55, 77),('dd', 77, 111)");
        refresh();

        execute("select col1, col2 from ( " +
                "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
                "  from t1, t2 where t2.y > 60) as t " +
                "where col1 = 'a' order by col3");

        assertThat(TestingHelpers.printedTable(response.rows()),
            is("a| 55\n" +
               "a| 77\n"));
    }

    @Test
    public void testNestedSubSelectWithJoins() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        execute("create table t2 (a string, i integer, y integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("insert into t2 (a, i, y) values ('aa', 22, 33),('bb', 33, 55),('cc', 55, 77),('dd', 77, 111)");
        refresh();

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1, t2 where t1.a='a' or t2.a='aa') as t) as tt " +
                "order by aa, xyi");

        assertThat(TestingHelpers.printedTable(response.rows()),
            is("aaa| 58\n" +
               "abb| 91\n" +
               "acc| 135\n" +
               "add| 191\n" +
               "baa| 60\n" +
               "caa| 62\n" +
               "daa| 66\n"));
    }

    @Test
    public void testNestedSubSelectWithOuterJoins() throws Exception {
        execute("create table t1 (a string, i integer, x integer)");
        execute("create table t2 (a string, i integer, y integer)");
        ensureYellow();

        execute("insert into t1 (a, i, x) values ('a', 2, 3),('b', 3, 5),('c', 5, 7),('d', 7, 11)");
        execute("insert into t2 (a, i, y) values ('a', 22, 33),('bb', 33, 55),('cc', 55, 77),('dd', 77, 111)");
        refresh();

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1 left join t2 on t1.a = t2.a where t1.a='a') as t) as tt " +
                "order by aa, xyi");

        assertThat(TestingHelpers.printedTable(response.rows()),
            is("aa| 58\n"));

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1 right join t2 on t1.a = t2.a where t1.a='a' or t2.a in ('aa', 'bb')) as t) as tt " +
                "order by aa, xyi");

        assertThat(TestingHelpers.printedTable(response.rows()),
            is("aa| 58\n" +
               "bb| NULL\n"));

        execute("select aa, xyi from (" +
                "  select (xy + i) as xyi, aa from (" +
                "    select concat(t1.a, t2.a) as aa, t2.i, (t1.x + t2.y) as xy " +
                "    from t1 full join t2 on t1.a = t2.a where t1.a='a' or t2.a in ('aa', 'bb')) as t) as tt " +
                "order by aa, xyi");

        assertThat(TestingHelpers.printedTable(response.rows()),
            is("aa| 58\n" +
               "bb| NULL\n"));
    }
}
