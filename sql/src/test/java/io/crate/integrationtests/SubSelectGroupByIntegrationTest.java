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
import io.crate.testing.UseJdbc;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@UseJdbc
public class SubSelectGroupByIntegrationTest extends SQLTransportIntegrationTest {

    @Before
    public void initTestData() throws Exception {
        execute("create table t1 (x int, y int)");
        execute("create table t2 (z int)");
        execute("create table t3 (id int, company_id int, country int)");
        ensureYellow();

        execute("insert into t1 (x, y) values (1, 3), (1, 4), (2, 5)");
        execute("insert into t2 (z) values (4), (5), (6)");
        execute("insert into t3 (id, company_id, country) values (1, 8, 3), (2, 9, 4), (3, 10, 5)");
        execute("refresh table t1, t2, t3");
    }

    @Test
    public void testSelect() throws Exception {
        execute(
            "select count(x) from (select x from t1 limit 1) as tt " +
            "group by x"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testSelectWithWhereClause() throws Exception {
        execute(
            "select count(x) from (select x from t1 limit 3) as tt " +
            "where x = 2 " +
            "group by x"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testDistributedSelectWithWhereClause() throws Exception {
        execute(
            "select count(x) from (select x from t1 group by x limit 1) as tt " +
            "group by x " +
            "limit 1"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testAggregationWithGroupByAndOrderBy() throws Exception {
        execute(
            "select max(id), country from (select * from t3 order by 2 limit 1) as tt " +
            "group by country " +
            "order by 2"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("1| 3\n"));
    }

    @Test
    public void testAggregationWithJoinInSubselect() throws Exception {
        execute(
            "select max(country), count from (select count(*) as count, country from t3 group by country order by 2) as t " +
            "group by count " +
            "order by 1 " +
            "limit 100 "
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("5| 1\n"));
    }

    @Test
    public void testFunctionAndAggregationWithJoinInSubselect() throws Exception {
        execute(
            "select c + 100, max(max) from (select company_id + 10 as c, max(country) as max from t3 group by company_id + 10) t " +
            "group by c + 100 " +
            "limit 100 "
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("118| 3\n119| 4\n120| 5\n"));
    }

    @Test
    public void testJoinWithSubqueries() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("JOIN with sub queries is not supported");
        execute(
            "select x from (select x from t1 limit 3) as tt1, " +
            "(select z from t2 limit 1) as tt2 " +
            "group by x"
        );
    }
}
