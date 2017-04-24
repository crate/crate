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
        execute("create table t (x int, y int)");
        execute("create table u (z int)");
        ensureYellow();

        execute("insert into t (x, y) values (1, 3), (1, 4), (2, 5)");
        execute("insert into u (z) values (4), (5), (6)");
        execute("refresh table t, u");
    }

    @Test
    public void testSelect() throws Exception {
        execute(
            "select count(x) from (select x from t limit 1) as tt " +
            "group by x"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testSelectWithWhereClause() throws Exception {
        execute(
            "select count(x) from (select x from t limit 3) as tt " +
            "where x = 2 " +
            "group by x"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testDistributedSelectWithWhereClause() throws Exception {
        execute(
            "select count(x) from (select x from t group by x limit 1) as tt " +
            "group by x " +
            "limit 1"
        );
        assertThat(TestingHelpers.printedTable(response.rows()), is("1\n"));
    }

    @Test
    public void testJoinWithSubqueries() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("JOIN with sub queries is not supported");
        execute(
            "select x from (select x from t limit 3) as t1, " +
            "(select z from u limit 1) as t2 " +
            "group by x"
        );
    }
}
