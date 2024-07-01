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
import org.junit.Test;

import io.crate.testing.SQLResponse;

public class SelectOrderByIntegrationTest extends IntegTestCase {

    @Test
    public void testSelectOrderByNullSortingASC() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        execute("select age from characters order by age");
        assertThat(response).hasRows(
            "32",
            "34",
            "43",
            "112",
            "NULL",
            "NULL",
            "NULL");
    }

    @Test
    public void testSelectOrderByNullSortingDESC() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        execute("select age from characters order by age desc");
        assertThat(response).hasRows(
            "NULL",
            "NULL",
            "NULL",
            "112",
            "43",
            "34",
            "32");
    }

    @Test
    public void testSelectOrderByNullSortingASCWithFunction() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        execute("select abs(age) from characters order by 1 asc");
        assertThat(response).hasRows(
            "32",
            "34",
            "43",
            "112",
            "NULL",
            "NULL",
            "NULL");
    }

    @Test
    public void testSelectOrderByNullSortingDESCWithFunction() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        execute("select abs(age) from characters order by 1 desc");
        assertThat(response).hasRows(
            "NULL",
            "NULL",
            "NULL",
            "112",
            "43",
            "34",
            "32");
    }


    @Test
    public void testSelectGroupByOrderByNullSortingASC() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        execute("select age from characters group by age order by age");
        assertThat(response).hasRows(
            "32",
            "34",
            "43",
            "112",
            "NULL");
    }

    @Test
    public void testSelectGroupByOrderByNullSortingDESC() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        execute("select age from characters group by age order by age desc");
        assertThat(response).hasRows(
            "NULL",
            "112",
            "43",
            "34",
            "32");
    }

    @Test
    public void testOrderByNullsFirstAndLast() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse response = execute(
            "select details['job'] from characters order by details['job'] nulls first limit 1");
        assertThat(response.rows()[0][0]).isNull();

        response = execute(
            "select details['job'] from characters order by details['job'] desc nulls first limit 1");
        assertThat(response.rows()[0][0]).isNull();

        response = execute(
            "select details['job'] from characters order by details['job'] nulls last");
        assertThat(response.rows()[((Long) response.rowCount()).intValue() - 1][0]).isNull();

        response = execute(
            "select details['job'] from characters order by details['job'] desc nulls last");
        assertThat(response.rows()[((Long) response.rowCount()).intValue() - 1][0]).isNull();


        response = execute(
            "select distinct details['job'] from characters order by details['job'] desc nulls last");
        assertThat(response.rows()[((Long) response.rowCount()).intValue() - 1][0]).isNull();
    }

    @Test
    public void testOrderByScalarOnColumnsWithNullValues() {
        execute(
            "create table t1 (" +
            "   i integer," +
            "   d double," +
            "   t timestamp with time zone," +
            "   str string" +
            ") clustered into 1 shards");
        execute("insert into t1 ( i, d, t, str) values " +
                "(null, null, null, null), " +
                "(null, 1.1, null, 'a'), " +
                "(2, 2.2, null, null)," +
                "(null, 3.3, 1521479461, null), " +
                "(4, null, 1521479462, 'b'), " +
                "(null, 1.0, null, null)");
        execute("refresh table t1");

        execute("select str from t1 order by upper(str) limit 5");
        assertThat(response).hasRows(
            "a",
            "b",
            "NULL",
            "NULL",
            "NULL");

        execute("select i from t1 order by ln(i)");
        assertThat(response.rows()[0][0]).isEqualTo(2);

        execute("select d from t1 order by ceil(d)");
        assertThat(response.rows()[0][0]).isEqualTo(1.0);

        execute("select i from t1 order by ceil(i)");
        assertThat(response.rows()[0][0]).isEqualTo(2);

        execute("select t from t1 order by date_trunc('year', 'Europe/London', t)");
        assertThat(response.rows()[0][0]).isEqualTo(1521479461L);
    }

    @Test
    public void testOrderByLiteralConstant() {
        execute("create table t1 (id int)");
        execute("insert into t1 (id) values (1), (2)");
        execute("refresh table t1");
        execute("select 1 + 0, id from t1 order by 1, 2"); // add 2nd order by to get deterministic results
        assertThat(response).hasRows(
            "1| 1",
            "1| 2");
    }
}
