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

package io.crate.integrationtests

import org.junit.Test

import static com.carrotsearch.randomizedtesting.RandomizedTest.$
import static io.crate.testing.TestingHelpers.printedTable

class TableFunctionITest extends SQLTransportIntegrationTest {

    @Test
    public void testSelectFromUnnest() throws Exception {
        execute("select * from unnest([1, 2], ['Trillian', 'Marvin'])")
        assert response.rowCount() == 2L
        assert printedTable(response.rows()) == "" +
            "1| Trillian\n" +
            "2| Marvin\n"
    }

    @Test
    public void testSelectFromUnnestWithOrderByAndLimit() throws Exception {
        execute("select * from unnest([1, 2], ['Trillian', 'Marvin']) order by col1 desc limit 1")
        assert response.rowCount() == 1L
        assert printedTable(response.rows()) == "2| Marvin\n"
    }

    @Test
    public void testSelectFromUnnestWithScalarFunction() throws Exception {
        execute("select substr(col2, 0, 1) from unnest([1, 2], ['Trillian', 'Marvin']) order by col1 limit 1")
        assert printedTable(response.rows()) == "T\n"
    }

    @Test
    public void testInsertIntoFromSelectUnnest() throws Exception {
        execute("create table t (id int primary key, name string) with (number_of_replicas = 0)")
        ensureYellow()

        Object[] args = $($(1, 2), $("Marvin", "Trillian")); // non-bulk request
        execute("insert into t (select * from unnest(?, ?))", args)
        execute("refresh table t");

        assert printedTable(execute("select * from t order by id").rows()) == "1| Marvin\n2| Trillian\n"
    }

    @Test
    public void testGroupByFromUnnest() throws Exception {
        execute("select col1, count(*) from unnest(['Marvin', 'Marvin', 'Trillian']) group by col1 order by 2 desc")
        assert printedTable(response.rows()) == "Marvin| 2\nTrillian| 1\n"
    }

    @Test
    public void testGlobalAggregationFromUnnest() throws Exception {
        assert execute("select max(col1) from unnest([1, 2, 3, 4])").rows()[0][0] == 4;
    }

    @Test
    public void testJoinUnnestWithTable() throws Exception {
        execute("create table t (id int primary key)")
        ensureYellow()
        execute("insert into t (id) values (1)");
        execute("refresh table t");
        assert printedTable(execute("select * from unnest([1, 2]) inner join t on t.id = col1::integer").rows()) == "1| 1\n"
    }

    @Test
    public void testWhereClauseIsEvaluated() throws Exception {
        execute("select col1 from unnest([1, 2]) where col1 = 2")
        assert printedTable(response.rows()) == "2\n"
    }

    @Test
    public void testValueExpression() throws Exception {
        execute("select * from unnest(coalesce([1,2]))")
        assert printedTable(response.rows()) == "1\n2\n"
    }
}
