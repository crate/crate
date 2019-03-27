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
import io.crate.data.Paging;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class QueryThenFetchIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testCrateSearchServiceSupportsOrderByOnFunctionWithBooleanReturnType() throws Exception {
        execute("create table t (b byte, name string) with (number_of_replicas = 0)");
        execute("insert into t (name, b) values ('Marvin', 0), ('Trillian', 1), ('Arthur', 2), ('Max', 3)");
        execute("refresh table t");
        ensureGreen();

        execute("select * from t order by substr(name, 1, 1) = 'M', b");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "1| Trillian\n" +
            "2| Arthur\n" +
            "0| Marvin\n" +
            "3| Max\n"));
    }

    @Test
    public void testThatErrorsInSearchResponseCallbackAreNotSwallowed() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(containsString("d != java.lang.String"));

        execute("create table t (s string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (s) values ('foo')");
        execute("refresh table t");

        execute("select format('%d', s) from t");
    }

    @Test
    public void testTestWithTimestampThatIsInIntegerRange() {
        execute(
            "create table t (" +
            "   ts timestamp with time zone" +
            ") clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (ts) values (0)");
        execute("insert into t (ts) values (1425980155)");
        execute("refresh table t");

        execute("select extract(day from ts) from t order by 1");
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(17));
    }

    @Test
    public void testPushBasedQTF() throws Exception {
        // use high limit to trigger push based qtf

        execute("create table t (x string) with (number_of_replicas = 0)");
        execute("insert into t (x) values ('a')");
        execute("refresh table t");

        execute("select * from t limit ?", new Object[]{Paging.PAGE_SIZE + 10000});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testPushBasedQTFWithPaging() throws Exception {
        Paging.PAGE_SIZE = 10;
        // insert more docs than PAGE_SIZE and query at least 2 times of it to trigger push of
        // at least 2 pages
        int docCount = (Paging.PAGE_SIZE * 2) + 2;

        Object[][] bulkArgs = new Object[docCount][1];
        for (int i = 0; i < docCount; i++) {
            bulkArgs[i][0] = i;
        }

        execute("create table t (x int) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (x) values (?)", bulkArgs);
        execute("refresh table t");

        Long limit = (long) (docCount - 1);
        execute("select * from t limit ?", new Object[]{limit});
        assertThat(response.rowCount(), is(limit));

        // test if all is fine if we limit more than we have
        limit += 10;
        execute("select * from t limit ?", new Object[]{limit});
        assertThat(response.rowCount(), is((long) docCount));

        // test with sorting
        execute("select * from t order by x limit ?", new Object[]{limit});
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n21\n"));
        assertThat(response.rowCount(), is((long) docCount));
    }
}
