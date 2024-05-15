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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.data.Paging;
import io.crate.testing.Asserts;

public class QueryThenFetchIntegrationTest extends IntegTestCase {

    @Test
    public void testCrateSearchServiceSupportsOrderByOnFunctionWithBooleanReturnType() throws Exception {
        execute("create table t (b byte, name string) with (number_of_replicas = 0)");
        execute("insert into t (name, b) values ('Marvin', 0), ('Trillian', 1), ('Arthur', 2), ('Max', 3)");
        execute("refresh table t");
        ensureGreen();

        execute("select * from t order by substr(name, 1, 1) = 'M', b");
        assertThat(printedTable(response.rows())).isEqualTo(
            "1| Trillian\n" +
            "2| Arthur\n" +
            "0| Marvin\n" +
            "3| Max\n");
    }

    @Test
    public void testThatErrorsInSearchResponseCallbackAreNotSwallowed() throws Exception {
        execute("create table t (s string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (s) values ('foo')");
        execute("refresh table t");

        Asserts.assertSQLError(() -> execute("select format('%d', s) from t"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("d != java.lang.String");
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
        assertThat((Integer) response.rows()[0][0]).isEqualTo(1);
        assertThat((Integer) response.rows()[1][0]).isEqualTo(17);
    }

    @Test
    public void testPushBasedQTF() throws Exception {
        // use high limit to trigger push based qtf

        execute("create table t (x string) with (number_of_replicas = 0)");
        execute("insert into t (x) values ('a')");
        execute("refresh table t");

        execute("select * from t limit ?", new Object[]{Paging.PAGE_SIZE + 10000});
        assertThat(response.rowCount()).isEqualTo(1L);
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
        execute("insert into t (x) values (?)", bulkArgs);
        execute("refresh table t");

        long limit = docCount - 1;
        execute("select * from t limit ?", new Object[]{limit});
        assertThat(response.rowCount()).isEqualTo(limit);

        // test if all is fine if we limit more than we have
        limit += 10;
        execute("select * from t limit ?", new Object[]{limit});
        assertThat(response.rowCount()).isEqualTo((long) docCount);

        // test with sorting
        execute("select * from t order by x limit ?", new Object[]{limit});
        assertThat(printedTable(response.rows())).isEqualTo("0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n21\n");
        assertThat(response.rowCount()).isEqualTo((long) docCount);
    }
}
