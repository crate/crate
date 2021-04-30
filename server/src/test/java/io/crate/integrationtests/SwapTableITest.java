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

import org.junit.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SwapTableITest extends SQLIntegrationTestCase {

    @Test
    public void testSwapTwoTablesWithDropSource() {
        execute("create table t1 (x int)");
        execute("create table t2 (x double)");

        execute("insert into t1 (x) values (1)");
        execute("insert into t2 (x) values (2)");
        execute("refresh table t1, t2");

        execute("alter cluster swap table t1 to t2 with (drop_source = ?)", $(true));
        execute("select * from t2");
        assertThat(
            printedTable(response.rows()),
            is("1\n")
        );

        assertThat(
            printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('t1', 't2') order by 1").rows()),
            is("t2\n")
        );

        assertThrows(() ->  execute("select * from t1"),
                     isSQLError(containsString("Relation 't1' unknown"),
                                UNDEFINED_TABLE,
                                NOT_FOUND,
                                4041));
    }

    @Test
    public void testSwapPartitionedTableWithNonPartitioned() {
        execute("create table t1 (x int)");
        execute("create table t2 (p int) partitioned by (p) clustered into 1 shards with (number_of_replicas = 0)");

        execute("insert into t1 (x) values (1)");
        execute("insert into t2 (p) values (2)");
        execute("refresh table t1, t2");

        execute("alter cluster swap table t1 to t2");
        assertThat(
            printedTable(execute("select * from t1").rows()),
            is("2\n")
        );
        assertThat(
            printedTable(execute("select * from t2").rows()),
            is("1\n")
        );
        assertThat(
            printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('t1', 't2') order by 1").rows()),
            is("t1\n" +
               "t2\n")
        );
    }

    @Test
    public void testSwapTwoPartitionedTablesWhereOneIsEmpty() throws Exception {
        execute("create table t1 (p int) partitioned by (p) clustered into 1 shards with (number_of_replicas = 0)");
        execute("create table t2 (p int) partitioned by (p) clustered into 1 shards with (number_of_replicas = 0)");

        execute("insert into t1 (p) values (1), (2)");

        execute("alter cluster swap table t1 to t2");
        execute("select * from t1");
        assertThat(response.rowCount(), is(0L));

        execute("select * from t2 order by p");
        assertThat(
            printedTable(response.rows()),
            is("1\n" +
               "2\n")
        );
        assertThat(
            printedTable(execute(
                "select table_name from information_schema.tables where table_name in ('t1', 't2') order by 1").rows()),
            is("t1\n" +
               "t2\n")
        );
    }
}
