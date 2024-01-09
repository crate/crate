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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;

@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class TransportSQLActionSingleNodeTest extends IntegTestCase {

    @Test
    public void testUnassignedShards() throws Exception {
        int numReplicas = 3;
        int numShards = 5;
        long expectedUnassignedShards = numShards * numReplicas; // calculation is correct for cluster.numDataNodes = 1

        execute("create table locations (id integer primary key, name string) " +
                "clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas +
                ", \"write.wait_for_active_shards\"=1)");
        ensureYellow();

        execute("select count(*) from sys.shards where table_name = 'locations' and state = 'UNASSIGNED'");
        assertThat(response).hasColumns("count(*)");
        assertThat(response).hasRows(new Object[] { expectedUnassignedShards });
    }

    @Test
    public void testPrimarySecondaryUnassignedShards() throws Exception {
        int numReplicas = 3;
        int numShards = 5;

        execute("create table locations (id integer primary key, name string) " +
                "clustered into " + numShards +
                " shards with(number_of_replicas=" + numReplicas +
                ", \"write.wait_for_active_shards\"=1)");
        ensureYellow();

        execute("select \"primary\", state, count(*) from sys.shards where table_name = 'locations' group by \"primary\", state order by \"primary\"");
        assertThat(response).hasRowCount(2L);
        assertThat(response.rows()[0][0]).isEqualTo(false);
        assertThat(response.rows()[0][1]).isEqualTo("UNASSIGNED");
        assertThat(response.rows()[0][2]).isEqualTo(15L);
        assertThat(response.rows()[1][0]).isEqualTo(true);
        assertThat(response.rows()[1][1]).isEqualTo("STARTED");
        assertThat(response.rows()[1][2]).isEqualTo(5L);
    }

    @Test
    public void testPrimarySecondaryUnassignedShardsWithPartitions() throws Exception {
        int numReplicas = 1;
        int numShards = 5;

        execute("create table locations (id integer primary key, name string) " +
                "partitioned by (id) clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas +
                ", \"write.wait_for_active_shards\"=1)");
        execute("insert into locations (id, name) values (1, 'name1')");
        execute("insert into locations (id, name) values (2, 'name2')");
        refresh();
        ensureYellow();

        execute("select \"primary\", state, count(*) from sys.shards where table_name = 'locations' group by \"primary\", state order by \"primary\"");
        assertThat(response).hasRowCount(2L);
        assertThat(response.rows()[0][0]).isEqualTo(false);
        assertThat(response.rows()[0][1]).isEqualTo("UNASSIGNED");
        assertThat(response.rows()[0][2]).isEqualTo(10L);
        assertThat(response.rows()[1][0]).isEqualTo(true);
        assertThat(response.rows()[1][1]).isEqualTo("STARTED");
        assertThat(response.rows()[1][2]).isEqualTo(10L);
    }

    @Test
    public void testPartitionIdentPrimarySecondaryUnassignedShards() throws Exception {
        int numReplicas = 1;
        int numShards = 5;

        execute("create table locations (id integer primary key, name string) " +
                "partitioned by (id) clustered into " + numShards + " shards with(number_of_replicas=" + numReplicas +
                ", \"write.wait_for_active_shards\"=1)");
        execute("insert into locations (id, name) values (1, 'name1')");
        execute("insert into locations (id, name) values (2, 'name2')");
        refresh();
        ensureYellow();

        execute("select table_name, partition_ident, state from sys.shards where table_name = 'locations' " +
                "group by table_name, partition_ident, state order by partition_ident, state");
        assertThat(response).hasRowCount(4L);

        String expected = "locations| 04132| STARTED\n" +
                          "locations| 04132| UNASSIGNED\n" +
                          "locations| 04134| STARTED\n" +
                          "locations| 04134| UNASSIGNED\n";

        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(expected);
    }

    @Test
    public void testSelectNoResultWithTypes() throws Exception {
        assertResponseWithTypes("select * from sys.nodes where 1=0");
    }

    @Test
    public void testInsertBulkWithTypes() throws Exception {
        execute("create table bla1 (id integer primary key, name string) " +
                "clustered into 2 shards with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into bla1 (id, name) values (?, ?)",
            new Object[][]{
                new Object[]{1, "Ford"},
                new Object[]{2, "Trillian"}
            });
    }

    @Test
    public void testInsertBulkDifferentTypesResultsInRemoteFailure() throws Exception {
        execute("create table foo (value integer) with (number_of_replicas=0, column_policy = 'dynamic')");
        long[] rowCounts = execute("insert into foo (bar) values (?)",
            new Object[][] {
                new Object[] {Map.of("foo", 127)},
                new Object[] {1},
            });
        // One is inserted, the other fails because of a cast error
        assertThat(rowCounts[0] + rowCounts[1]).isEqualTo(-1L);
    }

    @Test
    public void testInsertDynamicNullArrayInBulk() throws Exception {
        execute("create table foo (value integer) with (number_of_replicas=0, column_policy = 'dynamic')");
        var listWithNull = new ArrayList<>(1);
        listWithNull.add(null);
        long[] rowCounts = execute("insert into foo (bar) values (?)",
            new Object[][]{
                new Object[]{listWithNull},
                new Object[]{List.of(1, 2)},
            });
        assertThat(rowCounts[0]).isEqualTo(1L);
        assertThat(rowCounts[1]).isEqualTo(1L);

        execute("select data_type from information_schema.columns where table_name = 'foo' and column_name = 'bar'");
        // integer values for unknown columns will be result in a long type for range safety
        assertThat(response.rows()[0][0]).isEqualTo("bigint_array");
    }

    @Test
    public void testSelectUnknownNoResultWithTypes() throws Exception {
        execute("create table unknown (id integer primary key, name object(ignored)) " +
                "clustered into 2 shards with (number_of_replicas=0)");
        ensureYellow();

        assertResponseWithTypes("select name['bla'] from unknown where 1=0");
    }

    @Test
    public void testDMLStatementsWithTypes() throws Exception {
        execute("create table bla (id integer primary key, name string) " +
                "clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        assertResponseWithTypes("insert into bla (id, name) (select 4, 'Trillian' from sys.cluster)");
        assertResponseWithTypes("insert into bla (id, name) values (1, 'Ford')");
        assertResponseWithTypes("update bla set name='Arthur' where name ='Ford'");
        assertResponseWithTypes("update bla set name='Arthur' where id=1");
        assertResponseWithTypes("delete from bla where id=1");
        assertResponseWithTypes("delete from bla where name='Ford'");

    }

    private void assertResponseWithTypes(String stmt) {
        SQLResponse sqlResponse = execute(stmt);
        assertThat(sqlResponse.columnTypes()).isNotNull();
        assertThat(sqlResponse.columnTypes().length).isEqualTo(sqlResponse.cols().length);
    }

    @Test
    public void testSubscriptArray() throws Exception {
        execute("create table test (id integer primary key, names array(string)) " +
                "clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (id, names) values (?, ?)",
            new Object[]{1, Arrays.asList("Arthur", "Ford")});
        refresh();
        execute("select names[1] from test");
        assertThat(response).hasRowCount(1L);
        assertThat(response.rows()[0][0]).isEqualTo("Arthur");
    }

    @Test
    public void testSubscriptArrayNesting() throws Exception {
        execute("create table test (id integer primary key, names array(object as (surname string))) " +
                "clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (id, names) values (?, ?)",
                new Object[] {1, Arrays.asList(Map.of("surname", "Adams"))});
        refresh();
        execute("select names[1]['surname'] from test");
        assertThat(response).hasRowCount(1L);
        assertThat(response.rows()[0][0]).isEqualTo("Adams");
    }

    @Test
    public void testSelectRegexpMatchesGroup() throws Exception {
        execute("create table test (id integer primary key, text string) " +
                "clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (id, text) values (1, 'Time is an illusion')");
        refresh();
        execute("select regexp_matches(text, '(\\w+)\\s(\\w+)')[1] as first_word," +
                "regexp_matches(text, '(\\w+)\\s(\\w+)')[2] as matched from test order by first_word");
        assertThat(response).hasRowCount(1L);
        assertThat(response.rows()[0][0]).isEqualTo("Time");
        assertThat(response.rows()[0][1]).isEqualTo("is");
    }


    @Test
    public void testKilledAllLog() throws Exception {
        execute("create table likes (" +
                "   event_id string," +
                "   item_id string" +
                ") clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("SET GLOBAL stats.enabled = true");

        final String stmt = "insert into likes (event_id, item_id) values (?, ?)";
        final Object[][] bulkArgs = new Object[100][];
        for (int i = 0; i < bulkArgs.length; i++) {
            bulkArgs[i] = new Object[]{"event1", "item1"};
        }
        final CompletableFuture<long[]> res = new CompletableFuture<>();
        Thread insertThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    res.complete(execute(stmt, bulkArgs));
                } catch (Exception e) {
                    // that's what we want
                    res.completeExceptionally(e);
                }
            }
        });
        insertThread.start();
        Thread.sleep(50);
        execute("kill all");
        insertThread.join();
        try {
            res.get();
        } catch (ExecutionException e) {
            // in this case the job was successfully killed, so it must occur as killed the jobs log
            SQLResponse response = execute("select * from sys.jobs_log where error = ? and stmt = ?", new Object[]{"Job killed", stmt});
            if (response.rowCount() < 1L) {
                assertThat(execute("select * from sys.jobs_log").rows()).isEmpty();
            }
        }
        waitUntilShardOperationsFinished();
    }

    @Test
    public void test_unnest_with_single_nested_array() throws Exception {
        execute("select x from unnest([[1, 2, 3], [4]]) as t (x)");
        assertThat(response).hasRows(
            "1",
            "2",
            "3",
            "4"
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        execute("reset GLOBAL stats.enabled");
    }
}
