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

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;

@IntegTestCase.ClusterScope(numDataNodes = 2)
public class PartitionedTableConcurrentIntegrationTest extends IntegTestCase {

    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(10, TimeUnit.SECONDS);

    @After
    public void resetSettings() throws Exception {
        execute("RESET GLOBAL cluster.routing.rebalance.enable");
    }

    /**
     * Test depends on 2 data nodes
     */
    @Test
    public void testSelectWhileShardsAreRelocating() throws Throwable {
        // Automatic rebalancing would disturb our manual allocation and could lead to test failures as reallocation
        // may be issued/run concurrently (by the test and by the cluster itself).
        execute("SET GLOBAL cluster.routing.rebalance.enable = 'none'");

        execute("create table t (name string, p string) " +
                "clustered into 2 shards " +
                "partitioned by (p) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (name, p) values (?, ?)", new Object[][]{
            new Object[]{"Marvin", "a"},
            new Object[]{"Trillian", "a"},
        });
        execute("refresh table t");

        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();
        final CountDownLatch selects = new CountDownLatch(100);

        Thread t = new Thread(() -> {
            while (selects.getCount() > 0) {
                try {
                    execute("select * from t");
                } catch (Throwable t1) {
                    // The failed job should have three started operations
                    SQLResponse res = execute("select id from sys.jobs_log where error is not null order by started desc limit 1");
                    if (res.rowCount() > 0) {
                        String id = (String) res.rows()[0][0];
                        res = execute("select count(*) from sys.operations_log where name=? or name = ? and job_id = ?", new Object[]{"collect", "fetchContext", id});
                        if ((long) res.rows()[0][0] < 3) {
                            // set the error if there where less than three attempts
                            lastThrowable.set(t1);
                        }
                    }
                } finally {
                    selects.countDown();
                }
            }
        });
        t.start();

        PartitionName partitionName = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "t"),
            Collections.singletonList("a"));
        final String indexName = partitionName.asIndexName();

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        DiscoveryNodes nodes = clusterService.state().nodes();
        List<String> nodeIds = new ArrayList<>(2);
        for (DiscoveryNode node : nodes) {
            if (node.isDataNode()) {
                nodeIds.add(node.getId());
            }
        }
        final Map<String, String> nodeSwap = new HashMap<>(2);
        nodeSwap.put(nodeIds.get(0), nodeIds.get(1));
        nodeSwap.put(nodeIds.get(1), nodeIds.get(0));

        final CountDownLatch relocations = new CountDownLatch(20);

        Thread relocatingThread = new Thread(() -> {
            while (relocations.getCount() > 0) {
                ClusterStateResponse clusterStateResponse = FutureUtils.get(admin().cluster().state(new ClusterStateRequest().indices(indexName)));
                List<ShardRouting> shardRoutings = clusterStateResponse.getState().routingTable().allShards(indexName);

                int numMoves = 0;
                for (ShardRouting shardRouting : shardRoutings) {
                    if (shardRouting.currentNodeId() == null) {
                        continue;
                    }
                    if (shardRouting.state() != ShardRoutingState.STARTED) {
                        continue;
                    }
                    String toNode = nodeSwap.get(shardRouting.currentNodeId());

                    assert new IndexParts(shardRouting.getIndexName()).getTable().equals("t") : "Must use index of `t` table";
                    execute(
                        "alter table t PARTITION (p = 'a') REROUTE MOVE SHARD ? FROM ? TO ?",
                        new Object[] { shardRouting.shardId().id(), shardRouting.currentNodeId(), toNode }
                    );
                    numMoves++;
                }

                if (numMoves > 0) {
                    FutureUtils.get(client().admin().cluster().health(new ClusterHealthRequest()
                        .waitForEvents(Priority.LANGUID)
                        .waitForNoRelocatingShards(false)
                        .timeout(ACCEPTABLE_RELOCATION_TIME)
                    ));
                    relocations.countDown();
                }
            }
        });
        relocatingThread.start();
        relocations.await(SQLTransportExecutor.REQUEST_TIMEOUT.getSeconds() + 1, TimeUnit.SECONDS);
        selects.await(SQLTransportExecutor.REQUEST_TIMEOUT.getSeconds() + 1, TimeUnit.SECONDS);

        Throwable throwable = lastThrowable.get();
        if (throwable != null) {
            throw throwable;
        }

        t.join();
        relocatingThread.join();
    }

    private Bucket deletePartitionsAndExecutePlan(String stmt) throws Exception {
        execute("create table t (name string, p string) partitioned by (p)");
        execute("insert into t (name, p) values ('Arthur', 'a'), ('Trillian', 't')");
        ensureYellow();

        PlanForNode plan = plan(stmt);
        execute("delete from t");
        return new CollectionBucket(execute(plan).getResult());
    }

    @Test
    public void testExecuteDeleteAllPartitions_PartitionsAreDeletedMeanwhile() throws Exception {
        Bucket bucket = deletePartitionsAndExecutePlan("delete from t");
        assertThat(bucket.size(), is(1));
        Row row = bucket.iterator().next();
        assertThat(row.numColumns(), is(1));
        assertThat(row.get(0), is(-1L));
    }

    @Test
    public void testExecuteDeleteSomePartitions_PartitionsAreDeletedMeanwhile() throws Exception {
        Bucket bucket = deletePartitionsAndExecutePlan("delete from t where name = 'Trillian'");
        assertThat(bucket.size(), is(1));
        Row row = bucket.iterator().next();
        assertThat(row.numColumns(), is(1));
        assertThat(row.get(0), is(0L));
    }

    @Test
    public void testExecuteDeleteByQuery_PartitionsAreDeletedMeanwhile() throws Exception {
        Bucket bucket = deletePartitionsAndExecutePlan("delete from t where p = 'a'");
        assertThat(bucket.size(), is(1));
        Row row = bucket.iterator().next();
        assertThat(row.numColumns(), is(1));
        assertThat(row.get(0), is(-1L));
    }

    @Test
    public void testExecuteUpdate_PartitionsAreDeletedMeanwhile() throws Exception {
        Bucket bucket = deletePartitionsAndExecutePlan("update t set name = 'BoyceCodd'");
        assertThat(bucket.size(), is(1));
        Row row = bucket.iterator().next();
        assertThat(row.numColumns(), is(1));
        assertThat(row.get(0), is(0L));
    }

    @Test
    public void testTableUnknownExceptionIsNotRaisedIfPartitionsAreDeletedAfterPlan() throws Exception {
        Bucket bucket = deletePartitionsAndExecutePlan("select * from t");
        assertThat(bucket.size(), is(0));
    }

    @Test
    public void testTableUnknownExceptionIsNotRaisedIfPartitionsAreDeletedAfterPlanSingleNode() throws Exception {
        // with a sinlge node, this test leads to empty shard collectors
        internalCluster().ensureAtMostNumDataNodes(1);
        Bucket bucket = deletePartitionsAndExecutePlan("select * from t");
        assertThat(bucket.size(), is(0));
    }

    @Test
    public void testTableUnknownExceptionNotRaisedIfPartitionsDeletedAfterCountPlan() throws Exception {
        Bucket bucket = deletePartitionsAndExecutePlan("select count(*) from t");
        assertThat(bucket.iterator().next().get(0), is(0L));
    }

    @Test
    public void testRefreshDuringPartitionDeletion() throws Exception {
        execute("create table t (name string, p string) partitioned by (p)");
        execute("insert into t (name, p) values ('Arthur', 'a'), ('Trillian', 't')");
        execute("refresh table t");

        PlanForNode plan = plan("refresh table t"); // create a plan in which the partitions exist
        execute("delete from t");

        // shouldn't throw an exception:
        execute(plan).getResult(); // execute now that the partitions are gone
    }

    @Test
    public void testOptimizeDuringPartitionDeletion() throws Exception {
        execute("create table t (name string, p string) partitioned by (p)");
        execute("insert into t (name, p) values ('Arthur', 'a'), ('Trillian', 't')");
        execute("refresh table t");

        PlanForNode plan = plan("optimize table t"); // create a plan in which the partitions exist
        execute("delete from t");

        // shouldn't throw an exception:
        execute(plan).getResult(); // execute now that the partitions are gone
    }

    private void deletePartitionWhileInsertingData(final boolean useBulk) throws Exception {
        execute("create table parted (id int, name string) " +
                "clustered into 1 shards " +
                "partitioned by (id) " +
                "with (number_of_replicas = 0)");

        int numberOfDocs = 100;
        final Object[][] bulkArgs = new Object[numberOfDocs][];
        for (int i = 0; i < numberOfDocs; i++) {
            bulkArgs[i] = new Object[]{i % 2, randomAsciiLettersOfLength(10)};
        }

        // partition to delete
        final int idToDelete = 1;

        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final CountDownLatch insertLatch = new CountDownLatch(1);
        final String insertStmt = "insert into parted (id, name) values (?, ?)";
        Thread insertThread = new Thread(() -> {
            try {
                if (useBulk) {
                    execute(insertStmt, bulkArgs);
                } else {
                    for (Object[] args : bulkArgs) {
                        execute(insertStmt, args);
                    }
                }
            } catch (Exception t) {
                exceptionRef.set(t);
            } finally {
                insertLatch.countDown();
            }
        });

        final CountDownLatch deleteLatch = new CountDownLatch(1);
        final String partitionName = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "parted"),
            Collections.singletonList(String.valueOf(idToDelete))
        ).asIndexName();
        final Object[] deleteArgs = new Object[]{idToDelete};
        Thread deleteThread = new Thread(() -> {
            boolean deleted = false;
            while (!deleted) {
                try {
                    Metadata metadata = client().admin().cluster().state(new ClusterStateRequest()).get()
                        .getState().metadata();
                    if (metadata.indices().get(partitionName) != null) {
                        execute("delete from parted where id = ?", deleteArgs);
                        deleted = true;
                    }
                } catch (Throwable t) {
                    // ignore (mostly partition index does not exists yet)
                }
            }
            deleteLatch.countDown();
        });

        insertThread.start();
        deleteThread.start();
        deleteLatch.await(SQLTransportExecutor.REQUEST_TIMEOUT.getSeconds() + 1, TimeUnit.SECONDS);
        insertLatch.await(SQLTransportExecutor.REQUEST_TIMEOUT.getSeconds() + 1, TimeUnit.SECONDS);

        Exception exception = exceptionRef.get();
        if (exception != null) {
            throw exception;
        }

        insertThread.join();
        deleteThread.join();
    }

    @Test
    public void testDeletePartitionWhileInsertingData() throws Exception {
        deletePartitionWhileInsertingData(false);
    }

    @Test
    public void testDeletePartitionWhileBulkInsertingData() throws Exception {
        deletePartitionWhileInsertingData(true);
    }

    private static Map createColumnMap(int numCols, String prefix) {
        Map<String, Object> map = new HashMap<>(numCols);
        for (int i = 0; i < numCols; i++) {
            map.put(String.format("%s_col_%d", prefix, i), i);
        }
        return map;
    }


    @Test
    public void testInsertIntoDynamicObjectColumnAddsAllColumnsToTemplate() throws Exception {
        // regression test for issue that caused columns not being added to metadata/tableinfo of partitioned table
        // when inserting a lot of new dynamic columns to various partitions of a table
        execute("create table dyn_parted (id int, bucket string, data object(dynamic), primary key (id, bucket)) " +
                "partitioned by (bucket) " +
                "with (number_of_replicas = 0)");
        ensureYellow();

        int bulkSize = 10;
        int numCols = 5;
        String[] buckets = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        final CountDownLatch countDownLatch = new CountDownLatch(buckets.length);
        AtomicInteger numSuccessfulInserts = new AtomicInteger(0);
        for (String bucket : buckets) {
            Object[][] bulkArgs = new Object[bulkSize][];
            for (int i = 0; i < bulkSize; i++) {
                bulkArgs[i] = new Object[]{i, bucket, createColumnMap(numCols, bucket)};
            }
            new Thread(() -> {
                try {
                    execute("insert into dyn_parted (id, bucket, data) values (?, ?, ?)", bulkArgs, TimeValue.timeValueSeconds(10));
                    numSuccessfulInserts.incrementAndGet();
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
        }

        countDownLatch.await();
        // on a reasonable fast machine all inserts always work.
        assertThat("At least one insert must work without timeout", numSuccessfulInserts.get(), Matchers.greaterThanOrEqualTo(1));

        // table info is maybe not up-to-date immediately as doc table info's are cached
        // and invalidated/rebuild on cluster state changes
        assertBusy(() -> {
            execute("select count(*) from information_schema.columns where table_name = 'dyn_parted'");
            assertThat(response.rows()[0][0], is(3L + numCols * numSuccessfulInserts.get()));
        }, 10L, TimeUnit.SECONDS);
    }

    @Test
    public void testConcurrentPartitionCreationWithColumnCreationTypeMissMatch() throws Exception {
        // dynamic column creation must result in a consistent type across partitions
        execute("create table t1 (p int) " +
                "clustered into 1 shards " +
                "partitioned by (p) " +
                "with (number_of_replicas = 0, column_policy = 'dynamic')");

        Thread insertNumbers = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    execute("insert into t1 (p, x) values (?, ?)", $(i, i));
                } catch (Throwable e) {
                    // may fail if other thread is faster
                }
            }
        });
        Thread insertStrings = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    execute("insert into t1 (p, x) values (?, ?)", $(i, "foo" + i));
                } catch (Throwable e) {
                    // may fail if other thread is faster
                }
            }
        });

        insertNumbers.start();
        insertStrings.start();

        insertNumbers.join();
        insertStrings.join();

        // this query should never fail
        execute("select * from t1 order by x limit 50");
    }
}
