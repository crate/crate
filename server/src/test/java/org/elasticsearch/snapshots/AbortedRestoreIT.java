/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import io.crate.testing.UseRandomizedSchema;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.hamcrest.Matcher;
import org.junit.Assert;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static io.crate.testing.Asserts.assertThat;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
@UseRandomizedSchema(random = false)
public class AbortedRestoreIT extends AbstractSnapshotIntegTestCase {

    public void testAbortedRestoreAlsoAbortFileRestores() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        String indexName = "testAbortRestore";
        String tableName = "doc." + indexName;
        execute("create table " + tableName + "(x text) " +
            "clustered into 1 shards " +
            "with (column_policy='strict', number_of_replicas = 0)");


        int docs = scaledRandomIntBetween(10, 1_000);
        for (int i =0 ; i < docs; i++) {
            execute("insert into " + tableName + "(x) values (?)", new Object[]{randomAlphaOfLengthBetween(10, 20)});
        }

        ensureGreen();
        execute("optimize table " + tableName + " with (max_num_segments=1)");

        final String repositoryName = "repository";
        createRepository(repositoryName, "mock");

        final String snapshotName = "snapshot";
        execute(String.format(Locale.ENGLISH, "create snapshot %s.%s ALL WITH (wait_for_completion=true)", repositoryName, snapshotName));
        waitNoPendingTasksOnAll();
        execute("drop table " + tableName);
        waitNoPendingTasksOnAll();

        logger.info("--> blocking all data nodes for repository [{}]", repositoryName);
        blockAllDataNodes(repositoryName);
        failReadsAllDataNodes(repositoryName);

        logger.info("--> starting restore");
        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(repositoryName, snapshotName).waitForCompletion(true).indices(indexName);
        final CompletableFuture<RestoreSnapshotResponse> future = client().admin().cluster().execute(RestoreSnapshotAction.INSTANCE, restoreSnapshotRequest);
        assertBusy(() -> {
            final RecoveryResponse recoveries = client().execute(RecoveryAction.INSTANCE,
                new RecoveryRequest(IndicesOptions.LENIENT_EXPAND_OPEN, true, indexName)).get();
          //  RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(indexName)).get();

            assertThat(recoveries.hasRecoveries()).isTrue();
            final List<RecoveryState> shardRecoveries = recoveries.shardRecoveryStates().get(indexName);
            assertThat(shardRecoveries).hasSize(1);
            assertThat(future.isDone()).isFalse();
//            execute(String.format(Locale.ENGLISH,
//                "select state from sys.snapshot_restore where repository = %s AND snapshot = %s", repositoryName, snapshotName));
//            Assert.assertThat(response.rowCount(), is(1L));
//            Assert.assertThat(response.rowCount(), is(1L));
//            assertThat(response.rows()[0][0]).isEqualTo("STARTED"); // not yet done

            for (RecoveryState shardRecovery : shardRecoveries) {
                assertThat(shardRecovery.getRecoverySource().getType()).isEqualTo(RecoverySource.Type.SNAPSHOT);
                assertThat(shardRecovery.getStage()).isEqualTo(RecoveryState.Stage.INDEX);
            }
        });

        final ThreadPool.Info snapshotThreadPoolInfo = threadPool(dataNode).info(ThreadPool.Names.SNAPSHOT);
        assertThat(snapshotThreadPoolInfo.getMax()).isGreaterThan(0);

        logger.info("--> waiting for snapshot thread [max={}] pool to be full", snapshotThreadPoolInfo.getMax());
        waitForMaxActiveSnapshotThreads(dataNode, snapshotThreadPoolInfo.getMax());

        logger.info("--> aborting restore by deleting the index");
        execute("drop table " + tableName);

        logger.info("--> unblocking repository [{}]", repositoryName);
        unblockAllDataNodes(repositoryName);

        logger.info("--> restore should have failed");
        final RestoreSnapshotResponse restoreSnapshotResponse = future.get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards()).isEqualTo(1);
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards()).isEqualTo(0);

//        execute(String.format(Locale.ENGLISH,
//            "select state from sys.snapshot_restore where repository = %s AND snapshot = %s", repositoryName, snapshotName));
//        Assert.assertThat(response.rowCount(), is(1L));
//        assertThat(response.rows()[0][0]).isEqualTo("FAILED");

        logger.info("--> waiting for snapshot thread pool to be empty");
        waitForMaxActiveSnapshotThreads(dataNode, 0);
    }

    private static void waitForMaxActiveSnapshotThreads(final String node, int maxActiveSnapshotThreads) throws Exception {
        assertBusy(() -> assertThat(threadPoolStats(node, ThreadPool.Names.SNAPSHOT).getActive()).isEqualTo(maxActiveSnapshotThreads), 30L, TimeUnit.SECONDS);
    }

    private static ThreadPool threadPool(final String node) {
        return internalCluster().getInstance(ClusterService.class, node).getClusterApplierService().threadPool();
    }

    private static ThreadPoolStats.Stats threadPoolStats(final String node, final String threadPoolName) {
        return StreamSupport.stream(threadPool(node).stats().spliterator(), false)
            .filter(threadPool -> threadPool.getName().equals(threadPoolName))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Failed to find thread pool " + threadPoolName));
    }
}
