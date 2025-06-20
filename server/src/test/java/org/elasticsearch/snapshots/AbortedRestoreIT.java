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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseRandomizedSchema;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class AbortedRestoreIT extends AbstractSnapshotIntegTestCase {

    @Test
    @UseRandomizedSchema(random = false)
    public void testAbortedRestoreAlsoAbortFileRestores() throws Exception {
        String masterNode = cluster().startMasterOnlyNode();
        final String dataNode = cluster().startDataOnlyNode();

        execute("create table tbl (x int) clustered into 1 shards with (number_of_replicas = 0)");
        Object[][] rows = new Object[scaledRandomIntBetween(10, 500)][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[] { randomInt() };
        }
        execute("insert into tbl (x) values (?)", rows);
        ensureGreen();
        execute("optimize table tbl with (max_num_segments = 1)");

        String repositoryName = "repo";
        createRepo(repositoryName, "mock");

        execute("create snapshot repo.snap1 ALL with (wait_for_completion = true)");
        execute("drop table tbl");

        logger.info("--> blocking all data nodes for repository [{}]", repositoryName);
        blockAllDataNodes(repositoryName);
        mockRepo(repositoryName, masterNode).setFailReadsAfterUnblock(true);

        logger.info("--> starting restore");
        CompletableFuture<SQLResponse> future = sqlExecutor.execute(
            "restore snapshot repo.snap1 TABLE tbl with (wait_for_completion = true)",
            new Object[0][]
        );

        RelationName relationName = new RelationName("doc", "tbl");

        assertBusy(() -> {
            ClusterService clusterService = cluster().getInstance(ClusterService.class);
            String indexUUID = clusterService().state().metadata().getIndex(relationName, List.of(), false, IndexMetadata::getIndexUUID);
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexUUID);
            assertThat(indexMetadata).isNotNull();
            var index = new Index(relationName.indexNameOrAlias(), indexMetadata.getIndexUUID());
            var indicesService = cluster().getInstance(IndicesService.class, dataNode);
            var indexService = indicesService.indexService(index);
            assertThat(indexService).isNotNull();
            var shardA = indexService.getShardOrNull(0);
            assertThat(shardA).isNotNull();
            assertThat(shardA.recoveryState()).isNotNull();
            assertThat(shardA.recoveryState().getRecoverySource().getType()).isEqualTo(RecoverySource.Type.SNAPSHOT);
            assertThat(shardA.recoveryState().getStage()).isEqualTo(RecoveryState.Stage.INDEX);
        });

        var snapshotExecutor = (EsThreadPoolExecutor) threadPool(dataNode).executor(ThreadPool.Names.SNAPSHOT);
        assertThat(snapshotExecutor.getMaximumPoolSize()).isGreaterThan(0);

        logger.info("--> waiting for snapshot thread [max={}] pool to be full", snapshotExecutor.getMaximumPoolSize());
        waitForMaxActiveSnapshotThreads(dataNode, snapshotExecutor.getMaximumPoolSize());

        logger.info("--> aborting restore by deleting the index");
        execute("drop table tbl");

        logger.info("--> unblocking repository [{}]", repositoryName);
        unblockAllDataNodes(repositoryName);

        future.get();

        logger.info("--> waiting for snapshot thread pool to be empty");
        waitForMaxActiveSnapshotThreads(dataNode, 0);
    }

    private static void waitForMaxActiveSnapshotThreads(final String node, int expectedCount) throws Exception {
        assertBusy(() -> assertThat(threadPoolStats(node, ThreadPool.Names.SNAPSHOT).active()).isEqualTo(expectedCount), 30L, TimeUnit.SECONDS);
    }

    private static ThreadPool threadPool(final String node) {
        return cluster().getInstance(ClusterService.class, node).getClusterApplierService().threadPool();
    }

    private static ThreadPoolStats.Stats threadPoolStats(final String node, final String threadPoolName) {
        ThreadPool threadPool = threadPool(node);
        ThreadPoolStats.Stats stats = threadPool.stats(threadPoolName);
        if (stats == null) {
            throw new AssertionError("Failed to find thread pool " + threadPoolName);
        }
        return stats;
    }
}

