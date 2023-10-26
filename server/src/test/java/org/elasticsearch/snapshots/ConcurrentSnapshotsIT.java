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

package org.elasticsearch.snapshots;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.THROWABLE;
import static org.elasticsearch.snapshots.SnapshotsService.MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.junit.Test;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.common.concurrent.CompletableFutures;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.SQLExceptions;
import io.crate.integrationtests.disruption.discovery.AbstractDisruptionTestCase;
import io.crate.testing.UseRandomizedSchema;

@UseRandomizedSchema(random = false)
@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrentSnapshotsIT extends AbstractSnapshotIntegTestCase {

    // Large snapshot pool settings to set up nodes for tests involving multiple repositories that need to have enough
    // threads so that blocking some threads on one repository doesn't block other repositories from doing work
    private static final Settings LARGE_SNAPSHOT_POOL_SETTINGS = Settings.builder()
        .put("thread_pool.snapshot.core", 5)
        .put("thread_pool.snapshot.max", 5)
        .build();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(AbstractDisruptionTestCase.DEFAULT_SETTINGS)
            .build();
    }

    @Test
    public void testLongRunningSnapshotAllowsConcurrentSnapshot() throws Exception {
        cluster().startMasterOnlyNode();
        final String dataNode = cluster().startDataOnlyNode();
        final String repoName = "repo1";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl_slow");

        CompletableFuture<SnapshotInfo> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "slow-snapshot",
            repoName,
            dataNode
        );

        final String dataNode2 = cluster().startDataOnlyNode();
        ensureStableCluster(3);

        createTableWithRecord("tbl_fast", dataNode2, dataNode);

        CompletableFuture<SnapshotInfo> future = cluster().client()
            .execute(
                CreateSnapshotAction.INSTANCE,
                new CreateSnapshotRequest(repoName, "fast-snapshot")
                    .indices("tbl_fast")
                    .waitForCompletion(true)
            ).thenApply(x -> x.getSnapshotInfo());
        assertSuccessful(future);

        assertThat(createSlowFuture.isDone()).isFalse();
        unblockNode(repoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    @Test
    public void testDeletesAreBatched() throws Exception {
        cluster().startMasterOnlyNode();
        final String dataNode = cluster().startDataOnlyNode();
        String repoName = "repo1";
        createRepo(repoName, "mock");
        createTable("tbl");
        ensureGreen();

        final int numSnapshots = randomIntBetween(1, 4);
        List<CompletableFuture<SnapshotInfo>> createSnapshotFutures = new ArrayList<>(numSnapshots);
        List<String> snapshotNames = new ArrayList<>(numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
            final String snapshot = "snap-" + i;
            snapshotNames.add(snapshot);
            createSnapshotFutures.add(startFullSnapshot("repo1", snapshot));
        }
        assertBusy(() -> assertThat(CompletableFutures.allAsList(createSnapshotFutures)).isCompleted());
        for (var f : createSnapshotFutures) {
            assertSuccessful(f);
        }

        createTableWithRecord("tbl_slow");

        CompletableFuture<SnapshotInfo> createSlowFuture = startFullSnapshotBlockedOnDataNode("blocked-snapshot", "repo1", dataNode);

        List<CompletableFuture<AcknowledgedResponse>> deleteFutures = new ArrayList<>(snapshotNames.size());
        Collections.shuffle(snapshotNames, random());
        for (String snapshotName : snapshotNames) {
            deleteFutures.add(startDelete("repo1", snapshotName));
        }
        assertThat(createSlowFuture).isNotDone();

        final long repoGenAfterInitialSnapshots = getRepositoryData(repoName).getGenId();
        assertThat(repoGenAfterInitialSnapshots).isEqualTo(numSnapshots - 1L);
        unblockNode(repoName, dataNode);

        assertSuccessful(createSlowFuture);

        logger.info("--> waiting for batched deletes to finish");
        assertBusy(() -> assertThat(CompletableFutures.allAsList(deleteFutures)).isCompleted());

        logger.info("--> verifying repository state");
        final RepositoryData repositoryDataAfterDeletes = getRepositoryData(repoName);
        // One increment for snapshot, one for all the deletes
        assertThat(repositoryDataAfterDeletes.getGenId()).isEqualTo(repoGenAfterInitialSnapshots + 2);
        assertThat(repositoryDataAfterDeletes.getSnapshotIds()).hasSize(1);
        assertThat(repositoryDataAfterDeletes.getSnapshotIds().iterator().next().getName()).isEqualTo("blocked-snapshot");
    }

    @Test
    public void testBlockedRepoDoesNotBlockOtherRepos() throws Exception {
        final String masterNode = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepo(blockedRepoName, "mock");
        createRepo(otherRepoName, "fs");
        createIndex("foo");
        ensureGreen();
        createTableWithRecord("tbl_slow");

        CompletableFuture<SnapshotInfo> createSlowFuture = startAndBlockFailingFullSnapshot(
            blockedRepoName,
            "blocked-snapshot"
        );
        CompletableFuture<SnapshotInfo> createFuture = startFullSnapshot(otherRepoName, "snap");
        unblockNode(blockedRepoName, masterNode);
        assertSuccessful(createFuture);
        assertBusy(() -> {
           assertThat(createSlowFuture).isCompletedExceptionally();
           assertThatThrownBy(() -> createSlowFuture.join())
               .satisfiesAnyOf(
                    t -> assertThat(t).hasRootCauseExactlyInstanceOf(SnapshotException.class),
                    t -> assertThat(t).hasRootCauseExactlyInstanceOf(IOException.class)
                );
        });
        execute("DROP REPOSITORY \"" + blockedRepoName + "\"");
    }

    @Test
    public void testMultipleReposAreIndependent() throws Exception {
        cluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = cluster().startDataOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepo(blockedRepoName, "mock");
        createRepo(otherRepoName, "fs");
        createTableWithRecord("tbl");

        var createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            blockedRepoName,
            dataNode
        );

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        createNSnapshots(otherRepoName, randomIntBetween(1, 5));

        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    /**
     * Like {@link #testMultipleReposAreIndependent()} but also includes a delete
     **/
    @Test
    public void testMultipleReposAreIndependent2() throws Exception {
        cluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second repository's concurrent operations.
        final String dataNode = cluster().startDataOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepo(blockedRepoName, "mock");
        createRepo(otherRepoName, "fs");
        createTableWithRecord("tbl");

        var createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot", blockedRepoName, dataNode);

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        String[] snapshotNames = createNSnapshots(otherRepoName, randomIntBetween(1, 5));
        startDelete(otherRepoName, snapshotNames).get();

        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    @Test
    public void testMultipleReposAreIndependent3() throws Exception {
        final String masterNode = cluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        cluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepo(blockedRepoName, "mock");
        createRepo(otherRepoName, "fs");
        createTableWithRecord("tbl");

        var createSlowFuture = startFullSnapshot(blockedRepoName, "blocked-snapshot");
        assertSuccessful(createSlowFuture);
        mockRepo(blockedRepoName, masterNode).setBlockOnAnyFiles(true);
        var slowDeleteFuture = startDelete(blockedRepoName, "blocked-snapshot");

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        String[] snapshotNames = createNSnapshots(otherRepoName, randomIntBetween(1, 5));
        startDelete(otherRepoName, snapshotNames).get();

        unblockNode(blockedRepoName, masterNode);
        assertBusy(() -> assertThat(slowDeleteFuture).isCompleted());
    }

    @Test
    public void testSnapshotRunsAfterInProgressDelete() throws Exception {
        final String masterNode = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");

        ensureGreen();
        createTableWithRecord("tbl");

        final String firstSnapshot = "first-snapshot";
        assertSuccessful(startFullSnapshot(repoName, firstSnapshot));

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        var deleteFuture = startDelete(repoName, firstSnapshot);
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(10L));

        var createSnapshotFuture = startFullSnapshot(repoName, "second-snapshot");
        unblockNode(repoName, masterNode);
        assertBusy(() -> {
            assertThat(deleteFuture)
                .failsWithin(0, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withRootCauseExactlyInstanceOf(IOException.class);
        });

        assertSuccessful(createSnapshotFuture);
    }


    @Test
    public void test_swap_table_after_snapshot_blocked_on_data_node() throws Exception {
        cluster().startMasterOnlyNode();
        String dataNode = cluster().startDataOnlyNode();
        String repoName = "r1";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        createTableWithRecord("tbl2");
        CompletableFuture<SnapshotInfo> snapshot = startFullSnapshotBlockedOnDataNode("s1", repoName, dataNode);

        execute("alter cluster swap table tbl1 to tbl2");
        unblockNode(repoName, dataNode);
        assertThat(snapshot).succeedsWithin(5, TimeUnit.SECONDS);
        SnapshotInfo snapshotInfo = snapshot.join();
        // Shards can become temporarily unavailable during a table swap, which can cause snapshots to abort
        if (snapshotInfo.state() == SnapshotState.SUCCESS) {
            execute("drop table tbl1");
            execute("drop table tbl2");
            execute("restore snapshot r1.s1 ALL with (wait_for_completion = true)");
        } else if (snapshotInfo.state() == SnapshotState.FAILED) {
            assertThat(snapshotInfo.reason()).isEqualTo("aborted");
        } else {
            assertThat(snapshotInfo.shardFailures()).isNotEmpty();
            for (SnapshotShardFailure shardFailures : snapshotInfo.shardFailures()) {
                assertThat(shardFailures.reason()).isEqualTo("aborted");
            }
        }

        // can still create new snapshots after
        execute("create snapshot r1.s2 all with (wait_for_completion = true)");
        execute("drop table tbl1");
        execute("drop table tbl2");
        if (randomBoolean()) {
            execute("drop snapshot r1.s1");
        }
        execute("restore snapshot r1.s2 ALL with (wait_for_completion = true)");

        execute("refresh table tbl1");
        assertThat(execute("select count(*) from tbl1")).hasRows(
            "1"
        );
    }

    @Test
    public void test_swap_table_after_snapshot_blocked_on_index_file() throws Exception {
        String masterNode = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        String repoName = "r1";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        createTableWithRecord("tbl2");
        mockRepo(repoName, masterNode).setBlockOnWriteIndexFile();
        CompletableFuture<SnapshotInfo> snapshot = startFullSnapshot(repoName, "s1");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(10));

        execute("alter cluster swap table tbl1 to tbl2");
        unblockNode(repoName, masterNode);
        assertThat(snapshot)
            .succeedsWithin(5, TimeUnit.SECONDS)
            .satisfies(x -> assertThat(x.state()).isEqualTo(SnapshotState.SUCCESS));

        execute("drop table tbl1");
        execute("drop table tbl2");
        execute("restore snapshot r1.s1 ALL with (wait_for_completion = true)");
    }

    @Test
    public void testAbortOneOfMultipleSnapshots() throws Exception {
        String masterNode = cluster().startMasterOnlyNode();
        String dataNode = cluster().startDataOnlyNode();
        String repoName = "test-repo";
        createRepo(repoName, "mock");
        String firstTable = "tbl1";
        createTableWithRecord(firstTable);

        String firstSnapshot = "snapshot-one";
        var firstSnapshotResponse =
            startFullSnapshotBlockedOnDataNode(firstSnapshot, repoName, dataNode);

        String dataNode2 = cluster().startDataOnlyNode();
        ensureStableCluster(3);
        String secondTable = "tbl2";
        createTableWithRecord(secondTable, dataNode2, dataNode);

        String secondSnapshot = "snapshot-two";
        var secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(masterNode, state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.entries().size() == 2 && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        var deleteSnapshotsResponse = startDelete(repoName, firstSnapshot);
        awaitClusterState(masterNode, state -> hasDeletionsInProgress(state, 1));

        logger.info("--> start third snapshot");
        var thirdSnapshotResponse = cluster().client()
            .execute(
                CreateSnapshotAction.INSTANCE,
                new CreateSnapshotRequest(repoName, "snapshot-three")
                    .indices(secondTable)
                    .waitForCompletion(true)
            ).thenApply(x -> x.getSnapshotInfo());

        assertThat(firstSnapshotResponse).isNotDone();
        assertThat(secondSnapshotResponse).isNotDone();

        unblockNode(repoName, dataNode);
        final SnapshotInfo firstSnapshotInfo = firstSnapshotResponse.get();
        assertThat(firstSnapshotInfo.state()).isEqualTo(SnapshotState.FAILED);
        assertThat(firstSnapshotInfo.reason()).isEqualTo("Snapshot was aborted by deletion");

        assertSuccessful(secondSnapshotResponse);
        assertSuccessful(thirdSnapshotResponse);

        assertThat(deleteSnapshotsResponse.get().isAcknowledged()).isTrue();

        logger.info("--> verify that the first snapshot is gone");

        execute("SELECT name FROM sys.snapshots WHERE repository=? ORDER BY name", new Object[]{repoName});
        assertThat(response).hasRows(
            "snapshot-three",
            "snapshot-two"
        );
    }

    @Test
    public void testCascadedAborts() throws Exception {
        cluster().startMasterOnlyNode();
        String dataNode = cluster().startDataOnlyNode();
        String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");

        String firstSnapshot = "snapshot-one";
        var firstSnapshotResponse =
            startFullSnapshotBlockedOnDataNode(firstSnapshot, repoName, dataNode);

        String dataNode2 = cluster().startDataOnlyNode();
        ensureStableCluster(3);
        createTableWithRecord("tbl2", dataNode2, dataNode);

        String secondSnapshot = "snapshot-two";
        var secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(cluster().getMasterName(), state -> {
            var snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.entries().size() == 2
                && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        var deleteSnapshotsResponse = startDelete(repoName, firstSnapshot);
        awaitClusterState(cluster().getMasterName(), state -> hasDeletionsInProgress(state, 1));

        var thirdSnapshotResponse = startFullSnapshot(repoName, "snapshot-three");

        assertThat(firstSnapshotResponse).isNotDone();
        assertThat(secondSnapshotResponse).isNotDone();

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> {
            SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries()).hasSize(3);
        }, 30L, TimeUnit.SECONDS);

        var delOne = startDelete(repoName, "snapshot-one");
        var delTwo = startDelete(repoName, "snapshot-two");
        var delThree = startDelete(repoName, "snapshot-three");

        logger.info("--> waiting for second and third snapshot to finish");
        assertBusy(() -> {
            final SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries()).hasSize(3);
            assertThat(snapshotsInProgress.entries().get(0).state()).isEqualTo(SnapshotsInProgress.State.ABORTED);
        }, 30L, TimeUnit.SECONDS);

        unblockNode(repoName, dataNode);

        logger.info("--> verify all snapshots were aborted");
        assertThat(firstSnapshotResponse.get().state()).isEqualTo(SnapshotState.FAILED);
        assertThat(secondSnapshotResponse.get().state()).isEqualTo(SnapshotState.FAILED);
        assertThat(thirdSnapshotResponse.get().state()).isEqualTo(SnapshotState.FAILED);

        logger.info("--> verify both deletes have completed");
        assertThat(deleteSnapshotsResponse.get().isAcknowledged()).isTrue();
        assertThat(delOne.get().isAcknowledged()).isTrue();
        assertThat(delTwo.get().isAcknowledged()).isTrue();
        assertThat(delThree.get().isAcknowledged()).isTrue();

        logger.info("--> verify that all snapshots are gone");
        execute("SELECT count(*) FROM sys.snapshots WHERE repository=?", new Object[]{repoName});
        assertThat(response.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void testMasterFailOverWithQueuedDeletes() throws Exception {
        cluster().startMasterOnlyNodes(3);
        String dataNode = cluster().startDataOnlyNode();
        String repoName = "test-repo";
        createRepo(repoName, "mock");

        String firstIndex = "index-one";
        createTableWithRecord(firstIndex);

        String firstSnapshot = "snapshot-one";
        mockRepo(repoName, dataNode).blockOnDataFiles(true);
        var firstSnapshotResponse = createSnapshot(cluster().nonMasterClient(), repoName, firstSnapshot, false);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        String dataNode2 = cluster().startDataOnlyNode();
        ensureStableCluster(5);
        String secondIndex = "index-two";
        createTableWithRecord(secondIndex, dataNode2, dataNode);

        String secondSnapshot = "snapshot-two";
        var secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(cluster().getMasterName(), state -> {
            var snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.entries().size() == 2
                && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        var firstDeleteFuture = startDelete(cluster().nonMasterClient(), repoName, firstSnapshot);
        awaitClusterState(cluster().getMasterName(), state -> hasDeletionsInProgress(state, 1));

        mockRepo(repoName, dataNode2).setBlockOnAnyFiles(true);
        var snapshotThreeFuture = createSnapshot(cluster().nonMasterClient(), repoName, "snapshot-three", false);
        waitForBlock(dataNode2, repoName, TimeValue.timeValueSeconds(30L));

        assertThat(firstSnapshotResponse).isNotDone();
        assertThat(secondSnapshotResponse).isNotDone();

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> {
            final SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries()).hasSize(3);
        }, 30L, TimeUnit.SECONDS);

        var delOne = startDelete(cluster().nonMasterClient(), repoName, "snapshot-one");
        var delTwo = startDelete(cluster().nonMasterClient(), repoName, "snapshot-two");
        var delThree = startDelete(cluster().nonMasterClient(), repoName, "snapshot-three");
        logger.info("--> wait for delete to be enqueued in cluster state");
        awaitClusterState(cluster().getMasterName(), state -> {
            SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            return deletionsInProgress.getEntries().size() == 1
                && deletionsInProgress.getEntries().get(0).getSnapshots().size() == 3;
        });

        logger.info("--> waiting for second snapshot to finish and the other two snapshots to become aborted");
        assertBusy(() -> {
            final SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries()).hasSize(2);
            for (SnapshotsInProgress.Entry entry
                : clusterService().state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries()) {
                assertThat(entry.state()).isEqualTo(SnapshotsInProgress.State.ABORTED);
                assertThat(entry.snapshot().getSnapshotId().getName()).isNotEqualTo(secondSnapshot);
            }
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> stopping current master node");
        cluster().stopCurrentMasterNode();

        unblockNode(repoName, dataNode);
        unblockNode(repoName, dataNode2);

        try {
            assertThat(firstDeleteFuture.get().isAcknowledged()).isTrue();
            assertThat(delOne.get().isAcknowledged()).isTrue();
            assertThat(delTwo.get().isAcknowledged()).isTrue();
            assertThat(delThree.get().isAcknowledged()).isTrue();
        } catch (Exception e) {
            Throwable cause = SQLExceptions.unwrap(e);
            if (cause instanceof RepositoryException re) {
                // rarely the master node fails over twice when shutting down the initial master
                // and fails the transport listener.
                assertThat(re.repository()).isEqualTo("_all");
                assertThat(re.getMessage()).endsWith("Failed to update cluster state during repository operation");
            } else if (cause instanceof SnapshotMissingException sme) {
                // very rarely a master node fail-over happens at such a time that the client on the data-node
                // sees a disconnect exception after the master has already started the delete, leading to the delete
                // retry to run into a situation where the snapshot has already been potentially deleted.
                Assertions.assertThat(sme.getSnapshotName()).isEqualTo(firstSnapshot);
            } else {
                throw e;
            }
        }

        assertThat(snapshotThreeFuture).isCompletedExceptionally();

        logger.info("--> verify that all snapshots are gone and no more work is left in the cluster state");
        assertBusy(() -> {
            execute("SELECT count(*) FROM sys.snapshots WHERE repository=?", new Object[]{repoName});
            assertThat(response.rows()[0][0]).isEqualTo(0L);
            ClusterState state = clusterService().state();
            SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries()).isEmpty();
            SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            assertThat(snapshotDeletionsInProgress.getEntries()).isEmpty();
        }, 30L, TimeUnit.SECONDS);
    }

    @Test
    public void testMultiplePartialSnapshotsQueuedAfterDelete() throws Exception {
        final String masterNode = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        createTableWithRecord("tbl2");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        mockRepo(repoName, masterNode).blockOnDataFiles(true);
        var deleteFuture = startDelete(repoName, "*");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));
        var snapshotThree = createSnapshot(cluster().client(), repoName, "snapshot-three", true);
        var snapshotFour = createSnapshot(cluster().client(), repoName, "snapshot-four", true);
        awaitClusterState(masterNode, state -> hasSnapshotsInProgress(state, 2));

        execute("drop table tbl2");
        unblockNode(repoName, masterNode);

        assertThat(snapshotThree.get().state()).isEqualTo(SnapshotState.PARTIAL);
        assertThat(snapshotFour.get().state()).isEqualTo(SnapshotState.PARTIAL);
        assertThat(deleteFuture.get().isAcknowledged()).isTrue();
    }

    @Test
    public void testAbortNotStartedSnapshotWithoutIO() throws Exception {
        String masterNode = cluster().startMasterOnlyNode();
        String dataNode = cluster().startDataOnlyNode();
        String repoName = "repo1";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");

        var createSnapshot1Future = startFullSnapshotBlockedOnDataNode("snapshot1", repoName, dataNode);
        var createSnapshot2Future = startFullSnapshot(repoName, "snapshot2");

        awaitClusterState(masterNode, state -> hasSnapshotsInProgress(state, 2));

        execute("drop snapshot repo1.snapshot2");
        assertThat(createSnapshot2Future)
            .failsWithin(5, TimeUnit.SECONDS)
            .withThrowableOfType(ExecutionException.class)
            .withRootCauseExactlyInstanceOf(SnapshotException.class);

        assertThat(createSnapshot1Future.isDone()).isFalse();
        unblockNode(repoName, dataNode);
        assertSuccessful(createSnapshot1Future);
        assertThat(getRepositoryData(repoName).getGenId()).isEqualTo(0L);
    }

    @Test
    public void testStartWithSuccessfulShardSnapshotPendingFinalization() throws Exception {
        final String masterName = cluster().startMasterOnlyNode();
        final String dataNode = cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");

        createTableWithRecord("tbl1");
        startFullSnapshot(repoName, "first-snapshot").get(5, TimeUnit.SECONDS);

        mockRepo(repoName, masterName).setBlockOnWriteIndexFile();
        var blockedSnapshot = startFullSnapshot(repoName, "snap-blocked");
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));
        awaitClusterState(masterName, state -> hasSnapshotsInProgress(state, 1));

        mockRepo(repoName, dataNode).setBlockOnAnyFiles(true);
        var otherSnapshot = startFullSnapshot(repoName, "other-snapshot");
        awaitClusterState(masterName, state -> hasSnapshotsInProgress(state, 2));
        assertThat(blockedSnapshot.isDone()).isFalse();
        unblockNode(repoName, masterName);
        awaitClusterState(masterName, state -> hasSnapshotsInProgress(state, 1));

        awaitMasterFinishRepoOperations();

        unblockNode(repoName, dataNode);
        assertSuccessful(blockedSnapshot);
        assertSuccessful(otherSnapshot);
    }

    @Test
    public void testAssertMultipleSnapshotsAndPrimaryFailOver() throws Exception {
        cluster().startMasterOnlyNode();
        final String dataNode = cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");

        final String table = "tbl1";
        execute(
            "create table tbl1 (id string primary key, s string) " +
                "clustered into 1 shards with (number_of_replicas = 1)");
        execute("insert into tbl1 (id, s) values ('some_id', 'foo')");
        execute("refresh table tbl1");
        ensureYellow(table);

        mockRepo(repoName, dataNode).blockOnDataFiles(true);
        var firstSnapshotResponse = startFullSnapshotFromMasterClient(repoName, "snapshot-one");
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        cluster().startDataOnlyNode();
        ensureStableCluster(3);
        ensureGreen(table);

        String secondSnapshot = "snapshot-two";
        var secondSnapshotResponse = startFullSnapshotFromMasterClient(repoName, secondSnapshot);

        cluster().restartNode(dataNode, TestCluster.EMPTY_CALLBACK);
        ensureStableCluster(3);
        ensureGreen(table);

        assertThat(firstSnapshotResponse.get().state()).isEqualTo(SnapshotState.PARTIAL);
        assertThat(secondSnapshotResponse.get().state()).isEqualTo(SnapshotState.PARTIAL);
    }

    @Test
    public void testQueuedDeletesWithFailures() throws Exception {
        String masterNode = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);

        var firstDeleteSnapshotFuture = startDelete(cluster().client(), repoName, "*");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        var snapshotFuture = startFullSnapshot(repoName, "snapshot-queued");
        awaitClusterState(masterNode, state -> (hasSnapshotsInProgress(state, 1)));

        var secondDeleteSnapshotFuture = startDelete(
            cluster().client(),
            repoName,
            "*"
        );
        awaitClusterState(masterNode, state -> hasDeletionsInProgress(state, 2));

        unblockNode(repoName, masterNode);
        assertBusy(() -> assertThat(firstDeleteSnapshotFuture).isCompletedExceptionally());

        // Second delete works out cleanly since the repo is unblocked now
        assertThat(secondDeleteSnapshotFuture.get().isAcknowledged()).isTrue();
        // Snapshot should have been aborted
        assertThatThrownBy(snapshotFuture::get)
            .hasRootCauseExactlyInstanceOf(SnapshotException.class)
            .rootCause().hasMessageEndingWith("Snapshot was aborted by deletion");

        assertThat(execute("SELECT count(*) FROM sys.snapshots WHERE repository=?", new Object[]{repoName}))
            .hasRows("0");
        awaitClusterState(
            masterNode,
            state -> hasSnapshotsInProgress(state, 0) && hasDeletionsInProgress(state, 0)
        );
    }

    @Test
    public void testQueuedOperationsOnMasterDisconnectAndRepoFailure() throws Exception {
        cluster().startMasterOnlyNodes(3);
        final String dataNode = cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final String masterNode = cluster().getMasterName();
        final NetworkDisruption networkDisruption = isolateMasterDisruption(new NetworkDisruption.NetworkDisconnect());
        cluster().setDisruptionScheme(networkDisruption);

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final var firstFailedSnapshotFuture =
                startFullSnapshotFromMasterClient(repoName, "failing-snapshot-1");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));
        final var secondFailedSnapshotFuture =
                startFullSnapshotFromMasterClient(repoName, "failing-snapshot-2");
        awaitClusterState(masterNode, state -> hasSnapshotsInProgress(state, 2));

        final var deleteFuture = startDelete(repoName, "*");
        awaitClusterState(masterNode, state -> hasDeletionsInProgress(state, 1));

        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, masterNode);
        networkDisruption.stopDisrupting();

        logger.info("--> make sure all failing requests get a response");
        assertBusy(() -> assertThat(firstFailedSnapshotFuture).isDone());
        assertBusy(() -> assertThat(secondFailedSnapshotFuture).isDone());
        assertBusy(() -> assertThat(deleteFuture).isDone());

        awaitNoMoreRunningOperations();
    }

    @Test
    public void testMultipleSnapshotsQueuedAfterDelete() throws Exception {
        final String masterNode = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        final var deleteFuture = startAndBlockOnDeleteSnapshot(repoName, "*");
        final var snapshotThree = startFullSnapshot(repoName, "snapshot-three");
        final var snapshotFour = startFullSnapshot(repoName, "snapshot-four");

        unblockNode(repoName, masterNode);

        assertSuccessful(snapshotThree);
        assertSuccessful(snapshotFour);
        assertAcked(deleteFuture.get());
    }

    @Test
    public void testQueuedSnapshotsWaitingForShardReady() throws Exception {
        cluster().startMasterOnlyNode();
        cluster().startDataOnlyNodes(2);
        final String repoName = "test-repo";
        createRepo(repoName, "fs");

        final String testTable = "tbl1";
        final int noShards = between(2, 10);
        // Create index on two nodes and make sure each node has a primary by setting no replicas
        execute("CREATE TABLE \"" + testTable + "\"(a int, s string) CLUSTERED BY (a) INTO " + noShards +
                " SHARDS WITH (number_of_replicas=0)");

        ensureGreen(testTable);

        logger.info("--> indexing some data");
        Object[][] data = new Object[100][2];
        for (int i = 0; i < 100; i++) {
            data[i][0] = i;
            data[i][1] = "foo" + i;
        }
        execute("INSERT INTO \"" + testTable + "\"(a, s) VALUES(?, ?)", data);
        execute("REFRESH TABLE \"" + testTable + "\"");
        execute("SELECT COUNT(*) FROM \"" + testTable + "\"");
        assertThat(response).hasRows("100");

        logger.info("--> start relocations");
        allowNodes(testTable, 1);

        logger.info("--> wait for relocations to start");
        assertBusy(() -> {
            var assertion = new SoftAssertions();
            execute("SELECT count(*) FROM sys.shards WHERE table_name=? AND routing_state='RELOCATING'",
                    new Object[]{testTable});
            assertion.assertThat((long) response.rows()[0][0]).isGreaterThan(0L);

            if (assertion.wasSuccess() == false) { // Maybe all shards are already relocated
                execute("SELECT count(*) FROM sys.shards WHERE table_name=? GROUP BY node",
                        new Object[]{testTable});
                assertThat((long) response.rows()[0][0]).isEqualTo(noShards);
            }
        }, 1L, TimeUnit.MINUTES);
        logger.info("--> start two snapshots");
        final String snapshotOne = "snap-1";
        final String snapshotTwo = "snap-2";
        final var snapOneResponse = startFullSnapshot(repoName, snapshotOne, testTable);
        final var snapTwoResponse = startFullSnapshot(repoName, snapshotTwo, testTable);

        awaitNoMoreRunningOperations();
        logger.info("--> wait for snapshot to complete");
        for (SnapshotInfo si : List.of(snapOneResponse.get(), snapTwoResponse.get())) {
            assertThat(si.state()).isEqualTo(SnapshotState.SUCCESS);
            assertThat(si.shardFailures()).isEmpty();
        }
    }

    @Test
    public void testBackToBackQueuedDeletes() throws Exception {
        final String masterName = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        final String[] snapshots = createNSnapshots(repoName, 2);
        final String snapshotOne = snapshots[0];
        final String snapshotTwo = snapshots[1];

        final var deleteSnapshotOne = startAndBlockOnDeleteSnapshot(repoName, snapshotOne);
        final var deleteSnapshotTwo = startDelete(repoName, snapshotTwo);
        awaitClusterState(masterName, state -> (hasDeletionsInProgress(state, 2)));

        unblockNode(repoName, masterName);
        assertAcked(deleteSnapshotOne.get());
        assertAcked(deleteSnapshotTwo.get());

        final RepositoryData repositoryData = getRepositoryData(repoName);
        assertThat(repositoryData.getSnapshotIds()).isEmpty();
        // Two snapshots and two distinct delete operations move us 4 steps from -1 to 3
        assertThat(repositoryData.getGenId()).isEqualTo(3L);
    }

    @Test
    public void testQueuedOperationsAfterFinalizationFailure() throws Exception {
        cluster().startMasterOnlyNodes(3);
        cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");

        final String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        final var snapshotThree = startAndBlockFailingFullSnapshot(repoName, "snap-other");

        final String masterName = cluster().getMasterName();

        final String snapshotOne = snapshotNames[0];
        final var deleteSnapshotOne = startDelete(repoName, snapshotOne);
        awaitClusterState(masterName, state -> (hasDeletionsInProgress(state, 1)));

        unblockNode(repoName, masterName);

        assertBusy(() -> assertThat(snapshotThree).isCompletedExceptionally());
        assertAcked(deleteSnapshotOne.get());
    }

    @Test
    public void testStartDeleteDuringFinalizationCleanup() throws Exception {
        final String masterName = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        createNSnapshots(repoName, randomIntBetween(1, 5));
        final String snapshotName = "snap-name";
        blockMasterFromDeletingIndexNFile(repoName);
        final var snapshotFuture = startFullSnapshot(repoName, snapshotName);
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));
        final var deleteFuture = startDelete(repoName, snapshotName);
        awaitClusterState(masterName, state -> (hasDeletionsInProgress(state, 1)));
        unblockNode(repoName, masterName);
        assertSuccessful(snapshotFuture);
        assertAcked(deleteFuture.get(30L, TimeUnit.SECONDS));
    }

    @Test
    public void testEquivalentDeletesAreDeduplicated() throws Exception {
        final String masterName = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        blockMasterFromDeletingIndexNFile(repoName);
        final int deletes = randomIntBetween(2, 10);
        final List<CompletableFuture<AcknowledgedResponse>> deleteResponses = new ArrayList<>(deletes);
        for (int i = 0; i < deletes; ++i) {
            deleteResponses.add(startDelete(repoName, "*"));
        }
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));
        awaitClusterState(masterName, state -> (hasDeletionsInProgress(state, 1)));
        final AtomicInteger cntNotDone = new AtomicInteger(0);
        deleteResponses.forEach(dr -> cntNotDone.getAndAdd(dr.isDone() ? 0 : 1));
        // Since the deletes are de-duplicated, at least one should be blocked
        assertThat(cntNotDone.get()).isGreaterThanOrEqualTo(1);
        awaitClusterState(masterName, state -> (hasDeletionsInProgress(state, 1)));
        unblockNode(repoName, masterName);
        for (var deleteResponse : deleteResponses) {
            assertAcked(deleteResponse.get());
        }
    }

    @Test
    public void testMasterFailoverOnFinalizationLoop() throws Exception {
        cluster().startMasterOnlyNodes(3);
        final String dataNode = cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");
        final var networkDisruption = isolateMasterDisruption(new NetworkDisruption.NetworkDisconnect());
        cluster().setDisruptionScheme(networkDisruption);

        final String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));
        final String masterName = cluster().getMasterName();
        blockMasterFromDeletingIndexNFile(repoName);
        final var snapshotThree = startFullSnapshotFromMasterClient(repoName, "snap-other");
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));

        final String snapshotOne = snapshotNames[0];
        final var deleteSnapshotOne = startDelete(repoName, snapshotOne);
        awaitClusterState(masterName, state -> (hasDeletionsInProgress(state, 1)));
        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);

        unblockNode(repoName, masterName);
        networkDisruption.stopDisrupting();
        ensureStableCluster(4);

        assertSuccessful(snapshotThree);
        try {
            deleteSnapshotOne.get();
        } catch (Exception e) {
            // ignored
        }
        awaitNoMoreRunningOperations();
    }

    @Test
    public void testStatusMultipleSnapshotsMultipleRepos() throws Exception {
        final String masterNode = cluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked-1";
        final String otherBlockedRepoName = "test-repo-blocked-2";
        createRepo(blockedRepoName, "mock");
        createRepo(otherBlockedRepoName, "mock");
        createTableWithRecord("tbl1");

        final var createSlowFuture1 =
                startFullSnapshotBlockedOnDataNode("blocked-snapshot", blockedRepoName, dataNode);
        final var createSlowFuture2 =
                startFullSnapshotBlockedOnDataNode("blocked-snapshot-2", blockedRepoName, dataNode);
        final var createSlowFuture3 =
                startFullSnapshotBlockedOnDataNode("other-blocked-snapshot", otherBlockedRepoName, dataNode);
        awaitClusterState(masterNode, state -> (hasSnapshotsInProgress(state, 3)));

        assertSnapshotStatusCountOnRepo(blockedRepoName, 2);
        assertSnapshotStatusCountOnRepo(otherBlockedRepoName, 1);

        unblockNode(blockedRepoName, dataNode);
        awaitClusterState(masterNode, state -> (hasSnapshotsInProgress(state, 1)));
        assertSnapshotStatusCountOnRepo(blockedRepoName, 0);
        assertSnapshotStatusCountOnRepo(otherBlockedRepoName, 1);

        unblockNode(otherBlockedRepoName, dataNode);
        assertSuccessful(createSlowFuture1);
        assertSuccessful(createSlowFuture2);
        assertSuccessful(createSlowFuture3);

        awaitNoMoreRunningOperations();
    }

    @Test
    public void testInterleavedAcrossMultipleRepos() throws Exception {
        final String masterNode = cluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked-1";
        final String otherBlockedRepoName = "test-repo-blocked-2";
        createRepo(blockedRepoName, "mock");
        createRepo(otherBlockedRepoName, "mock");
        createTableWithRecord("tbl1");

        final var createSlowFuture1 =
                startFullSnapshotBlockedOnDataNode("blocked-snapshot", blockedRepoName, dataNode);
        final var createSlowFuture2 =
                startFullSnapshotBlockedOnDataNode("blocked-snapshot-2", blockedRepoName, dataNode);
        final var createSlowFuture3 =
                startFullSnapshotBlockedOnDataNode("other-blocked-snapshot", otherBlockedRepoName, dataNode);
        awaitClusterState(masterNode, state -> (hasSnapshotsInProgress(state, 3)));
        unblockNode(blockedRepoName, dataNode);
        unblockNode(otherBlockedRepoName, dataNode);

        assertSuccessful(createSlowFuture1);
        assertSuccessful(createSlowFuture2);
        assertSuccessful(createSlowFuture3);
    }

    @Test
    public void testMasterFailoverAndMultipleQueuedUpSnapshotsAcrossTwoRepos() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        cluster().startMasterOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        final String dataNode = cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final String otherRepoName = "other-test-repo";
        createRepo(repoName, "mock");
        createRepo(otherRepoName, "mock");
        createTableWithRecord("tbl1");
        createNSnapshots(repoName, randomIntBetween(2, 5));
        final int countOtherRepo = randomIntBetween(2, 5);
        createNSnapshots(otherRepoName, countOtherRepo);

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        blockMasterFromFinalizingSnapshotOnIndexFile(otherRepoName);

        startFullSnapshot(repoName, "snapshot-blocked-1");
        startFullSnapshot(repoName, "snapshot-blocked-2");
        startFullSnapshot(otherRepoName, "snapshot-other-blocked-1");
        startFullSnapshot(otherRepoName, "snapshot-other-blocked-2");

        final String masterNode = cluster().getMasterName();
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));
        waitForBlock(masterNode, otherRepoName, TimeValue.timeValueSeconds(30L));
        awaitClusterState(masterNode, state -> (hasSnapshotsInProgress(state, 4)));

        cluster().stopCurrentMasterNode();
        ensureStableCluster(3, dataNode);
        awaitMasterFinishRepoOperations();

        final RepositoryData repositoryData = getRepositoryData(otherRepoName);
        assertThat(repositoryData.getSnapshotIds().size())
                .isGreaterThanOrEqualTo(countOtherRepo)
                .isLessThanOrEqualTo(countOtherRepo + 2);
    }

    @Test
    public void testConcurrentOperationsLimit() throws Exception {
        final String masterNode = cluster().startMasterOnlyNode();
        cluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepo(repoName, "mock");
        createTableWithRecord("tbl1");

        final int limitToTest = randomIntBetween(1, 3);
        // Setting not exposed via SQL
        client().admin().cluster()
                .execute(ClusterUpdateSettingsAction.INSTANCE, new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder()
                                .put(MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING.getKey(), limitToTest)
                )).get();

        final String[] snapshotNames = createNSnapshots(repoName, limitToTest + 1);
        MockRepository mockRepo = mockRepo(repoName, masterNode);
        mockRepo.setBlockOnDeleteIndexFile();
        mockRepo.setBlockOnWriteIndexFile();
        final int[] blockedSnapshots = new int[1];
        boolean blockedDelete = false;
        final List<CompletableFuture<SnapshotInfo>> snapshotFutures = new ArrayList<>();
        CompletableFuture<AcknowledgedResponse> deleteFuture = null;
        for (int i = 0; i < limitToTest; ++i) {
            if (blockedDelete || randomBoolean()) {
                snapshotFutures.add(startFullSnapshot(repoName, "snap-" + i));
                ++blockedSnapshots[0];
            } else {
                blockedDelete = true;
                deleteFuture = startDelete(repoName, randomFrom(snapshotNames));
            }
        }
        awaitClusterState(masterNode, state -> (hasSnapshotsInProgress(state, blockedSnapshots[0])));
        if (blockedDelete) {
            awaitClusterState(masterNode, state -> hasDeletionsInProgress(state, 1));
        }
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        final String expectedFailureMessage = "Cannot start another operation, already running [" + limitToTest +
                                              "] operations and the current limit for concurrent snapshot operations is set to [" + limitToTest + "]";
        var createSnapshotFuture = startFullSnapshot(repoName, "expected-to-fail");
        assertThatThrownBy(createSnapshotFuture::get)
            .extracting(SQLExceptions::unwrap, THROWABLE)
            .isExactlyInstanceOf(ConcurrentSnapshotExecutionException.class)
            .hasMessageContaining(expectedFailureMessage);

        if (blockedDelete == false || limitToTest == 1) {
            var deleteSnapshotFuture = startDelete(repoName, snapshotNames);
            assertThatThrownBy(deleteSnapshotFuture::get)
                .extracting(SQLExceptions::unwrap, THROWABLE)
                .isExactlyInstanceOf(ConcurrentSnapshotExecutionException.class)
                .hasMessageContaining(expectedFailureMessage);
        }

        unblockNode(repoName, masterNode);
        if (deleteFuture != null) {
            assertAcked(deleteFuture.get());
        }
        for (var snapshotFuture : snapshotFutures) {
            assertSuccessful(snapshotFuture);
        }
    }

    private boolean hasSnapshotsInProgress(ClusterState state, int count) {
        var snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        return snapshotsInProgress.entries().size() == count;
    }

    private boolean hasDeletionsInProgress(ClusterState state, int count) {
        var snapshotDeletionsInProgress = state.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        );
        return snapshotDeletionsInProgress.getEntries().size() == count;
    }

    private String[] createNSnapshots(String repoName, int count) throws Exception {
        final String[] snapshotNames = new String[count];
        final String prefix = "snap-" + UUIDs.randomBase64UUID(random()).toLowerCase(Locale.ROOT) + "-";
        for (int i = 0; i < count; i++) {
            final String name = prefix + i;
            startFullSnapshot(repoName, name).get(5, TimeUnit.SECONDS);
            snapshotNames[i] = name;
        }
        logger.info("--> created {} in [{}]", snapshotNames, repoName);
        return snapshotNames;
    }

    private void createTable(String name) {
        execute(String.format(Locale.ENGLISH,
            """
            create table "%s" (
                id string primary key,
                s string
            ) clustered into 1 shards with (
                number_of_replicas = 0
            )
            """,
            name
        ));
    }

    private void createTableWithRecord(String name) {
        execute(String.format(Locale.ENGLISH,
            """
            create table "%s" (
                id string primary key,
                s string
            ) clustered into 1 shards with (
                number_of_replicas = 0
            )
            """,
            name
        ));
        execute("insert into \"" + name + "\" (id, s) values ('some_id', 'foo')");
        execute("refresh table \"" + name + "\"");
    }

    private void createTableWithRecord(String name, String nodeInclude, String nodeExclude) {
        String statement = String.format(Locale.ENGLISH,
            """
            create table "%s" (
                id string primary key,
                s string
            ) clustered into 1 shards with (
                number_of_replicas = 0,
                "routing.allocation.include._name" = ?,
                "routing.allocation.exclude._name" = ?
            )
            """,
            name
        );
        execute(statement, new Object[] { nodeInclude, nodeExclude });
        execute("insert into \"" + name + "\" (id, s) values ('some_id', 'foo')");
        execute("refresh table \"" + name + "\"");
    }

    private CompletableFuture<AcknowledgedResponse> startDelete(String repoName,
                                                                String... snapshotNames) {
        return startDelete(cluster().client(), repoName, snapshotNames);
    }

    private CompletableFuture<AcknowledgedResponse> startDelete(Client client,
                                                                String repoName,
                                                                String... snapshotNames) {
        logger.info("--> deleting snapshots [{}] from repo [{}]", snapshotNames, repoName);
        DeleteSnapshotRequest request = new DeleteSnapshotRequest(repoName, snapshotNames);
        return client.execute(DeleteSnapshotAction.INSTANCE, request);
    }

    private CompletableFuture<SnapshotInfo> startFullSnapshot(String repoName,
                                                              String snapshotName,
                                                              String... indices) {
        return createSnapshot(cluster().client(), repoName, snapshotName, false, indices);
    }

    private CompletableFuture<SnapshotInfo> startFullSnapshotFromMasterClient(String repoName,
                                                                              String snapshotName) {
        return createSnapshot(cluster().masterClient(), repoName, snapshotName, false);
    }

    private CompletableFuture<SnapshotInfo> createSnapshot(Client client,
                                                           String repoName,
                                                           String snapshotName,
                                                           boolean partial,
                                                           String... indices) {
        logger.info("--> creating full snapshot [{}] to repo [{}]", snapshotName, repoName);
        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(repoName, snapshotName)
            .indices(indices)
            .partial(partial)
            .waitForCompletion(true);
        return client
            .execute(CreateSnapshotAction.INSTANCE, createSnapshotRequest)
            .thenApply(x -> x.getSnapshotInfo());
    }

    private CompletableFuture<SnapshotInfo> startAndBlockFailingFullSnapshot(String blockedRepoName,
                                                                             String snapshotName) throws InterruptedException {
        blockMasterFromFinalizingSnapshotOnIndexFile(blockedRepoName);
        CompletableFuture<SnapshotInfo> future = createSnapshot(
            cluster().client(),
            blockedRepoName,
            snapshotName,
            false
        );
        waitForBlock(cluster().getMasterName(), blockedRepoName, TimeValue.timeValueSeconds(30L));
        return future;
    }

    private CompletableFuture<AcknowledgedResponse> startAndBlockOnDeleteSnapshot(String repoName, String snapshotName) throws Exception {
        final String masterName = cluster().getMasterName();
        mockRepo(repoName, masterName).setBlockOnDeleteIndexFile();
        final CompletableFuture<AcknowledgedResponse> fut = startDelete(cluster().masterClient(), repoName, snapshotName);
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));
        return fut;
    }

    private void assertSuccessful(CompletableFuture<SnapshotInfo> future) throws Exception {
        assertBusy(() -> {
            assertThat(future).isCompletedWithValueMatching(sInfo -> sInfo.state() == SnapshotState.SUCCESS);
        });
    }

    private CompletableFuture<SnapshotInfo> startFullSnapshotBlockedOnDataNode(String snapshotName,
                                                                               String repoName,
                                                                               String dataNode) throws InterruptedException {
        mockRepo(repoName, dataNode).blockOnDataFiles(true);
        CompletableFuture<SnapshotInfo> future = startFullSnapshot(repoName, snapshotName);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));
        return future;
    }


    private static boolean snapshotHasCompletedShard(String snapshot, SnapshotsInProgress snapshotsInProgress) {
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                for (ObjectCursor<SnapshotsInProgress.ShardSnapshotStatus> shard : entry.shards().values()) {
                    if (shard.value.state().completed()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static void blockMasterFromDeletingIndexNFile(String repositoryName) {
        mockRepo(repositoryName, cluster().getMasterName()).setBlockOnDeleteIndexFile();
    }

    private static String startDataNodeWithLargeSnapshotPool() {
        return cluster().startDataOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
    }

    private void assertSnapshotStatusCountOnRepo(String repoName, int count) {
        var clusterService = cluster().getInstance(ClusterService.class, cluster().getMasterName());
        var snapshotsInProgress = clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        int snapshotCountInRepo = 0;
        for (var entry : snapshotsInProgress.entries()) {
            if (entry.repository().equals(repoName)) {
                snapshotCountInRepo++;
            }
        }
        assertThat(snapshotCountInRepo).isEqualTo(count);
    }

    private void awaitNoMoreRunningOperations() throws Exception {
        logger.info("--> verify no more operations in the cluster state");
        awaitClusterState(cluster().getMasterName(),
                          state -> state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().isEmpty()
                                   && state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
                                              .hasDeletionsInProgress() == false);
    }

    protected void awaitMasterFinishRepoOperations() throws Exception {
        logger.info("--> waiting for master to finish all repo operations on its SNAPSHOT pool");
        final ThreadPool masterThreadPool = cluster().getMasterNodeInstance(ThreadPool.class);
        assertBusy(() -> {
            for (ThreadPoolStats.Stats stat : masterThreadPool.stats()) {
                if (ThreadPool.Names.SNAPSHOT.equals(stat.getName())) {
                    assertThat(stat.getActive()).isEqualTo(0);
                    break;
                }
            }
        });
    }
}
