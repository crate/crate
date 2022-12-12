/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.snapshots.SnapshotsService.MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.common.unit.TimeValue;
import io.crate.integrationtests.disruption.discovery.AbstractDisruptionTestCase;
import io.crate.testing.UseRandomizedSchema;

@SuppressWarnings("resource")
@UseRandomizedSchema(random = false)
//@Repeat(iterations = 50)
@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrentSnapshotsIT extends AbstractSnapshotIntegTestCase {

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
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-slow");

        CreateSnapshotFuture createSlowFuture = startFullSnapshotBlockedOnDataNode("slow-snapshot", repoName, dataNode);

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String indexFast = "index-fast";
        createIndexWithContent(indexFast, dataNode2, dataNode);

        CreateSnapshotFuture future = startFullSnapshot(repoName, "fast-snapshot", indexFast);
        assertSuccessful(future);

        unblockNode(repoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    @Test
    public void testDeletesAreBatched() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        createIndex("foo");
        ensureGreen();

        final int numSnapshots = randomIntBetween(1, 4);
        CreateSnapshotFuture[] createSnapshotFutures = new CreateSnapshotFuture[numSnapshots];
        final Collection<String> snapshotNames = new HashSet<>(numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
            final String snapshot = "snap-" + i;
            snapshotNames.add(snapshot);
            createSnapshotFutures[i] = startFullSnapshot(repoName, snapshot);
        }
        assertBusy(() -> assertThat(CompletableFuture.allOf(createSnapshotFutures)).isCompleted());
        for (CreateSnapshotFuture f : createSnapshotFutures) {
            assertSuccessful(f);
        }

        createIndexWithContent("index-slow");

        CreateSnapshotFuture createSlowFuture = startFullSnapshotBlockedOnDataNode("blocked-snapshot", repoName, dataNode);

        List<DeleteSnapshotFuture> deleteFutures = new ArrayList<>(snapshotNames.size());
        while (snapshotNames.isEmpty() == false) {
            final Collection<String> toDelete = randomSubsetOf(snapshotNames);
            if (toDelete.isEmpty()) {
                continue;
            }
            snapshotNames.removeAll(toDelete);
            for (String snapshotToDelete : toDelete) {
                deleteFutures.add(startDelete(repoName, snapshotToDelete));
            }
        }

        assertThat(createSlowFuture).isNotDone();

        final long repoGenAfterInitialSnapshots = getRepositoryData(repoName).getGenId();
        assertThat(repoGenAfterInitialSnapshots).isEqualTo(numSnapshots - 1L);
        unblockNode(repoName, dataNode);

        assertSuccessful(createSlowFuture);

        logger.info("--> waiting for batched deletes to finish");
        DeleteSnapshotFuture[] deleteFuturesArray = deleteFutures.toArray(new DeleteSnapshotFuture[0]);
        assertBusy(() -> assertThat(CompletableFuture.allOf(deleteFuturesArray)).isCompleted());

        logger.info("--> verifying repository state");
        final RepositoryData repositoryDataAfterDeletes = getRepositoryData(repoName);
        // One increment for snapshot, one for all the deletes
        assertThat(repositoryDataAfterDeletes.getGenId()).isEqualTo(repoGenAfterInitialSnapshots + 2);
        assertThat(repositoryDataAfterDeletes.getSnapshotIds()).hasSize(1);
        assertThat(repositoryDataAfterDeletes.getSnapshotIds().iterator().next().getName()).isEqualTo("blocked-snapshot");
    }

    @Test
    public void testBlockedRepoDoesNotBlockOtherRepos() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndex("foo");
        ensureGreen();
        createIndexWithContent("index-slow");

        CreateSnapshotFuture createSlowFuture = startAndBlockFailingFullSnapshot(blockedRepoName, "blocked-snapshot");
        CreateSnapshotFuture createFuture = startFullSnapshot(otherRepoName, "snap");
        unblockNode(blockedRepoName, masterNode);
        assertSuccessful(createFuture);
        assertBusy(() -> {
           assertThat(createSlowFuture).isCompletedExceptionally();
           assertThat(createSlowFuture.throwable()).satisfiesAnyOf(
               t -> assertThat(t).hasRootCauseExactlyInstanceOf(SnapshotException.class),
               t -> assertThat(t).hasRootCauseExactlyInstanceOf(IOException.class)
           );
        });
        execute("DROP REPOSITORY \"" + blockedRepoName + "\"");
    }

    @Test
    public void testMultipleReposAreIndependent() throws Exception {
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndexWithContent("test-index");

        CreateSnapshotFuture createSlowFuture = startFullSnapshotBlockedOnDataNode("blocked-snapshot", blockedRepoName, dataNode);

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        createNSnapshots(otherRepoName, randomIntBetween(1, 5));

        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    @Test
    public void testMultipleReposAreIndependent2() throws Exception {
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second repository's concurrent operations.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndexWithContent("test-index");

        CreateSnapshotFuture createSlowFuture = startFullSnapshotBlockedOnDataNode("blocked-snapshot", blockedRepoName, dataNode);

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        String[] snapshotNames = createNSnapshots(otherRepoName, randomIntBetween(1, 5));
        startDelete(otherRepoName, snapshotNames).get();

        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    @Test
    public void testMultipleReposAreIndependent3() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndexWithContent("test-index");

        CreateSnapshotFuture createSlowFuture = startFullSnapshot(blockedRepoName, "blocked-snapshot");
        assertSuccessful(createSlowFuture);
        blockNodeOnAnyFiles(blockedRepoName, masterNode);
        DeleteSnapshotFuture slowDeleteFuture = startDelete(blockedRepoName, "blocked-snapshot");

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        String[] snapshotNames = createNSnapshots(otherRepoName, randomIntBetween(1, 5));
        startDelete(otherRepoName, snapshotNames).get();

        unblockNode(blockedRepoName, masterNode);
        assertBusy(() -> assertThat(slowDeleteFuture).isCompleted());
    }

    @Test
    public void testSnapshotRunsAfterInProgressDelete() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        ensureGreen();
        createIndexWithContent("index-test");

        final String firstSnapshot = "first-snapshot";
        assertSuccessful(startFullSnapshot(repoName, firstSnapshot));

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        DeleteSnapshotFuture deleteFuture = startDelete(repoName, firstSnapshot);
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(10L));

        CreateSnapshotFuture createSnapshotFuture = startFullSnapshot(repoName, "second-snapshot");
        unblockNode(repoName, masterNode);
        assertBusy(() -> {
            assertThat(deleteFuture).isCompletedExceptionally();
            assertThat(deleteFuture.throwable()).satisfiesAnyOf(
                t -> assertThat(t).isExactlyInstanceOf(IOException.class),
                t -> assertThat(t).hasCauseExactlyInstanceOf(IOException.class)
            );
        });

        assertSuccessful(createSnapshotFuture);
    }

    @Test
    public void testAbortOneOfMultipleSnapshots() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String firstIndex = "index-one";
        createIndexWithContent(firstIndex);

        final String firstSnapshot = "snapshot-one";
        final CreateSnapshotFuture firstSnapshotResponse =
            startFullSnapshotBlockedOnDataNode(firstSnapshot, repoName, dataNode);

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String secondIndex = "index-two";
        createIndexWithContent(secondIndex, dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final CreateSnapshotFuture secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.entries().size() == 2 && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        final DeleteSnapshotFuture deleteSnapshotsResponse = startDelete(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        logger.info("--> start third snapshot");
        final CreateSnapshotFuture thirdSnapshotResponse = startFullSnapshot(repoName, "snapshot-three", secondIndex);

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
        assertThat(printedTable(response.rows())).isEqualTo(
        """
            snapshot-three
            snapshot-two
            """);
    }

    @Test
    public void testCascadedAborts() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");

        final String firstSnapshot = "snapshot-one";
        final CreateSnapshotFuture firstSnapshotResponse =
            startFullSnapshotBlockedOnDataNode(firstSnapshot, repoName, dataNode);

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        createIndexWithContent("index-two", dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final CreateSnapshotFuture secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.entries().size() == 2 && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        final DeleteSnapshotFuture deleteSnapshotsResponse = startDelete(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        final CreateSnapshotFuture thirdSnapshotResponse = startFullSnapshot(repoName, "snapshot-three");

        assertThat(firstSnapshotResponse).isNotDone();
        assertThat(secondSnapshotResponse).isNotDone();

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> {
            final SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries()).hasSize(3);
        }, 30L, TimeUnit.SECONDS);

        final DeleteSnapshotFuture delOne = startDelete(repoName, "snapshot-one");
        final DeleteSnapshotFuture delTwo = startDelete(repoName, "snapshot-two");
        final DeleteSnapshotFuture delThree = startDelete(repoName, "snapshot-three");

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
        assertAcked(deleteSnapshotsResponse.get());
        assertAcked(delOne.get());
        assertAcked(delTwo.get());
        assertAcked(delThree.get());

        logger.info("--> verify that all snapshots are gone");
        execute("SELECT count(*) FROM sys.snapshots WHERE repository=?", new Object[]{repoName});
        assertThat(response.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void testMasterFailOverWithQueuedDeletes() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String firstIndex = "index-one";
        createIndexWithContent(firstIndex);

        final String firstSnapshot = "snapshot-one";
        blockDataNode(repoName, dataNode);
        final CreateSnapshotFuture firstSnapshotResponse = startFullSnapshotFromNonMasterClient(repoName, firstSnapshot);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(5);
        final String secondIndex = "index-two";
        createIndexWithContent(secondIndex, dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final CreateSnapshotFuture secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.entries().size() == 2 && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        final DeleteSnapshotFuture firstDeleteFuture = startDeleteFromNonMasterClient(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        blockDataNode(repoName, dataNode2);
        final CreateSnapshotFuture snapshotThreeFuture = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(dataNode2, repoName, TimeValue.timeValueSeconds(30L));

        assertThat(firstSnapshotResponse).isNotDone();
        assertThat(secondSnapshotResponse).isNotDone();

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> {
            final SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries()).hasSize(3);
        }, 30L, TimeUnit.SECONDS);

        final DeleteSnapshotFuture delOne = startDeleteFromNonMasterClient(repoName, "snapshot-one");
        final DeleteSnapshotFuture delTwo = startDeleteFromNonMasterClient(repoName, "snapshot-two");
        final DeleteSnapshotFuture delThree = startDeleteFromNonMasterClient(repoName, "snapshot-three");
        logger.info("--> wait for delete to be enqueued in cluster state");
        awaitClusterState(state -> {
            final SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            return deletionsInProgress.getEntries().size() == 1 && deletionsInProgress.getEntries().get(0).getSnapshots().size() == 3;
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
        internalCluster().stopCurrentMasterNode();

        unblockNode(repoName, dataNode);
        unblockNode(repoName, dataNode2);

        assertAcked(firstDeleteFuture.get());
        assertAcked(delOne.get());
        assertAcked(delTwo.get());
        assertAcked(delThree.get());
        assertThat(snapshotThreeFuture).isCompletedExceptionally();

        logger.info("--> verify that all snapshots are gone and no more work is left in the cluster state");
        assertBusy(() -> {
            execute("SELECT count(*) FROM sys.snapshots WHERE repository=?", new Object[]{repoName});
            assertThat(response.rows()[0][0]).isEqualTo(0L);
            final ClusterState state = clusterService().state();
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries()).isEmpty();
            final SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            assertThat(snapshotDeletionsInProgress.getEntries()).isEmpty();
        }, 30L, TimeUnit.SECONDS);
    }

    @Test
    public void testAssertMultipleSnapshotsAndPrimaryFailOver() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String testIndex = "index-one";
        createIndexWithContent(testIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(testIndex);

        blockDataNode(repoName, dataNode);
        final CreateSnapshotFuture firstSnapshotResponse = startFullSnapshotFromMasterClient(repoName, "snapshot-one");
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        ensureGreen(testIndex);

        final String secondSnapshot = "snapshot-two";
        final CreateSnapshotFuture secondSnapshotResponse = startFullSnapshotFromMasterClient(repoName, secondSnapshot);

        internalCluster().restartNode(dataNode, TestCluster.EMPTY_CALLBACK);

        assertThat(firstSnapshotResponse.get().state()).isEqualTo(SnapshotState.PARTIAL);
        assertThat(secondSnapshotResponse.get().state()).isEqualTo(SnapshotState.PARTIAL);
    }

    @Test
    public void testQueuedDeletesWithFailures() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);

        final DeleteSnapshotFuture firstDeleteSnapshotFuture = startDelete(repoName, snapshotNames);
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        final CreateSnapshotFuture snapshotFuture = startFullSnapshot(repoName, "snapshot-queued");
        awaitNSnapshotsInProgress(1);

        DeleteSnapshotFuture secondDeleteSnapshotFuture =
            startDelete(repoName, ArrayUtils.concat(snapshotNames, new String[] {"snapshot-queued"}));
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, masterNode);
        assertBusy(() -> assertThat(firstDeleteSnapshotFuture).isCompletedExceptionally());

        // Second delete works out cleanly since the repo is unblocked now
        assertThat(secondDeleteSnapshotFuture.get().isAcknowledged()).isTrue();
        // Snapshot should have been aborted
        assertThat(snapshotFuture.get().state()).isEqualTo(SnapshotState.FAILED);

        execute("SELECT count(*) FROM sys.snapshots WHERE repository=?", new Object[]{repoName});
        assertThat(response.rows()[0][0]).isEqualTo(0L);
        awaitNoMoreRunningOperations();
    }

    @Test
    public void testQueuedDeletesWithOverlap() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        final DeleteSnapshotFuture firstDeleteSnapshotFuture = startAndBlockOnDeleteSnapshot(repoName, snapshotNames);

        final CreateSnapshotFuture snapshotFuture = createSnapshot(client(masterNode), repoName, "snapshot-queued");
        awaitNSnapshotsInProgress(1);

        final DeleteSnapshotFuture secondDeleteFuture =
            startDelete(repoName, ArrayUtils.concat(snapshotNames, new String[] {"snapshot-queued"}));
        awaitNDeletionsInProgress(1);

        unblockNode(repoName, masterNode);
        assertThat(firstDeleteSnapshotFuture.get().isAcknowledged()).isTrue();

        // Second delete works out cleanly since the repo is unblocked now
        assertBusy(() -> assertThat(secondDeleteFuture).isDone());
        // Snapshot should have been aborted
        assertThat(snapshotFuture.get().state()).isEqualTo(SnapshotState.FAILED);

        execute("SELECT count(*) FROM sys.snapshots WHERE repository=?", new Object[]{repoName});
        assertThat(response.rows()[0][0]).isEqualTo(0L);
        awaitNoMoreRunningOperations();
    }

    @Test
    public void testQueuedOperationsOnMasterRestart() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        startAndBlockOnDeleteSnapshot(repoName, snapshotNames);

        startFullSnapshot(repoName, "snapshot-three");

        startDelete(repoName, ArrayUtils.concat(snapshotNames, new String[] {"snapshot-three"}));
//        awaitNDeletionsInProgress(1);

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
    }

    @Test
    public void testQueuedOperationsOnMasterDisconnect() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        final String masterNode = internalCluster().getMasterName();
        final NetworkDisruption networkDisruption = isolateMasterDisruption(new NetworkDisruption.NetworkDisconnect());
        internalCluster().setDisruptionScheme(networkDisruption);

        blockNodeOnAnyFiles(repoName, masterNode);
        final DeleteSnapshotFuture firstDeleteSnapshotFuture = deleteSnapshot(client(masterNode), repoName, snapshotNames);
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        final CreateSnapshotFuture createThirdSnapshot = createSnapshot(client(masterNode), repoName, "snapshot-three");
        awaitNSnapshotsInProgress(1);

        final DeleteSnapshotFuture secondDeleteSnapshotFuture = deleteSnapshot(client(masterNode), repoName, snapshotNames);
        awaitNDeletionsInProgress(2);

        networkDisruption.startDisrupting();
//        ensureStableCluster(3, dataNode);
        unblockNode(repoName, masterNode);
        networkDisruption.stopDisrupting();

        logger.info("--> make sure all failing requests get a response");
        assertThat(firstDeleteSnapshotFuture.get().isAcknowledged()).isTrue();
        assertThat(secondDeleteSnapshotFuture.get().isAcknowledged()).isTrue();
        assertThat(createThirdSnapshot.get().state()).isEqualTo(SnapshotState.SUCCESS);

        awaitNoMoreRunningOperations();
    }

    @Test
    public void testQueuedOperationsOnMasterDisconnectAndRepoFailure() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        final String masterNode = internalCluster().getMasterName();
        final NetworkDisruption networkDisruption = isolateMasterDisruption(new NetworkDisruption.NetworkDisconnect());
        internalCluster().setDisruptionScheme(networkDisruption);

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final CreateSnapshotFuture firstFailedSnapshotFuture =
            startFullSnapshotFromMasterClient(repoName, "failing-snapshot-1");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));
        final CreateSnapshotFuture secondFailedSnapshotFuture =
            startFullSnapshotFromMasterClient(repoName, "failing-snapshot-2");
        awaitNSnapshotsInProgress(2);

        final DeleteSnapshotFuture deleteFuture = deleteSnapshot(client(masterNode), repoName,
                ArrayUtils.concat(snapshotNames, new String[] {"failing-snapshot-1", "failing-snapshot-2"}));
        awaitNDeletionsInProgress(1);

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
    public void testQueuedOperationsAndBrokenRepoOnMasterFailOver() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();

        startAndBlockOnDeleteSnapshot(repoName, snapshotNames);

        corruptIndexN(repoPath, generation);

        client().execute(CreateSnapshotAction.INSTANCE, new CreateSnapshotRequest(repoName, "snapshot-three")
            .waitForCompletion(false));

        final DeleteSnapshotFuture deleteFuture = startDeleteFromNonMasterClient(repoName, snapshotNames);
        awaitNDeletionsInProgress(1);

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        assertThat(deleteFuture).isCompletedExceptionally();
    }

    @Test
    public void testQueuedSnapshotOperationsAndBrokenRepoOnMasterFailOver() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();
        final String masterNode = internalCluster().getMasterName();
        blockNodeOnAnyFiles(repoName, masterNode);
        final CreateSnapshotFuture snapshotThree = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        corruptIndexN(repoPath, generation);

        final CreateSnapshotFuture snapshotFour = startFullSnapshotFromNonMasterClient(repoName, "snapshot-four");
        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        assertThat(snapshotThree).isCompletedExceptionally();
        assertThat(snapshotFour).isCompletedExceptionally();
    }

    @Test
    public void testQueuedSnapshotOperationsAndBrokenRepoOnMasterFailOver2() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();
        final String masterNode = internalCluster().getMasterName();
        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final CreateSnapshotFuture snapshotThree = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        corruptIndexN(repoPath, generation);

        final CreateSnapshotFuture snapshotFour = startFullSnapshotFromNonMasterClient(repoName, "snapshot-four");
        awaitNSnapshotsInProgress(2);

        final NetworkDisruption networkDisruption = isolateMasterDisruption(new NetworkDisruption.NetworkDisconnect());
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, masterNode);
        networkDisruption.stopDisrupting();
        awaitNoMoreRunningOperations();
        assertThat(snapshotThree).isCompletedExceptionally();
        assertThat(snapshotFour).isCompletedExceptionally();
    }

    @Test
    public void testQueuedSnapshotOperationsAndBrokenRepoOnMasterFailOverMultipleRepos() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final String masterNode = internalCluster().getMasterName();

        final String blockedRepoName = "repo-blocked";
        createRepository(blockedRepoName, "mock");
        String[] snapshotNames = createNSnapshots(blockedRepoName, randomIntBetween(1, 5));
        blockNodeOnAnyFiles(blockedRepoName, masterNode);
        final DeleteSnapshotFuture deleteFuture = startDeleteFromNonMasterClient(blockedRepoName, snapshotNames);
        waitForBlock(masterNode, blockedRepoName, TimeValue.timeValueSeconds(30L));
        final CreateSnapshotFuture createBlockedSnapshot =
            startFullSnapshotFromNonMasterClient(blockedRepoName, "queued-snapshot");

        final long generation = getRepositoryData(repoName).getGenId();
        blockNodeOnAnyFiles(repoName, masterNode);
        final CreateSnapshotFuture snapshotThree = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        corruptIndexN(repoPath, generation);

        final CreateSnapshotFuture snapshotFour = startFullSnapshotFromNonMasterClient(repoName, "snapshot-four");
        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        assertBusy(() -> assertThat(snapshotThree).isCompletedExceptionally());
        assertBusy(() -> assertThat(snapshotFour).isCompletedExceptionally());
        assertAcked(deleteFuture.get());
        try {
            createBlockedSnapshot.get();
        } catch (Exception e) {
            // Ignored, thrown most of the time but due to retries when shutting down the master could randomly pass when the request is
            // retried and gets executed after the above delete
        }
    }

    @Test
    public void testMultipleSnapshotsQueuedAfterDelete() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(1, 5));

        final DeleteSnapshotFuture deleteFuture = startAndBlockOnDeleteSnapshot(repoName, snapshotNames);
        final CreateSnapshotFuture snapshotThree = startFullSnapshot(repoName, "snapshot-three");
        final CreateSnapshotFuture snapshotFour = startFullSnapshot(repoName, "snapshot-four");

        unblockNode(repoName, masterNode);

        assertSuccessful(snapshotThree);
        assertSuccessful(snapshotFour);
        assertAcked(deleteFuture.get());
    }


    @Test
    public void testQueuedSnapshotsWaitingForShardReady() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        final String testIndex = "test-idx";
        // Create index on two nodes and make sure each node has a primary by setting no replicas
        execute("CREATE TABLE \"" + testIndex + "\"(a int, s string) CLUSTERED INTO " + between(2, 10) +
                " SHARDS WITH (number_of_replicas=0)");

        ensureGreen(testIndex);

        logger.info("--> indexing some data");
        Object[][] data = new Object[100][2];
        for (int i = 0; i < 100; i++) {
            data[i][0] = i;
            data[i][1] = "foo" + i;
        }
        execute("INSERT INTO \"" + testIndex + "\"(a, s) VALUES(?, ?)", data);
        refresh();

        logger.info("--> start relocations");
        allowNodes(testIndex, 1);

        logger.info("--> wait for relocations to start");
        assertBusy(() -> {
            execute("SELECT count(*) FROM sys.shards WHERE table_name=? AND routing_state='RELOCATING'",
                    new Object[]{testIndex});
            assertThat((long) response.rows()[0][0]).isGreaterThan(0L);
        });
        logger.info("--> start two snapshots");
        final String snapshotOne = "snap-1";
        final String snapshotTwo = "snap-2";
        final CreateSnapshotFuture snapOneResponse = startFullSnapshot(repoName, snapshotOne, testIndex);
        final CreateSnapshotFuture snapTwoResponse = startFullSnapshot(repoName, snapshotTwo, testIndex);

        logger.info("--> wait for snapshot to complete");
        for (SnapshotInfo si : List.of(snapOneResponse.get(), snapTwoResponse.get())) {
            assertThat(si.state()).isEqualTo(SnapshotState.SUCCESS);
            assertThat(si.shardFailures()).isEmpty();
        }
    }

    @Test
    public void testBackToBackQueuedDeletes() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        final String[] snapshots = createNSnapshots(repoName, 2);
        final String snapshotOne = snapshots[0];
        final String snapshotTwo = snapshots[1];

        final DeleteSnapshotFuture deleteSnapshotOne = startAndBlockOnDeleteSnapshot(repoName, snapshotOne);
        final DeleteSnapshotFuture deleteSnapshotTwo = startDelete(repoName, snapshotTwo);
        awaitNDeletionsInProgress(2);

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
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");

        final String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        final CreateSnapshotFuture snapshotThree = startAndBlockFailingFullSnapshot(repoName, "snap-other");

        final String masterName = internalCluster().getMasterName();

        final String snapshotOne = snapshotNames[0];
        final DeleteSnapshotFuture deleteSnapshotOne = startDelete(repoName, snapshotOne);
        awaitNDeletionsInProgress(1);

        unblockNode(repoName, masterName);

        assertBusy(() -> assertThat(snapshotThree).isCompletedExceptionally());
        assertAcked(deleteSnapshotOne.get());
    }

    @Test
    public void testStartDeleteDuringFinalizationCleanup() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        createNSnapshots(repoName, randomIntBetween(1, 5));
        final String snapshotName = "snap-name";
        blockMasterFromDeletingIndexNFile(repoName);
        final CreateSnapshotFuture snapshotFuture = startFullSnapshot(repoName, snapshotName);
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));
        final DeleteSnapshotFuture deleteFuture = startDelete(repoName, snapshotName);
        awaitNDeletionsInProgress(1);
        unblockNode(repoName, masterName);
        assertSuccessful(snapshotFuture);
        assertAcked(deleteFuture.get(30L, TimeUnit.SECONDS));
    }

    @Test
    public void testEquivalentDeletesAreDeduplicated() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(1, 5));

        blockMasterFromDeletingIndexNFile(repoName);
        final int deletes = randomIntBetween(2, 10);
        final List<DeleteSnapshotFuture> deleteResponses = new ArrayList<>(deletes);
        for (int i = 0; i < deletes; ++i) {
            deleteResponses.add(startDelete(repoName, snapshotNames));
        }
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));
        awaitNDeletionsInProgress(1);
        for (DeleteSnapshotFuture deleteResponse : deleteResponses) {
            assertThat(deleteResponse).isNotDone();
        }
        awaitNDeletionsInProgress(1);
        unblockNode(repoName, masterName);
        for (DeleteSnapshotFuture deleteResponse : deleteResponses) {
            assertAcked(deleteResponse.get());
        }
    }

    @Test
    public void testMasterFailoverOnFinalizationLoop() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        final NetworkDisruption networkDisruption = isolateMasterDisruption(new NetworkDisruption.NetworkDisconnect());
        internalCluster().setDisruptionScheme(networkDisruption);

        final String[] snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));
        final String masterName = internalCluster().getMasterName();
        blockMasterFromDeletingIndexNFile(repoName);
        final CreateSnapshotFuture snapshotThree = startFullSnapshotFromMasterClient(repoName, "snap-other");
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));

        final String snapshotOne = snapshotNames[0];
        final DeleteSnapshotFuture deleteSnapshotOne = startDelete(repoName, snapshotOne);
        awaitNDeletionsInProgress(1);
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
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked-1";
        final String otherBlockedRepoName = "test-repo-blocked-2";
        createRepository(blockedRepoName, "mock");
        createRepository(otherBlockedRepoName, "mock");
        createIndexWithContent("test-index");

        final CreateSnapshotFuture createSlowFuture1 =
            startFullSnapshotBlockedOnDataNode("blocked-snapshot", blockedRepoName, dataNode);
        final CreateSnapshotFuture createSlowFuture2 =
            startFullSnapshotBlockedOnDataNode("blocked-snapshot-2", blockedRepoName, dataNode);
        final CreateSnapshotFuture createSlowFuture3 =
            startFullSnapshotBlockedOnDataNode("other-blocked-snapshot", otherBlockedRepoName, dataNode);
        awaitNSnapshotsInProgress(3);

        assertSnapshotStatusCountOnRepo(blockedRepoName, 2);
        assertSnapshotStatusCountOnRepo(otherBlockedRepoName, 1);

        unblockNode(blockedRepoName, dataNode);
        awaitNSnapshotsInProgress(1);
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
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked-1";
        final String otherBlockedRepoName = "test-repo-blocked-2";
        createRepository(blockedRepoName, "mock");
        createRepository(otherBlockedRepoName, "mock");
        createIndexWithContent("test-index");

        final CreateSnapshotFuture createSlowFuture1 =
            startFullSnapshotBlockedOnDataNode("blocked-snapshot", blockedRepoName, dataNode);
        final CreateSnapshotFuture createSlowFuture2 =
            startFullSnapshotBlockedOnDataNode("blocked-snapshot-2", blockedRepoName, dataNode);
        final CreateSnapshotFuture createSlowFuture3 =
            startFullSnapshotBlockedOnDataNode("other-blocked-snapshot", otherBlockedRepoName, dataNode);
        awaitNSnapshotsInProgress(3);
        unblockNode(blockedRepoName, dataNode);
        unblockNode(otherBlockedRepoName, dataNode);

        assertSuccessful(createSlowFuture1);
        assertSuccessful(createSlowFuture2);
        assertSuccessful(createSlowFuture3);
    }

    @Test
    public void testMasterFailoverAndMultipleQueuedUpSnapshotsAcrossTwoRepos() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final String otherRepoName = "other-test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createRepository(otherRepoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));
        final int countOtherRepo = randomIntBetween(2, 5);
        createNSnapshots(otherRepoName, countOtherRepo);
        // Skip corruption step. The original test corrupts the repository data. We don't cache the repository data therefore the corruption
        // will break the test.
//        corruptIndexN(repoPath, getRepositoryData(repoName).getGenId());

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        blockMasterFromFinalizingSnapshotOnIndexFile(otherRepoName);

        client().execute(CreateSnapshotAction.INSTANCE, new CreateSnapshotRequest(repoName, "snapshot-blocked-1")
            .waitForCompletion(false)).get();
        client().execute(CreateSnapshotAction.INSTANCE, new CreateSnapshotRequest(repoName, "snapshot-blocked-2")
            .waitForCompletion(false)).get();
        client().execute(CreateSnapshotAction.INSTANCE, new CreateSnapshotRequest(otherRepoName, "snapshot-other-blocked-1")
            .waitForCompletion(false)).get();
        client().execute(CreateSnapshotAction.INSTANCE, new CreateSnapshotRequest(otherRepoName, "snapshot-other-blocked-2")
            .waitForCompletion(false)).get();

        awaitNSnapshotsInProgress(4);
        final String initialMaster = internalCluster().getMasterName();
        waitForBlock(initialMaster, repoName, TimeValue.timeValueSeconds(30L));
        waitForBlock(initialMaster, otherRepoName, TimeValue.timeValueSeconds(30L));

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3, dataNode);
        awaitNoMoreRunningOperations();

        final RepositoryData repositoryData = getRepositoryData(otherRepoName);
        assertThat(repositoryData.getSnapshotIds()).hasSize(countOtherRepo + 2);
    }

    @Test
    public void testConcurrentOperationsLimit() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");

        final int limitToTest = randomIntBetween(1, 3);
        // Setting not exposed via SQL
        client().admin().cluster()
            .execute(ClusterUpdateSettingsAction.INSTANCE, new ClusterUpdateSettingsRequest().persistentSettings(
                Settings.builder()
                    .put(MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING.getKey(), limitToTest)
            )).get();

        final String[] snapshotNames = createNSnapshots(repoName, limitToTest + 1);
        blockNodeOnAnyFiles(repoName, masterName);
        int blockedSnapshots = 0;
        boolean blockedDelete = false;
        final List<CreateSnapshotFuture> snapshotFutures = new ArrayList<>();
        DeleteSnapshotFuture deleteFuture = null;
        for (int i = 0; i < limitToTest; ++i) {
            if (blockedDelete || randomBoolean()) {
                snapshotFutures.add(startFullSnapshot(repoName, "snap-" + i));
                ++blockedSnapshots;
            } else {
                blockedDelete = true;
                deleteFuture = startDelete(repoName, randomFrom(snapshotNames));
            }
        }
        awaitNSnapshotsInProgress(blockedSnapshots);
        if (blockedDelete) {
            awaitNDeletionsInProgress(1);
        }
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));

        final String expectedFailureMessage = "Cannot start another operation, already running [" + limitToTest +
                                              "] operations and the current limit for concurrent snapshot operations is set to [" + limitToTest + "]";
//        CreateSnapshotFuture createSnapshotFuture = startFullSnapshot(repoName, "expected-to-fail");
        CreateSnapshotFuture createSnapshotFuture = new CreateSnapshotFuture();
        BiConsumer<CreateSnapshotResponse, Throwable> listener = (r, t) -> {
            if (t != null) {
                createSnapshotFuture.t = t;
                createSnapshotFuture.completeExceptionally(t);
            } else if (r != null) {
                createSnapshotFuture.complete(r.getSnapshotInfo());
            }
        };
        client().execute(CreateSnapshotAction.INSTANCE, new CreateSnapshotRequest(repoName, "expected-to-fail")
            .waitForCompletion(false)).whenComplete(listener);

        createSnapshotFuture.get();
        assertBusy(() -> assertThat(createSnapshotFuture).isCompletedExceptionally());
        assertThat(createSnapshotFuture.throwable()).isExactlyInstanceOf(ConcurrentSnapshotExecutionException.class);
        assertThat(createSnapshotFuture.throwable()).hasMessageContaining(expectedFailureMessage);
        if (blockedDelete == false || limitToTest == 1) {
            DeleteSnapshotFuture deleteSnapshotFuture = startDelete(repoName, snapshotNames);
            assertBusy(() -> assertThat(deleteSnapshotFuture).isCompletedExceptionally());
            assertThat(deleteSnapshotFuture.throwable()).isExactlyInstanceOf(ConcurrentSnapshotExecutionException.class);
            assertThat(deleteSnapshotFuture.throwable()).hasMessageContaining(expectedFailureMessage);
        }

        unblockNode(repoName, masterName);
        if (deleteFuture != null) {
            assertAcked(deleteFuture.get());
        }
        for (CreateSnapshotFuture snapshotFuture : snapshotFutures) {
            assertSuccessful(snapshotFuture);
        }
    }

    private void createRepository(String name, String type) throws ExecutionException, InterruptedException {
        createRepository(name, type, randomRepoPath());
    }

    private void createRepository(String name, String type, Path repoPath) throws ExecutionException, InterruptedException {
        var putRepositoryRequest = new PutRepositoryRequest(name)
            .type(type)
            .settings(Settings.builder()
              .put("location", repoPath)
              .put("compress", randomBoolean())
              .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            );
        assertAcked(client().admin().cluster().execute(PutRepositoryAction.INSTANCE, putRepositoryRequest).get());
    }

    private static String startDataNodeWithLargeSnapshotPool() {
        return internalCluster().startDataOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
    }

    private void assertSnapshotStatusCountOnRepo(String repoName, int count) {
        var clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        var snapshotsInProgress = clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        int snapshotCountInRepo = 0;
        for (var entry : snapshotsInProgress.entries()) {
            if (entry.repository().equals(repoName)) {
                snapshotCountInRepo++;
            }
        }
        assertThat(snapshotCountInRepo).isEqualTo(count);
    }


    private String[] createNSnapshots(String repoName, int count) {
        final String[] snapshotNames = new String[count];
        final String prefix = "snap-" + UUIDs.randomBase64UUID(random()).toLowerCase(Locale.ROOT) + "-";
        for (int i = 0; i < count; i++) {
            final String name = prefix + i;
            startFullSnapshotSync(repoName, name);
            snapshotNames[i] = name;
        }
        logger.info("--> created {} in [{}]", snapshotNames, repoName);
        return snapshotNames;
    }

    private void awaitNoMoreRunningOperations() throws Exception {
        awaitNoMoreRunningOperations(internalCluster().getMasterName());
    }

    private DeleteSnapshotFuture startDeleteFromNonMasterClient(String repoName, String... snapshotNames) {
        logger.info("--> deleting snapshots [{}] from repo [{}] from non master client", snapshotNames, repoName);
        return deleteSnapshot(internalCluster().nonMasterClient(), repoName, snapshotNames);
    }

    private DeleteSnapshotFuture startDelete(String repoName, String... snapshotNames) {
        logger.info("--> deleting snapshots [{}] from repo [{}]", snapshotNames, repoName);
        return deleteSnapshot(internalCluster().client(), repoName, snapshotNames);
    }

    private DeleteSnapshotFuture deleteSnapshot(Client client, String repoName, String... snapshotNames) {
        DeleteSnapshotFuture future = new DeleteSnapshotFuture();
        BiConsumer<AcknowledgedResponse, Throwable> listener = (r, t) -> {
            if (t != null) {
                future.t = t;
                future.completeExceptionally(t);
            } else if (r != null) {
                future.complete(r);
            }
        };
        client.execute(DeleteSnapshotAction.INSTANCE, new DeleteSnapshotRequest(repoName, snapshotNames))
            .whenComplete(listener);
        return future;
    }

    private CreateSnapshotFuture startFullSnapshotFromNonMasterClient(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}] from non master client", snapshotName, repoName);
        return createSnapshot(internalCluster().nonMasterClient(), repoName, snapshotName);
    }

    private CreateSnapshotFuture startFullSnapshotFromMasterClient(@SuppressWarnings("SameParameterValue") String repoName,
                                                                   String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}] from master client", snapshotName, repoName);
        return createSnapshot(internalCluster().masterClient(), repoName, snapshotName);
    }

    private CreateSnapshotFuture startFullSnapshot(String repoName, String snapshotName, String... indices) {
        logger.info("--> creating full snapshot [{}] to repo [{}]", snapshotName, repoName);
        return createSnapshot(internalCluster().client(), repoName, snapshotName, indices);
    }

    private CreateSnapshotFuture createSnapshot(Client client, String repoName, String snapshotName, String... indices) {
        CreateSnapshotFuture future = new CreateSnapshotFuture();
        BiConsumer<CreateSnapshotResponse, Throwable> listener = (r, t) -> {
            if (t != null) {
                future.t = t;
                future.completeExceptionally(t);
            } else if (r != null) {
                future.complete(r.getSnapshotInfo());
            }
        };
        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(repoName, snapshotName);
        if (indices != null && indices.length > 0) {
            createSnapshotRequest.indices(indices);
        }
        client.execute(CreateSnapshotAction.INSTANCE, createSnapshotRequest.waitForCompletion(true)).whenComplete(listener);
        return future;
    }

    private void startFullSnapshotSync(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}]", snapshotName, repoName);
        execute("CREATE SNAPSHOT \"" + repoName + "\".\"" + snapshotName + "\" ALL WITH (wait_for_completion = true)");
    }

    private void awaitClusterState(Predicate<ClusterState> statePredicate) throws Exception {
        awaitBusyClusterState(internalCluster().getMasterName(), statePredicate);
    }


    // Large snapshot pool settings to set up nodes for tests involving multiple repositories that need to have enough
    // threads so that blocking some threads on one repository doesn't block other repositories from doing work
    private static final Settings LARGE_SNAPSHOT_POOL_SETTINGS = Settings.builder()
        .put("thread_pool.snapshot.core", 5).put("thread_pool.snapshot.max", 5).build();

    private static final Settings SINGLE_SHARD_NO_REPLICA = indexSettingsNoReplicas(1).build();

    private void createIndexWithContent(String indexName) {
        createIndexWithContent(indexName, SINGLE_SHARD_NO_REPLICA);
        ensureGreen();
    }

    private void createIndexWithContent(String indexName, String nodeInclude, String nodeExclude) {
        createIndexWithContent(indexName, indexSettingsNoReplicas(1)
            .put("index.routing.allocation.include._name", nodeInclude)
            .put("index.routing.allocation.exclude._name", nodeExclude).build());
        ensureGreen();
    }

    private void createIndexWithContent(String indexName, Settings indexSettings) {
        logger.info("--> creating index [{}]", indexName);
        execute("CREATE TABLE \"" + indexName + "\"(id string primary key, s string) CLUSTERED INTO " +
                indexSettings.get(SETTING_NUMBER_OF_SHARDS) + " SHARDS WITH(number_of_replicas=" +
                indexSettings.get(SETTING_NUMBER_OF_REPLICAS) + ")");
        execute("INSERT INTO \"" + indexName + "\"(id, s) VALUES(?, ?)", new Object[] {"some_id", "foo"});
        refresh(indexName);
    }

    private static boolean snapshotHasCompletedShard(@SuppressWarnings("SameParameterValue") String snapshot,
                                                     SnapshotsInProgress snapshotsInProgress) {
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

    private void assertSuccessful(CreateSnapshotFuture future) throws Exception {
        assertBusy(() -> {
            assertThat(future).isCompletedWithValueMatching(sInfo -> sInfo.state() == SnapshotState.SUCCESS);
        });
    }

    private void corruptIndexN(Path repoPath, long generation) throws IOException {
        logger.info("--> corrupting [index-{}] in [{}]", generation, repoPath);
        Path indexNBlob = repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generation);
        assertFileExists(indexNBlob);
        Files.write(indexNBlob, randomByteArrayOfLength(1), StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void awaitNDeletionsInProgress(int count) throws Exception {
        logger.info("--> wait for [{}] deletions to show up in the cluster state", count);
        awaitClusterState(state ->
                              state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).getEntries().size() == count);
    }

    private void awaitNSnapshotsInProgress(int count) throws Exception {
        logger.info("--> wait for [{}] snapshots to show up in the cluster state", count);
        awaitClusterState(state ->
                              state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().size() == count);
    }

    private DeleteSnapshotFuture startAndBlockOnDeleteSnapshot(@SuppressWarnings("SameParameterValue") String repoName,
                                                               String... snapshotNames)
        throws InterruptedException {
        final String masterName = internalCluster().getMasterName();
        blockNodeOnAnyFiles(repoName, masterName);
        final DeleteSnapshotFuture fut = startDelete(repoName, snapshotNames);
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));
        return fut;
    }

    private CreateSnapshotFuture startAndBlockFailingFullSnapshot(String blockedRepoName,
                                                                  String snapshotName) throws InterruptedException {
        blockMasterFromFinalizingSnapshotOnIndexFile(blockedRepoName);
        CreateSnapshotFuture future = startFullSnapshot(blockedRepoName, snapshotName);
        waitForBlock(internalCluster().getMasterName(), blockedRepoName, TimeValue.timeValueSeconds(30L));
        return future;
    }

    private CreateSnapshotFuture startFullSnapshotBlockedOnDataNode(String snapshotName,
                                                                    String repoName,
                                                                    String dataNode) throws InterruptedException {
        blockDataNode(repoName, dataNode);
        CreateSnapshotFuture future = startFullSnapshot(repoName, snapshotName);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));
        return future;
    }

    private static class CreateSnapshotFuture extends CompletableFuture<SnapshotInfo> {
        private Throwable t;

        private Throwable throwable() {
            return t;
        }
    }

    private static class DeleteSnapshotFuture extends CompletableFuture<AcknowledgedResponse> {
        private Throwable t;

        private Throwable throwable() {
            return t;
        }
    }
}
