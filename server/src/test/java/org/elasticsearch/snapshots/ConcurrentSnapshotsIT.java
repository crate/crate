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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import java.util.function.Predicate;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.common.unit.TimeValue;
import io.crate.concurrent.CompletableFutures;
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
    @UseRandomizedSchema(random = false)
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
    @UseRandomizedSchema(random = false)
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
        awaitClusterState(masterNode, hasInProgressDeletions(1));

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
        awaitClusterState(cluster().getMasterName(), hasInProgressDeletions(1));

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
        awaitClusterState(cluster().getMasterName(), hasInProgressDeletions(1));

        mockRepo(repoName, dataNode2).blockOnDataFiles(true);
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

        assertThat(firstDeleteFuture.get().isAcknowledged()).isTrue();
        assertThat(delOne.get().isAcknowledged()).isTrue();
        assertThat(delTwo.get().isAcknowledged()).isTrue();
        assertThat(delThree.get().isAcknowledged()).isTrue();
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
    @UseRandomizedSchema(random = false)
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
        awaitClusterState(masterNode, state -> inProgressSnapshots(state) == 2);

        execute("drop table tbl2");
        unblockNode(repoName, masterNode);

        assertThat(snapshotThree.get().state()).isEqualTo(SnapshotState.PARTIAL);
        assertThat(snapshotFour.get().state()).isEqualTo(SnapshotState.PARTIAL);
        assertThat(deleteFuture.get().isAcknowledged()).isTrue();
    }

    private int inProgressSnapshots(ClusterState state) {
        SnapshotsInProgress inProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        return inProgress.entries().size();
    }

    private Predicate<ClusterState> hasInProgressDeletions(int count) {
        return state ->
            state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).getEntries().size() == count;
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
                                                              String snapshotName) {
        return createSnapshot(cluster().client(), repoName, snapshotName, false);
    }

    private CompletableFuture<SnapshotInfo> createSnapshot(Client client,
                                                           String repoName,
                                                           String snapshotName,
                                                           boolean partial) {
        logger.info("--> creating full snapshot [{}] to repo [{}]", snapshotName, repoName);
        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(repoName, snapshotName)
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
}
