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
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TestFutureUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.carrotsearch.hppc.IntHashSet;

public class InternalSnapshotsInfoServiceTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private RepositoriesService repositoriesService;
    private RerouteService rerouteService;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        repositoriesService = mock(RepositoriesService.class);
        rerouteService = (reason, priority, listener) -> listener.onResponse(clusterService.state());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        final boolean terminated = terminate(threadPool);
        assert terminated;
        clusterService.close();
    }

    @Test
    public void testSnapshotShardSizes() throws Exception {
        final int maxConcurrentFetches = randomIntBetween(1, 10);

        final int numberOfShards = randomIntBetween(1, 50);
        final CountDownLatch rerouteLatch = new CountDownLatch(numberOfShards);
        final RerouteService rerouteService = (reason, priority, listener) -> {
            listener.onResponse(clusterService.state());
            assertThat(rerouteLatch.getCount()).isGreaterThanOrEqualTo(0L);
            rerouteLatch.countDown();
        };

        final InternalSnapshotsInfoService snapshotsInfoService =
            new InternalSnapshotsInfoService(Settings.builder()
                .put(InternalSnapshotsInfoService.INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING.getKey(), maxConcurrentFetches)
                .build(), clusterService, () -> repositoriesService, () -> rerouteService);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final long[] expectedShardSizes = new long[numberOfShards];
        for (int i = 0; i < expectedShardSizes.length; i++) {
            expectedShardSizes[i] = randomNonNegativeLong();
        }

        final AtomicInteger getShardSnapshotStatusCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        final Repository mockRepository = mock(Repository.class);

        var answer = new Answer<>() {

            public CompletableFuture<IndexShardSnapshotStatus> answer(InvocationOnMock invocation) throws Throwable {
                IndexId indexId = invocation.getArgument(1, IndexId.class);
                ShardId shardId = invocation.getArgument(2, ShardId.class);
                try {
                    assertThat(indexId.getName()).isEqualTo(indexName);
                    assertThat(shardId.id()).satisfies(
                        id -> assertThat(id).isGreaterThanOrEqualTo(0),
                        id -> assertThat(id).isLessThan(numberOfShards)
                    );
                    latch.await();
                    getShardSnapshotStatusCount.incrementAndGet();
                    return CompletableFuture.completedFuture(
                        IndexShardSnapshotStatus.newDone(0L, 0L, 0, 0, 0L, expectedShardSizes[shardId.id()], null));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        };
        when(mockRepository.getShardSnapshotStatus(any(), any(), any())).thenAnswer(answer);
        when(repositoriesService.repository("_repo")).thenReturn(mockRepository);

        applyClusterState("add-unassigned-shards", clusterState -> addUnassignedShards(clusterState, indexName, numberOfShards));
        waitForMaxActiveGenericThreads(Math.min(numberOfShards, maxConcurrentFetches));

        if (randomBoolean()) {
            applyClusterState("reapply-last-cluster-state-to-check-deduplication-works",
                state -> ClusterState.builder(state).incrementVersion().build());
        }

        assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes()).isEqualTo(numberOfShards);
        assertThat(snapshotsInfoService.numberOfKnownSnapshotShardSizes()).isEqualTo(0);

        latch.countDown();

        assertTrue(rerouteLatch.await(30L, TimeUnit.SECONDS));
        assertThat(snapshotsInfoService.numberOfKnownSnapshotShardSizes()).isEqualTo(numberOfShards);
        assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes()).isEqualTo(0);
        assertThat(snapshotsInfoService.numberOfFailedSnapshotShardSizes()).isEqualTo(0);
        assertThat(getShardSnapshotStatusCount.get()).isEqualTo(numberOfShards);

        final SnapshotShardSizeInfo snapshotShardSizeInfo = snapshotsInfoService.snapshotShardSizes();
        for (int i = 0; i < numberOfShards; i++) {
            final ShardRouting shardRouting = clusterService.state().routingTable().index(indexName).shard(i).primaryShard();
            assertThat(snapshotShardSizeInfo.getShardSize(shardRouting)).isEqualTo(expectedShardSizes[i]);
            assertThat(snapshotShardSizeInfo.getShardSize(shardRouting, Long.MIN_VALUE)).isEqualTo(expectedShardSizes[i]);
        }
    }

    @Test
    public void testErroneousSnapshotShardSizes() throws Exception {
        final AtomicInteger reroutes = new AtomicInteger();
        final RerouteService rerouteService = (reason, priority, listener) -> {
            reroutes.incrementAndGet();
            listener.onResponse(clusterService.state());
        };

        final InternalSnapshotsInfoService snapshotsInfoService =
            new InternalSnapshotsInfoService(Settings.builder()
                .put(InternalSnapshotsInfoService.INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING.getKey(), randomIntBetween(1, 10))
                .build(), clusterService, () -> repositoriesService, () -> rerouteService);

        final Map<InternalSnapshotsInfoService.SnapshotShard, Long> results = new ConcurrentHashMap<>();
        final Repository mockRepository = mock(Repository.class);

        var answer = new Answer<>() {
            public CompletableFuture<IndexShardSnapshotStatus> answer(InvocationOnMock invocation) throws Throwable {
                SnapshotId snapshotId = invocation.getArgument(0, SnapshotId.class);
                IndexId indexId = invocation.getArgument(1, IndexId.class);
                ShardId shardId = invocation.getArgument(2, ShardId.class);
                final InternalSnapshotsInfoService.SnapshotShard snapshotShard =
                    new InternalSnapshotsInfoService.SnapshotShard(new Snapshot("_repo", snapshotId), indexId, shardId);
                if (randomBoolean()) {
                    results.put(snapshotShard, Long.MIN_VALUE);
                    throw new SnapshotException(snapshotShard.snapshot(), "simulated");
                } else {
                    final long shardSize = randomNonNegativeLong();
                    results.put(snapshotShard, shardSize);
                    return CompletableFuture.completedFuture(
                        IndexShardSnapshotStatus.newDone(0L, 0L, 0, 0, 0L, shardSize, null));
                }
            }
        };
        when(mockRepository.getShardSnapshotStatus(any(), any(), any())).thenAnswer(answer);
        when(repositoriesService.repository("_repo")).thenReturn(mockRepository);

        final int maxShardsToCreate = scaledRandomIntBetween(10, 500);
        final Thread addSnapshotRestoreIndicesThread = new Thread(() -> {
            int remainingShards = maxShardsToCreate;
            while (remainingShards > 0) {
                final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                final int numberOfShards = randomIntBetween(1, remainingShards);
                try {
                    applyClusterState("add-more-unassigned-shards-for-" + indexName,
                        clusterState -> addUnassignedShards(clusterState, indexName, numberOfShards));
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    remainingShards -= numberOfShards;
                }
            }
        });
        addSnapshotRestoreIndicesThread.start();
        addSnapshotRestoreIndicesThread.join();

        final Predicate<Long> failedSnapshotShardSizeRetrieval = shardSize -> shardSize == Long.MIN_VALUE;
        assertBusy(() -> {
            assertThat(snapshotsInfoService.numberOfKnownSnapshotShardSizes()).isEqualTo(
                results.values().stream().filter(size -> failedSnapshotShardSizeRetrieval.test(size) == false).count()
            );
            assertThat(snapshotsInfoService.numberOfFailedSnapshotShardSizes())
                .isEqualTo((int) results.values().stream().filter(failedSnapshotShardSizeRetrieval).count());
            assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes()).isEqualTo(0);
        });

        final SnapshotShardSizeInfo snapshotShardSizeInfo = snapshotsInfoService.snapshotShardSizes();
        for (Map.Entry<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShard : results.entrySet()) {
            final ShardId shardId = snapshotShard.getKey().shardId();
            final ShardRouting shardRouting = clusterService.state().routingTable().index(shardId.getIndexName())
                .shard(shardId.id()).primaryShard();
            assertThat(shardRouting).isNotNull();

            final boolean success = failedSnapshotShardSizeRetrieval.test(snapshotShard.getValue()) == false;
            assertThat(snapshotShardSizeInfo.getShardSize(shardRouting)).isEqualTo(
                (success ? results.get(snapshotShard.getKey()) : ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
            );
            final long defaultValue = randomNonNegativeLong();
            assertThat(snapshotShardSizeInfo.getShardSize(shardRouting, defaultValue)).isEqualTo(
                success ? results.get(snapshotShard.getKey()) : defaultValue
            );
        }

        assertThat(results.size())
            .as("Expecting all snapshot shard size fetches to provide a size")
            .isEqualTo(maxShardsToCreate);
        assertThat( reroutes.get())
            .as("Expecting all snapshot shard size fetches to execute a Reroute")
            .isEqualTo(maxShardsToCreate);
    }

    @Test
    public void testNoLongerMaster() throws Exception {
        final InternalSnapshotsInfoService snapshotsInfoService =
            new InternalSnapshotsInfoService(Settings.EMPTY, clusterService, () -> repositoriesService, () -> rerouteService);

        final Repository mockRepository = mock(Repository.class);
        var answer = new Answer<>() {

            public CompletableFuture<IndexShardSnapshotStatus> answer(InvocationOnMock invocation) throws Throwable {
                return CompletableFuture.completedFuture(
                    IndexShardSnapshotStatus.newDone(0L, 0L, 0, 0, 0L, randomNonNegativeLong(), null)
                );
            }
        };
        when(mockRepository.getShardSnapshotStatus(any(), any(), any())).thenAnswer(answer);
        when(repositoriesService.repository("_repo")).thenReturn(mockRepository);

        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            final int nbShards =  randomIntBetween(1, 5);
            applyClusterState("restore-indices-when-master-" + indexName,
                clusterState -> addUnassignedShards(clusterState, indexName, nbShards));
        }

        applyClusterState("demote-current-master", this::demoteMasterNode);

        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            final int nbShards =  randomIntBetween(1, 5);
            applyClusterState("restore-indices-when-no-longer-master-" + indexName,
                clusterState -> addUnassignedShards(clusterState, indexName, nbShards));
        }

        assertBusy(() -> {
            assertThat(snapshotsInfoService.numberOfKnownSnapshotShardSizes()).isEqualTo(0);
            assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes()).isEqualTo(0);
            assertThat(snapshotsInfoService.numberOfFailedSnapshotShardSizes()).isEqualTo(0);
        });
    }

    @Test
    public void testCleanUpSnapshotShardSizes() throws Exception {
        final Repository mockRepository = mock(Repository.class);

        var answer = new Answer<>() {
            public CompletableFuture<IndexShardSnapshotStatus> answer(InvocationOnMock invocation) throws Throwable {
                SnapshotId snapshotId = invocation.getArgument(0, SnapshotId.class);
                if (randomBoolean()) {
                    throw new SnapshotException(new Snapshot("_repo", snapshotId), "simulated");
                } else {
                    return CompletableFuture.completedFuture(
                        IndexShardSnapshotStatus.newDone(0L, 0L, 0, 0, 0L, randomNonNegativeLong(), null));
                }
            }
        };
        when(mockRepository.getShardSnapshotStatus(any(), any(), any())).thenAnswer(answer);
        when(repositoriesService.repository("_repo")).thenReturn(mockRepository);

        final InternalSnapshotsInfoService snapshotsInfoService =
            new InternalSnapshotsInfoService(Settings.EMPTY, clusterService, () -> repositoriesService, () -> rerouteService);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int nbShards = randomIntBetween(1, 10);

        applyClusterState("new snapshot restore for index " + indexName,
            clusterState -> addUnassignedShards(clusterState, indexName, nbShards));

        // waiting for snapshot shard size fetches to be executed, as we want to verify that they are cleaned up
        assertBusy(() -> {
            assertThat(snapshotsInfoService.numberOfFailedSnapshotShardSizes() + snapshotsInfoService.numberOfKnownSnapshotShardSizes())
                .isEqualTo(nbShards);
        });

        if (randomBoolean()) {
            // simulate initialization and start of the shards
            final AllocationService allocationService = ESAllocationTestCase.createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), nbShards)
                .put(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), nbShards)
                .build(),
                snapshotsInfoService
            );
            applyClusterState("starting shards for " + indexName, clusterState ->
                    ESAllocationTestCase.startInitializingShardsAndReroute(allocationService, clusterState, indexName));
            assertTrue(clusterService.state().routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).isEmpty());

        } else {
            // simulate deletion of the index
            applyClusterState("delete index " + indexName, clusterState -> deleteIndex(clusterState, indexName));
            assertFalse(clusterService.state().metadata().hasIndex(indexName));
        }

        assertThat(snapshotsInfoService.numberOfKnownSnapshotShardSizes()).isEqualTo(0);
        assertThat(snapshotsInfoService.numberOfUnknownSnapshotShardSizes()).isEqualTo(0);
        assertThat(snapshotsInfoService.numberOfFailedSnapshotShardSizes()).isEqualTo(0);
    }

    private void applyClusterState(final String reason, final Function<ClusterState, ClusterState> applier) {
        TestFutureUtils.get(future -> clusterService.getClusterApplierService().onNewClusterState(reason,
            () -> applier.apply(clusterService.state()),
            new ClusterApplier.ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    future.onResponse(source);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    future.onFailure(e);
                }
            })
        );
    }

    private void waitForMaxActiveGenericThreads(final int nbActive) throws Exception {
        assertBusy(() -> {
            final ThreadPoolStats threadPoolStats = clusterService.getClusterApplierService().threadPool().stats();
            ThreadPoolStats.Stats generic = null;
            for (ThreadPoolStats.Stats threadPoolStat : threadPoolStats) {
                if (ThreadPool.Names.GENERIC.equals(threadPoolStat.getName())) {
                    generic = threadPoolStat;
                }
            }
            assertThat(generic).isNotNull();
            assertThat(generic.getActive()).isEqualTo(nbActive);
        }, 30L, TimeUnit.SECONDS);
    }

    private ClusterState addUnassignedShards(final ClusterState currentState, String indexName, int numberOfShards) {
        assertThat(currentState.metadata().hasIndex(indexName)).isFalse();

        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(Settings.builder()
                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(SETTING_CREATION_DATE, System.currentTimeMillis()));

        for (int i = 0; i < numberOfShards; i++) {
            indexMetadataBuilder.putInSyncAllocationIds(i, Collections.singleton(AllocationId.newInitializing().getId()));
        }

        final Metadata.Builder metadata = Metadata.builder(currentState.metadata())
            .put(indexMetadataBuilder.build(), true)
            .generateClusterUuidIfNeeded();

        final RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(random()),
            new Snapshot("_repo", new SnapshotId(randomAlphaOfLength(5), UUIDs.randomBase64UUID(random()))),
            Version.CURRENT,
            new IndexId(indexName, UUIDs.randomBase64UUID(random()))
        );

        final IndexMetadata indexMetadata = metadata.get(indexName);
        final Index index = indexMetadata.getIndex();

        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());
        routingTable.add(IndexRoutingTable.builder(index).initializeAsNewRestore(indexMetadata, recoverySource, new IntHashSet()).build());

        final RestoreInProgress.Builder restores =
            new RestoreInProgress.Builder(currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY));
        final ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shards = ImmutableOpenMap.builder();
        for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
            shards.put( new ShardId(index, i), new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId()));
        }

        restores.add(new RestoreInProgress.Entry(recoverySource.restoreUUID(), recoverySource.snapshot(), RestoreInProgress.State.INIT,
            Collections.singletonList(indexName), shards.build()));

        return ClusterState.builder(currentState)
            .putCustom(RestoreInProgress.TYPE, restores.build())
            .routingTable(routingTable.build())
            .metadata(metadata)
            .build();
    }

    private ClusterState demoteMasterNode(final ClusterState currentState) {
        final DiscoveryNode node = new DiscoveryNode("other", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        assertThat(currentState.nodes().get(node.getId())).isNull();

        return ClusterState.builder(currentState)
            .nodes(DiscoveryNodes.builder(currentState.nodes())
                .add(node)
                .masterNodeId(node.getId()))
            .build();
    }

    private ClusterState deleteIndex(final ClusterState currentState, final String indexName) {
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata()).remove(indexName))
            .routingTable(RoutingTable.builder(currentState.routingTable()).remove(indexName).build())
            .build();
    }
}
