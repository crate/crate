/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.indices.recovery;

import static com.carrotsearch.randomizedtesting.RandomizedTest.biasedDoubleBetween;
import static io.crate.testing.Asserts.assertThat;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.node.RecoverySettingsChunkSizePlugin.CHUNK_SIZE_SETTING;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushAction;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.flush.SyncedFlushUtil;
import org.elasticsearch.node.RecoverySettingsChunkSizePlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.elasticsearch.test.IntegTestCase.Scope;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.IndexParts;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexRecoveryIT extends IntegTestCase {

    private static final String INDEX_NAME = "test_idx_1";
    private static final String REPO_NAME = "test_repo_1";
    private static final String SNAP_NAME = "test_snap_1";

    private static final int MIN_DOC_COUNT = 500;
    private static final int MAX_DOC_COUNT = 1000;
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 0;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.addAll(super.nodePlugins());
        plugins.addAll(Arrays.asList(
            MockTransportService.TestPlugin.class,
            MockFSIndexStore.TestPlugin.class,
            RecoverySettingsChunkSizePlugin.class,
            TestAnalysisPlugin.class,
            InternalSettingsPlugin.class,
            MockEngineFactoryPlugin.class));
        return plugins;
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        cluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        cluster().assertSeqNos();
        cluster().assertSameDocIdsOnShards();
    }

    private void assertRecoveryStateWithoutStage(RecoveryState state, int shardId, RecoverySource recoverySource, boolean primary,
                                                 String sourceNode, String targetNode) {
        assertThat(state.getShardId().id()).isEqualTo(shardId);
        assertThat(state.getRecoverySource()).isEqualTo(recoverySource);
        assertThat(state.getPrimary()).isEqualTo(primary);
        if (sourceNode == null) {
            assertNull(state.getSourceNode());
        } else {
            assertNotNull(state.getSourceNode());
            assertThat(state.getSourceNode().getName()).isEqualTo(sourceNode);
        }
        if (targetNode == null) {
            assertNull(state.getTargetNode());
        } else {
            assertNotNull(state.getTargetNode());
            assertThat(state.getTargetNode().getName()).isEqualTo(targetNode);
        }
    }

    private void assertRecoveryState(RecoveryState state, int shardId, RecoverySource type, boolean primary, RecoveryState.Stage stage,
                                     String sourceNode, String targetNode) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage()).isEqualTo(stage);
    }

    private void assertOnGoingRecoveryState(RecoveryState state, int shardId, RecoverySource type, boolean primary,
                                            String sourceNode, String targetNode) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage()).isNotEqualTo(RecoveryState.Stage.DONE);
    }

    private void slowDownRecovery(long shardSizeBytes) {
        long chunkSize = Math.max(1, shardSizeBytes / 10);
        var response = FutureUtils.get(client().admin().cluster().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest()
                .transientSettings(Settings.builder()
                    // one chunk per sec..
                    .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), chunkSize, ByteSizeUnit.BYTES)
                    // small chunks
                    .put(CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunkSize, ByteSizeUnit.BYTES))
                )
            ));
        assertThat(response.isAcknowledged()).isTrue();
    }

    private void restoreRecoverySpeed() {
        var response = FutureUtils.get(client().admin().cluster().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest()
                .transientSettings(Settings.builder()
                    .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "20mb")
                    .put(CHUNK_SIZE_SETTING.getKey(), RecoverySettings.DEFAULT_CHUNK_SIZE)
                )
        ));
        assertThat(response.isAcknowledged()).isTrue();
    }

    private List<RecoveryState> findRecoveriesForTargetNode(String nodeName, List<RecoveryState> recoveryStates) {
        List<RecoveryState> nodeResponses = new ArrayList<>();
        for (RecoveryState recoveryState : recoveryStates) {
            if (recoveryState.getTargetNode().getName().equals(nodeName)) {
                nodeResponses.add(recoveryState);
            }
        }
        return nodeResponses;
    }

    private void createAndPopulateIndex(String name, int nodeCount, int shardCount, int replicaCount) {

        logger.info("--> creating test index: {}", name);
        execute("CREATE TABLE " + name + " (foo_int INT, foo_string TEXT, foo_float FLOAT) " +
                " CLUSTERED INTO " + shardCount + " SHARDS WITH (number_of_replicas=" + replicaCount + "," +
                " \"store.stats_refresh_interval\"=0)");
        ensureGreen();

        logger.info("--> indexing sample data");
        final int numDocs = between(MIN_DOC_COUNT, MAX_DOC_COUNT);
        final Object[][] docs = new Object[numDocs][];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = new Object[]{
                randomInt(),
                randomAlphaOfLength(32),
                randomFloat()
            };
        }

        execute("INSERT INTO " + name + " (foo_int, foo_string, foo_float) VALUES (?, ?, ?)", docs);
        execute("REFRESH TABLE " + name);
        execute("OPTIMIZE TABLE " + name);
        execute("SELECT COUNT(*) FROM " + name);
        assertThat(response).hasRows(new Object[] { (long) numDocs });
    }

    private void validateIndexRecoveryState(RecoveryState.Index indexState) {
        assertThat(indexState.time()).isGreaterThanOrEqualTo(0L);
        assertThat(indexState.recoveredFilesPercent()).isGreaterThanOrEqualTo(0.0f);
        assertThat(indexState.recoveredFilesPercent()).isLessThanOrEqualTo(100.0f);
        assertThat(indexState.recoveredBytesPercent()).isGreaterThanOrEqualTo(0.0f);
        assertThat(indexState.recoveredBytesPercent()).isLessThanOrEqualTo(100.0f);
    }

    @Test
    public void testGatewayRecovery() throws Exception {
        logger.info("--> start nodes");
        String node = cluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        cluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        var indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null);
        RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(indexName)).get();
        assertThat(response.shardRecoveryStates()).hasSize(SHARD_COUNT);
        assertThat(response.shardRecoveryStates().get(indexName)).hasSize(1);

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(indexName);
        assertThat(recoveryStates).hasSize(1);

        RecoveryState recoveryState = recoveryStates.get(0);

        assertRecoveryState(recoveryState, 0, RecoverySource.ExistingStoreRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, null, node);

        validateIndexRecoveryState(recoveryState.getIndex());
    }

    @Test
    public void testGatewayRecoveryTestActiveOnly() throws Exception {
        logger.info("--> start nodes");
        cluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        cluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        execute("SELECT * FROM sys.shards WHERE table_name = '" + INDEX_NAME + "'" +
                " AND recovery['stage'] != 'DONE'");
        assertThat(response).hasRowCount(0L);  // Should not expect any responses back
    }

    @Test
    public void testReplicaRecovery() throws Exception {
        final String nodeA = cluster().startNode();

        execute("CREATE TABLE " + INDEX_NAME + " (id BIGINT, data TEXT) " +
                " CLUSTERED INTO " + SHARD_COUNT + " SHARDS WITH (number_of_replicas=" + REPLICA_COUNT + ")");
        ensureGreen();

        final int numOfDocs = scaledRandomIntBetween(1, 200);
        try (BackgroundIndexer indexer = new BackgroundIndexer(
            sqlExecutor.getCurrentSchema(),
            IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null),
            "data",
            sqlExecutor.jdbcUrl(),
            numOfDocs,
            scaledRandomIntBetween(2, 5),
            true,
            null)) {
            waitForDocs(numOfDocs, indexer, sqlExecutor);
        }

        refresh();
        execute("SELECT COUNT(*) FROM " + INDEX_NAME);
        assertThat(response).hasRows(new Object[] { (long) numOfDocs });

        // force a shard recovery from nodeA to nodeB
        final String nodeB = cluster().startNode();
        execute("ALTER TABLE " + INDEX_NAME + " SET (number_of_replicas=1)");
        ensureGreen();


        // we should now have two total shards, one primary and one replica
        execute("SELECT * FROM sys.shards WHERE table_name = '" + INDEX_NAME + "'");
        assertThat(response).hasRowCount(2L);

        var indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null);
        final RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(indexName)).get();

        // we should now have two total shards, one primary and one replica
        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(indexName);
        assertThat(recoveryStates).hasSize(2);

        List<RecoveryState> nodeAResponses = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeAResponses).hasSize(1);
        List<RecoveryState> nodeBResponses = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBResponses).hasSize(1);

        // validate node A recovery
        final RecoveryState nodeARecoveryState = nodeAResponses.get(0);
        final RecoverySource expectedRecoverySource;
        expectedRecoverySource = RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        assertRecoveryState(nodeARecoveryState, 0, expectedRecoverySource, true, RecoveryState.Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryState.getIndex());

        // validate node B recovery
        final RecoveryState nodeBRecoveryState = nodeBResponses.get(0);
        assertRecoveryState(nodeBRecoveryState, 0, RecoverySource.PeerRecoverySource.INSTANCE, false, RecoveryState.Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryState.getIndex());

        cluster().stopRandomNode(TestCluster.nameFilter(nodeA));

        var res = execute("SELECT COUNT(*) FROM " + INDEX_NAME);
        assertThat(res).hasRows(new Object[] { (long) numOfDocs });
    }

    @Test
    public void testCancelNewShardRecoveryAndUsesExistingShardCopy() throws Exception {
        logger.info("--> start node A");
        final String nodeA = cluster().startNode();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> start node B");
        // force a shard recovery from nodeA to nodeB
        final String nodeB = cluster().startNode();

        logger.info("--> add replica for {} on node: {}", INDEX_NAME, nodeB);
        execute("ALTER TABLE " + INDEX_NAME + " SET (number_of_replicas=1, \"unassigned.node_left.delayed_timeout\"=0)");
        ensureGreen();

        logger.info("--> start node C");
        final String nodeC = cluster().startNode();

        // do sync flush to gen sync id
        execute("OPTIMIZE TABLE " + INDEX_NAME);
        //assertThat(client().admin().indices().prepareSyncedFlush(INDEX_NAME).get().failedShards()).isEqualTo(0));

        // hold peer recovery on phase 2 after nodeB down
        CountDownLatch phase1ReadyBlocked = new CountDownLatch(1);
        CountDownLatch allowToCompletePhase1Latch = new CountDownLatch(1);
        MockTransportService transportService = (MockTransportService) cluster().getInstance(TransportService.class, nodeA);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.CLEAN_FILES.equals(action)) {
                phase1ReadyBlocked.countDown();
                try {
                    allowToCompletePhase1Latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        logger.info("--> restart node B");
        cluster().restartNode(
            nodeB,
            new TestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    phase1ReadyBlocked.await();
                    // nodeB stopped, peer recovery from nodeA to nodeC, it will be cancelled after nodeB get started.
                    var indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null);
                    RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(indexName)).get();
                    List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(indexName);
                    List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
                    assertThat(nodeCRecoveryStates).hasSize(1);

                    assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE,
                                               false, nodeA, nodeC);
                    validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());

                    return super.onNodeStopped(nodeName);
                }
            });

        // wait for peer recovery from nodeA to nodeB which is a no-op recovery so it skips the CLEAN_FILES stage and hence is not blocked
        ensureGreen();
        allowToCompletePhase1Latch.countDown();
        transportService.clearAllRules();

        // make sure nodeA has primary and nodeB has replica
        ClusterState state = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        List<ShardRouting> startedShards = state.routingTable().shardsWithState(ShardRoutingState.STARTED);
        assertThat(startedShards).hasSize(2);
        for (ShardRouting shardRouting : startedShards) {
            if (shardRouting.primary()) {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName()).isEqualTo(nodeA);
            } else {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName()).isEqualTo(nodeB);
            }
        }
    }

    @Test
    public void testRerouteRecovery() throws Exception {
        logger.info("--> start node A");
        final String nodeA = cluster().startNode();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);
        execute("SELECT size FROM sys.shards WHERE table_name = '" + INDEX_NAME + "' AND primary=true");
        long shardSize = (long) response.rows()[0][0];

        logger.info("--> start node B");
        final String nodeB = cluster().startNode();

        ensureGreen();

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move shard from: {} to: {}", nodeA, nodeB);
        execute("ALTER TABLE " + INDEX_NAME + " REROUTE MOVE SHARD 0 FROM '" + nodeA + "' TO '" + nodeB + "'");

        logger.info("--> waiting for recovery to start both on source and target");
        final Index index = resolveIndex(IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null));
        assertBusy(() -> {
            IndicesService indicesService = cluster().getInstance(IndicesService.class, nodeA);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource())
                .isEqualTo(1);
            indicesService = cluster().getInstance(IndicesService.class, nodeB);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget())
                .isEqualTo(1);
        });

        logger.info("--> request recoveries");
        RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index.getName())).get();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(index.getName());
        List<RecoveryState> nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates).hasSize(1);
        List<RecoveryState> nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates).hasSize(1);

        assertRecoveryState(nodeARecoveryStates.get(0), 0, RecoverySource.EmptyStoreRecoverySource.INSTANCE, true,
                            RecoveryState.Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertOnGoingRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        logger.info("--> request node recovery stats");

        IndicesService indicesServiceNodeA = cluster().getInstance(IndicesService.class, nodeA);
        var recoveryStatsNodeA = indicesServiceNodeA.indexServiceSafe(index).getShard(0).recoveryStats();
        assertThat(recoveryStatsNodeA.currentAsSource())
            .as("node A should have ongoing recovery as source")
            .isEqualTo(1);
        assertThat(recoveryStatsNodeA.currentAsTarget())
            .as("node A should not have ongoing recovery as target")
            .isEqualTo(0);

        IndicesService indicesServiceNodeB = cluster().getInstance(IndicesService.class, nodeB);
        var recoveryStatsNodeB = indicesServiceNodeB.indexServiceSafe(index).getShard(0).recoveryStats();
        assertThat(recoveryStatsNodeB.currentAsSource())
            .as("node B should not have ongoing recovery as source")
            .isEqualTo(0);
        assertThat( recoveryStatsNodeB.currentAsTarget())
            .as("node B should have ongoing recovery as target")
            .isEqualTo(1);

        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();

        // wait for it to be finished
        ensureGreen(index.getName());

        response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index.getName())).get();
        recoveryStates = response.shardRecoveryStates().get(index.getName());
        assertThat(recoveryStates).hasSize(1);

        assertRecoveryState(recoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(recoveryStates.get(0).getIndex());
        Consumer<String> assertNodeHasThrottleTimeAndNoRecoveries = nodeName ->  {
            IndicesService indicesService = cluster().getInstance(IndicesService.class, nodeName);
            var recoveryStats = indicesService.indexServiceSafe(index).getShard(0).recoveryStats();
            assertThat(recoveryStats.currentAsSource()).isEqualTo(0);
            assertThat(recoveryStats.currentAsTarget()).isEqualTo(0);
        };
        // we have to use assertBusy as recovery counters are decremented only when the last reference to the RecoveryTarget
        // is decremented, which may happen after the recovery was done.

        // CrateDB does not expose the RecoveryStats via an API, it can only be retrieved by the IndicesService.
        // NodeA does not hold the index anymore, such resolving RecoveryStats via NodeA is not possible.
        //assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeA));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeB));

        logger.info("--> bump replica count");
        execute("ALTER TABLE " + INDEX_NAME + " SET (number_of_replicas=1)");
        ensureGreen();

        // TODO: NodeA should now contain the replica shards, thus resolving RecoveryStats via the IndicesService
        //  is possible again. BUT checked throttle time never increased on target NodeA, too fast?
        //assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeA));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeB));

        logger.info("--> start node C");
        String nodeC = cluster().startNode();
        assertThat(client().admin().cluster().health(new ClusterHealthRequest().waitForNodes("3")).get().isTimedOut()).isFalse();

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move replica shard from: {} to: {}", nodeA, nodeC);
        execute("ALTER TABLE " + INDEX_NAME + " REROUTE MOVE SHARD 0 FROM '" + nodeA + "' TO '" + nodeC + "'");

        response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index.getName())).get();
        recoveryStates = response.shardRecoveryStates().get(index.getName());

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates).hasSize(1);
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates).hasSize(1);
        List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates).hasSize(1);

        assertRecoveryState(nodeARecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, RecoveryState.Stage.DONE, nodeB, nodeA);
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());

        if (randomBoolean()) {
            // shutdown node with relocation source of replica shard and check if recovery continues
            cluster().stopRandomNode(TestCluster.nameFilter(nodeA));
            ensureStableCluster(2);

            response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index.getName())).get();
            recoveryStates = response.shardRecoveryStates().get(index.getName());

            nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
            assertThat(nodeARecoveryStates).hasSize(0);
            nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
            assertThat(nodeBRecoveryStates).hasSize(1);
            nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
            assertThat(nodeCRecoveryStates).hasSize(1);

            assertRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, nodeA, nodeB);
            validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

            assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, nodeB, nodeC);
            validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
        }

        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();
        ensureGreen();

        response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index.getName())).get();
        recoveryStates = response.shardRecoveryStates().get(index.getName());

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates).hasSize(0);
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates).hasSize(1);
        nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates).hasSize(1);

        assertRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, RecoveryState.Stage.DONE, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
    }

    @Test
    public void testSnapshotRecovery() throws Exception {
        logger.info("--> start node A");
        String nodeA = cluster().startNode();

        logger.info("--> create repository");
        execute("CREATE REPOSITORY " + REPO_NAME + " TYPE FS WITH (location = '" + randomRepoPath() + "', compress=false)");

        ensureGreen();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> snapshot");
        var snapshotName = REPO_NAME + "." + SNAP_NAME;
        execute("CREATE SNAPSHOT " + snapshotName + " ALL WITH (wait_for_completion=true)");

        execute("SELECT state FROM sys.snapshots WHERE name = '" + SNAP_NAME + "'");
        assertThat(response).hasRows("SUCCESS");

        execute("ALTER TABLE " + INDEX_NAME + " CLOSE");

        logger.info("--> restore");
        execute("RESTORE SNAPSHOT " + snapshotName + " ALL WITH (wait_for_completion=true)");

        ensureGreen();

        execute("SELECT id from sys.snapshots WHERE repository = ? AND name = ?", new Object[]{REPO_NAME, SNAP_NAME});

        String uuid = (String) response.rows()[0][0];
        SnapshotId snapshotId = new SnapshotId(SNAP_NAME, uuid);

        logger.info("--> request recoveries");
        var indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null);
        RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(indexName)).get();

        Repository repository = cluster().getMasterNodeInstance(RepositoriesService.class).repository(REPO_NAME);
        RepositoryData repositoryData = repository.getRepositoryData().get(5, TimeUnit.SECONDS);
        for (Map.Entry<String, List<RecoveryState>> indexRecoveryStates : response.shardRecoveryStates().entrySet()) {

            assertThat(indexRecoveryStates.getKey()).isEqualTo(indexName);
            List<RecoveryState> recoveryStates = indexRecoveryStates.getValue();
            assertThat(recoveryStates).hasSize(SHARD_COUNT);

            for (RecoveryState recoveryState : recoveryStates) {
                RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(
                    ((RecoverySource.SnapshotRecoverySource)recoveryState.getRecoverySource()).restoreUUID(),
                    new Snapshot(REPO_NAME, snapshotId),
                    Version.CURRENT, repositoryData.resolveIndexId(indexName));
                assertRecoveryState(recoveryState, 0, recoverySource, true, RecoveryState.Stage.DONE, null, nodeA);
                validateIndexRecoveryState(recoveryState.getIndex());
            }
        }
    }

    @Test
    public void testTransientErrorsDuringRecoveryAreRetried() throws Exception {
        final String indexName = "test";

        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "500ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "10s")
            .build();
        // start a master node
        cluster().startNode(nodeSettings);

        final String blueNodeName = cluster()
            .startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = cluster()
            .startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().health(new ClusterHealthRequest().waitForNodes(">=3")).get();
        assertThat(response.isTimedOut()).isFalse();

        execute("CREATE TABLE doc." + indexName + " (id int) CLUSTERED INTO 1 SHARDS " +
                "WITH (" +
                " number_of_replicas=0," +
                " \"routing.allocation.include.color\" = 'blue'" +
                ")");

        int numDocs = scaledRandomIntBetween(100, 8000);
        // Index 3/4 of the documents and flush. And then index the rest. This attempts to ensure that there
        // is a mix of file chunks and translog ops
        int threeFourths = (int) (numDocs * 0.75);
        var args = new Object[threeFourths][];
        for (int i = 0; i < threeFourths; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc." + indexName + " (id) VALUES (?)", args);
        execute("OPTIMIZE TABLE doc." + indexName);

        args = new Object[numDocs - threeFourths][];
        for (int i = 0; i < numDocs-threeFourths; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc." + indexName + " (id) VALUES (?)", args);
        ensureGreen();

        ClusterStateResponse stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get();
        final String blueNodeId = cluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertThat(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty()).isFalse();

        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM doc." + indexName);
        assertThat(searchResponse).hasRows(new Object[] { (long) numDocs });

        String[] recoveryActions = new String[]{
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.TRANSLOG_OPS,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            PeerRecoveryTargetService.Actions.FINALIZE
        };
        final String recoveryActionToBlock = randomFrom(recoveryActions);
        logger.info("--> will temporarily interrupt recovery action between blue & red on [{}]", recoveryActionToBlock);

        MockTransportService blueTransportService =
            (MockTransportService) cluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redTransportService =
            (MockTransportService) cluster().getInstance(TransportService.class, redNodeName);

        Runnable connectionBreaker = () -> {
            // Always break connection from source to remote to ensure that actions are retried
            blueTransportService.disconnectFromNode(redTransportService.getLocalDiscoNode());
            if (randomBoolean()) {
                // Sometimes break connection from remote to source to ensure that recovery is re-established
                redTransportService.disconnectFromNode(blueTransportService.getLocalDiscoNode());
            }
        };
        TransientReceiveRejected handlingBehavior = new TransientReceiveRejected(recoveryActionToBlock, connectionBreaker);
        redTransportService.addRequestHandlingBehavior(recoveryActionToBlock, handlingBehavior);

        try {
            logger.info("--> starting recovery from blue to red");
            execute("ALTER TABLE doc." + indexName + " SET (" +
                    " number_of_replicas=1," +
                    " \"routing.allocation.include.color\" = 'red,blue'" +
                    ")");

            ensureGreen();
            var nodeRedExecutor = executor(redNodeName);
            searchResponse = nodeRedExecutor.exec("SELECT COUNT(*) FROM doc." + indexName);
            assertThat(searchResponse).hasRows(new Object[] { (long) numDocs });
        } finally {
            blueTransportService.clearAllRules();
            redTransportService.clearAllRules();
        }
    }

    private class TransientReceiveRejected implements StubbableTransport.RequestHandlingBehavior<TransportRequest> {

        private final String actionName;
        private final Runnable connectionBreaker;
        private final AtomicInteger blocksRemaining;

        private TransientReceiveRejected(String actionName,
                                         Runnable connectionBreaker) {
            this.actionName = actionName;
            this.connectionBreaker = connectionBreaker;
            this.blocksRemaining = new AtomicInteger(randomIntBetween(1, 3));
        }

        @Override
        public void messageReceived(TransportRequestHandler<TransportRequest> handler,
                                    TransportRequest request,
                                    TransportChannel channel) throws Exception {
            if (blocksRemaining.getAndUpdate(i -> i == 0 ? 0 : i - 1) != 0) {
                String rejected = "rejected";
                String circuit = "circuit";
                String network = "network";
                String reason = randomFrom(rejected, circuit, network);
                if (reason.equals(rejected)) {
                    logger.info("--> preventing {} response by throwing exception", actionName);
                    throw new EsRejectedExecutionException("foo", false);
                } else if (reason.equals(circuit)) {
                    logger.info("--> preventing {} response by throwing exception", actionName);
                    throw new CircuitBreakingException("Broken");
                } else if (reason.equals(network)) {
                    logger.info("--> preventing {} response by breaking connection", actionName);
                    connectionBreaker.run();
                } else {
                    throw new AssertionError("Unknown failure reason: " + reason);
                }
            }
            handler.messageReceived(request, channel);
        }
    }

    @Test
    public void testDisconnectsWhileRecovering() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "1s")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "1s")
            .build();
        // start a master node
        cluster().startNode(nodeSettings);

        final String blueNodeName = cluster()
            .startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = cluster()
            .startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().health(new ClusterHealthRequest().waitForNodes(">=3")).get();
        assertThat(response.isTimedOut()).isFalse();

        execute("CREATE TABLE doc." + indexName + " (id int) CLUSTERED INTO 1 SHARDS " +
                "WITH (" +
                " number_of_replicas=0," +
                " \"routing.allocation.include.color\" = 'blue'" +
                ")");

        int numDocs = scaledRandomIntBetween(25, 250);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc." + indexName + " (id) VALUES (?)", args);
        ensureGreen();

        ClusterStateResponse stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get();
        final String blueNodeId = cluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertThat(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty()).isFalse();

        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM doc." + indexName);
        assertThat(searchResponse).hasRows(new Object[] { (long) numDocs });

        String[] recoveryActions = new String[]{
            PeerRecoverySourceService.Actions.START_RECOVERY,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            //RecoveryTarget.Actions.TRANSLOG_OPS, <-- may not be sent if already flushed
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.FINALIZE
        };
        final String recoveryActionToBlock = randomFrom(recoveryActions);
        final boolean dropRequests = randomBoolean();
        logger.info("--> will {} between blue & red on [{}]", dropRequests ? "drop requests" : "break connection", recoveryActionToBlock);

        MockTransportService blueMockTransportService =
            (MockTransportService) cluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redMockTransportService =
            (MockTransportService) cluster().getInstance(TransportService.class, redNodeName);
        TransportService redTransportService = cluster().getInstance(TransportService.class, redNodeName);
        TransportService blueTransportService = cluster().getInstance(TransportService.class, blueNodeName);
        final CountDownLatch requestFailed = new CountDownLatch(1);

        if (randomBoolean()) {
            // Fail on the sending side
            blueMockTransportService.addSendBehavior(redTransportService,
                                                     new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, requestFailed));
            redMockTransportService.addSendBehavior(blueTransportService,
                                                    new RecoveryActionBlocker(dropRequests, recoveryActionToBlock, requestFailed));
        } else {
            // Fail on the receiving side.
            blueMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                redMockTransportService.disconnectFromNode(blueMockTransportService.getLocalNode());
                handler.messageReceived(request, channel);
            });
            redMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                blueMockTransportService.disconnectFromNode(redMockTransportService.getLocalNode());
                handler.messageReceived(request, channel);
            });
        }

        logger.info("--> starting recovery from blue to red");
        execute("ALTER TABLE doc." + indexName + " SET (" +
                " number_of_replicas=1," +
                " \"routing.allocation.include.color\" = 'red,blue'" +
                ")");

        requestFailed.await();

        logger.info("--> clearing rules to allow recovery to proceed");
        blueMockTransportService.clearAllRules();
        redMockTransportService.clearAllRules();

        ensureGreen();
        var nodeRedExecutor = executor(redNodeName);
        searchResponse = nodeRedExecutor.exec("SELECT COUNT(*) FROM doc." + indexName);
        assertThat(searchResponse).hasRows(new Object[] { (long) numDocs });
    }

    private class RecoveryActionBlocker implements StubbableTransport.SendRequestBehavior {
        private final boolean dropRequests;
        private final String recoveryActionToBlock;
        private final CountDownLatch requestBlocked;

        RecoveryActionBlocker(boolean dropRequests, String recoveryActionToBlock, CountDownLatch requestBlocked) {
            this.dropRequests = dropRequests;
            this.recoveryActionToBlock = recoveryActionToBlock;
            this.requestBlocked = requestBlocked;
        }

        @Override
        public void sendRequest(Transport.Connection connection, long requestId, String action, TransportRequest request,
                                TransportRequestOptions options) throws IOException {
            if (recoveryActionToBlock.equals(action) || requestBlocked.getCount() == 0) {
                requestBlocked.countDown();
                if (dropRequests) {
                    logger.info("--> preventing {} request by dropping request", action);
                    return;
                } else {
                    logger.info("--> preventing {} request by throwing ConnectTransportException", action);
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        }
    }

    /**
     * Tests scenario where recovery target successfully sends recovery request to source but then the channel gets closed while
     * the source is working on the recovery process.
     */
    @Test
    public void testDisconnectsDuringRecovery() throws Exception {
        boolean primaryRelocation = randomBoolean();
        final String indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), "test", null);
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(),
                 TimeValue.timeValueMillis(randomIntBetween(0, 100)))
            .build();
        TimeValue disconnectAfterDelay = TimeValue.timeValueMillis(randomIntBetween(0, 100));
        // start a master node
        String masterNodeName = cluster().startMasterOnlyNode(nodeSettings);

        final String blueNodeName = cluster()
            .startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = cluster()
            .startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        execute("CREATE TABLE test (id int) CLUSTERED INTO 1 SHARDS " +
                "WITH (" +
                " number_of_replicas=0," +
                " \"routing.allocation.include.color\" = 'blue'" +
                ")");

        int numDocs = scaledRandomIntBetween(25, 250);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO test (id) VALUES (?)", args);
        ensureGreen();

        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM test");
        assertThat(searchResponse).hasRows(new Object[] { (long) numDocs });

        MockTransportService masterTransportService =
            (MockTransportService) cluster().getInstance(TransportService.class, masterNodeName);
        MockTransportService blueMockTransportService =
            (MockTransportService) cluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redMockTransportService =
            (MockTransportService) cluster().getInstance(TransportService.class, redNodeName);

        redMockTransportService.addSendBehavior(blueMockTransportService, new StubbableTransport.SendRequestBehavior() {
            private final AtomicInteger count = new AtomicInteger();

            @Override
            public void sendRequest(Transport.Connection connection, long requestId, String action, TransportRequest request,
                                    TransportRequestOptions options) throws IOException {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action) && count.incrementAndGet() == 1) {
                    // ensures that it's considered as valid recovery attempt by source
                    try {
                        assertBusy(() -> {
                            ClusterStateRequest stateRequest = new ClusterStateRequest().local(true);
                            List<ShardRouting> initializingShards = client(blueNodeName).admin().cluster().state(stateRequest).get()
                                .getState()
                                .routingTable()
                                .index(indexName)
                                .shard(0).getAllInitializingShards();
                            assertThat(initializingShards)
                                .as("Expected there to be some initializing shards")
                                .isNotEmpty();
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    connection.sendRequest(requestId, action, request, options);
                    try {
                        Thread.sleep(disconnectAfterDelay.millis());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new ConnectTransportException(connection.getNode(),
                                                        "DISCONNECT: simulation disconnect after successfully sending " + action + " request");
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            }
        });

        final AtomicBoolean finalized = new AtomicBoolean();
        blueMockTransportService.addSendBehavior(redMockTransportService, (connection, requestId, action, request, options) -> {
            logger.info("--> sending request {} on {}", action, connection.getNode());
            if (action.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                finalized.set(true);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        for (MockTransportService mockTransportService : Arrays.asList(redMockTransportService, blueMockTransportService)) {
            mockTransportService.addSendBehavior(masterTransportService, (connection, requestId, action, request, options) -> {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if ((primaryRelocation && finalized.get()) == false) {
                    assertNotEquals(action, ShardStateAction.SHARD_FAILED_ACTION_NAME);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        if (primaryRelocation) {
            logger.info("--> starting primary relocation recovery from blue to red");
            execute("ALTER TABLE test SET (" +
                    " \"routing.allocation.include.color\" = 'red'" +
                    ")");

            ensureGreen(); // also waits for relocation / recovery to complete
            // if a primary relocation fails after the source shard has been marked as relocated, both source and target are failed. If the
            // source shard is moved back to started because the target fails first, it's possible that there is a cluster state where the
            // shard is marked as started again (and ensureGreen returns), but while applying the cluster state the primary is failed and
            // will be reallocated. The cluster will thus become green, then red, then green again. Triggering a refresh here before
            // searching helps, as in contrast to search actions, refresh waits for the closed shard to be reallocated.
            refresh();
        } else {
            logger.info("--> starting replica recovery from blue to red");
            execute("ALTER TABLE test SET (" +
                    " number_of_replicas=1," +
                    " \"routing.allocation.include.color\" = 'red,blue'" +
                    ")");

            ensureGreen();
        }

        for (int i = 0; i < 10; i++) {
            searchResponse = execute("SELECT COUNT(*) FROM test");
            assertThat(searchResponse).hasRows(new Object[] { (long) numDocs });
        }
    }

    @Test
    public void testHistoryRetention() throws Exception {
        cluster().startNodes(3);

        final String indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), "test", null);
        execute("CREATE TABLE test (id int) CLUSTERED INTO 1 SHARDS " +
                "WITH (" +
                " number_of_replicas=2," +
                " \"recovery.file_based_threshold\" = 1.0" +
                ")");

        // Perform some replicated operations so the replica isn't simply empty, because ops-based recovery isn't better in that case
        int numDocs = scaledRandomIntBetween(25, 250);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO test (id) VALUES (?)", args);
        if (randomBoolean()) {
            execute("OPTIMIZE TABLE test");
        }
        ensureGreen();


        String firstNodeToStop = randomFrom(cluster().getNodeNames());
        Settings firstNodeToStopDataPathSettings = cluster().dataPathSettings(firstNodeToStop);
        cluster().stopRandomNode(TestCluster.nameFilter(firstNodeToStop));
        String secondNodeToStop = randomFrom(cluster().getNodeNames());
        Settings secondNodeToStopDataPathSettings = cluster().dataPathSettings(secondNodeToStop);
        cluster().stopRandomNode(TestCluster.nameFilter(secondNodeToStop));

        final long desyncNanoTime = System.nanoTime();
        //noinspection StatementWithEmptyBody
        while (System.nanoTime() <= desyncNanoTime) {
            // time passes
        }

        final int numNewDocs = scaledRandomIntBetween(25, 250);
        args = new Object[numNewDocs][];
        for (int i = 0; i < numNewDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO test (id) VALUES (?)", args);
        refresh();
        // Flush twice to update the safe commit's local checkpoint
        execute("OPTIMIZE TABLE test");
        execute("OPTIMIZE TABLE test");

        execute("ALTER TABLE test SET (number_of_replicas = 1)");
        cluster().startNode(randomFrom(firstNodeToStopDataPathSettings, secondNodeToStopDataPathSettings));
        ensureGreen(indexName);

        final RecoveryResponse recoveryResponse = client().admin().indices().recoveries(new RecoveryRequest(indexName)).get();
        final List<RecoveryState> recoveryStates = recoveryResponse.shardRecoveryStates().get(indexName);
        recoveryStates.removeIf(r -> r.getTimer().getStartNanoTime() <= desyncNanoTime);

        assertThat(recoveryStates).hasSize(1);
        assertThat(recoveryStates.get(0).getIndex().totalFileCount()).isEqualTo(0);
        assertThat(recoveryStates.get(0).getTranslog().recoveredOperations()).isGreaterThan(0);
    }

    @Test
    public void testDoNotInfinitelyWaitForMapping() {
        cluster().ensureAtLeastNumDataNodes(3);
        execute("CREATE ANALYZER test_analyzer (" +
                " TOKENIZER standard," +
                " TOKEN_FILTERS (test_token_filter)" +
                ")");
        execute("CREATE TABLE test (test_field TEXT INDEX USING FULLTEXT WITH (ANALYZER='test_analyzer'))" +
                " CLUSTERED INTO 1 SHARDS WITH (number_of_replicas = 0)");

        int numDocs = between(1, 10);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{Integer.toString(i)};
        }
        execute("INSERT INTO test (test_field) VALUES (?)", args);

        Semaphore recoveryBlocked = new Semaphore(1);
        for (DiscoveryNode node : clusterService().state().nodes()) {
            MockTransportService transportService = (MockTransportService) cluster().getInstance(
                TransportService.class, node.getName());
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                    if (recoveryBlocked.tryAcquire()) {
                        PluginsService pluginService = cluster().getInstance(PluginsService.class, node.getName());
                        for (TestAnalysisPlugin plugin : pluginService.filterPlugins(TestAnalysisPlugin.class)) {
                            plugin.throwParsingError.set(true);
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        execute("ALTER TABLE test SET (number_of_replicas = 1)");
        ensureGreen();
        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM test");
        assertThat(searchResponse).hasRows(new Object[] { (long) numDocs });
    }

    /** Makes sure the new master does not repeatedly fetch index metadata from recovering replicas */
    @Test
    public void testOngoingRecoveryAndMasterFailOver() throws Exception {
        cluster().startNodes(2);
        String nodeWithPrimary = cluster().startDataOnlyNode();
        execute("CREATE TABLE doc.test (id INT)" +
                " CLUSTERED INTO 1 SHARDS" +
                " WITH (number_of_replicas = 0, \"routing.allocation.include._name\" = '" + nodeWithPrimary + "')");
        MockTransportService transport = (MockTransportService) cluster().getInstance(TransportService.class, nodeWithPrimary);
        CountDownLatch phase1ReadyBlocked = new CountDownLatch(1);
        CountDownLatch allowToCompletePhase1Latch = new CountDownLatch(1);
        Semaphore blockRecovery = new Semaphore(1);
        transport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.CLEAN_FILES.equals(action) && blockRecovery.tryAcquire()) {
                phase1ReadyBlocked.countDown();
                try {
                    allowToCompletePhase1Latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        try {
            String nodeWithReplica = cluster().startDataOnlyNode();
            execute("ALTER TABLE doc.test SET (number_of_replicas = 1," +
                    " \"routing.allocation.include._name\"='" + nodeWithPrimary + "," + nodeWithReplica + "')");
            phase1ReadyBlocked.await();
            cluster().restartNode(clusterService().state().nodes().getMasterNode().getName(),
                                          new TestCluster.RestartCallback());
            cluster().ensureAtLeastNumDataNodes(3);
            execute("ALTER TABLE doc.test RESET (\"routing.allocation.include._name\")");
            execute("ALTER TABLE doc.test SET (number_of_replicas = 2)");
            assertThat(client().admin().cluster().health(new ClusterHealthRequest("test").waitForActiveShards(2)).get().isTimedOut()).isFalse();
        } finally {
            allowToCompletePhase1Latch.countDown();
        }
        ensureGreen();
    }

    @Test
    public void testRecoveryFlushReplica() throws Exception {
        cluster().ensureAtLeastNumDataNodes(3);
        String indexName = "test";
        execute("CREATE TABLE doc.test (num INT)" +
                " CLUSTERED INTO 1 SHARDS" +
                " WITH (number_of_replicas = 0)");
        int numDocs = randomIntBetween(1, 10);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc.test (num) VALUES (?)", args);
        execute("ALTER TABLE doc.test SET (number_of_replicas = 1)");
        ensureGreen();

        ShardId shardId = null;
        var indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
        for (ShardStats shardStats : indicesStats.getIndex(indexName).getShards()) {
            shardId = shardStats.getShardRouting().shardId();
            if (shardStats.getShardRouting().primary() == false) {
                assertThat(shardStats.getCommitStats().getNumDocs()).isEqualTo(numDocs);
                SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
                    shardStats.getCommitStats().getUserData().entrySet());
                assertThat(commitInfo.localCheckpoint).isEqualTo(shardStats.getSeqNoStats().getLocalCheckpoint());
                assertThat(commitInfo.maxSeqNo).isEqualTo(shardStats.getSeqNoStats().getMaxSeqNo());
            }
        }
        SyncedFlushUtil.attemptSyncedFlush(logger, cluster(), shardId);
        assertBusy(() -> assertThat(client().execute(SyncedFlushAction.INSTANCE, new SyncedFlushRequest(indexName)).get().failedShards()).isEqualTo(0));
        execute("ALTER TABLE doc.test SET (number_of_replicas = 2)");
        ensureGreen(indexName);
        // Recovery should keep syncId if no indexing activity on the primary after synced-flush.
        indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
        Set<String> syncIds = Stream.of(indicesStats.getIndex(indexName).getShards())
            .map(shardStats -> shardStats.getCommitStats().syncId())
            .collect(Collectors.toSet());
        assertThat(syncIds).hasSize(1);
    }

    @Test
    public void testRecoveryUsingSyncedFlushWithoutRetentionLease() throws Exception {
        cluster().ensureAtLeastNumDataNodes(2);
        String indexName = "test";
        execute("CREATE TABLE doc.test (num INT)" +
                " CLUSTERED INTO 1 SHARDS" +
                " WITH (" +
                "  number_of_replicas = 1," +
                "  \"unassigned.node_left.delayed_timeout\"='24h'," +
                "  \"soft_deletes.retention_lease.period\"='100ms'," +
                "  \"soft_deletes.retention_lease.sync_interval\"='100ms'" +
                " )");
        int numDocs = randomIntBetween(1, 10);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc.test (num) VALUES (?)", args);
        ensureGreen(indexName);

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        assertThat(SyncedFlushUtil.attemptSyncedFlush(logger, cluster(), shardId).successfulShards()).isEqualTo(2);

        final ClusterState clusterState = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        final ShardRouting shardToResync = randomFrom(clusterState.routingTable().shardRoutingTable(shardId).activeShards());
        cluster().restartNode(
            clusterState.nodes().get(shardToResync.currentNodeId()).getName(),
            new TestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertBusy(() -> {
                        var indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
                        assertThat(indicesStats.getShards()[0].getRetentionLeaseStats().leases().contains(
                            ReplicationTracker.getPeerRecoveryRetentionLeaseId(shardToResync))).isFalse();
                    });
                    return super.onNodeStopped(nodeName);
                }
            });

        ensureGreen(indexName);
    }

    @Test
    public void testRecoverLocallyUpToGlobalCheckpoint() throws Exception {
        cluster().ensureAtLeastNumDataNodes(2);
        List<String> nodes = randomSubsetOf(2, StreamSupport.stream(clusterService().state().nodes().getDataNodes().spliterator(), false)
            .map(node -> node.value.getName()).collect(Collectors.toSet()));
        String indexName = "test";
        execute("CREATE TABLE doc.test (num INT)" +
                " CLUSTERED INTO 1 SHARDS" +
                " WITH (" +
                "  number_of_replicas = 1," +
                "  \"global_checkpoint_sync.interval\"='24h'," +
                "  \"routing.allocation.include._name\"='" + String.join(",", nodes) + "'" +
                " )");
        ensureGreen(indexName);
        int numDocs = randomIntBetween(1, 100);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc.test (num) VALUES (?)", args);
        refresh(indexName);
        String failingNode = randomFrom(nodes);
        PlainActionFuture<StartRecoveryRequest> startRecoveryRequestFuture = new PlainActionFuture<>();
        // Peer recovery fails if the primary does not see the recovering replica in the replication group (when the cluster state
        // update on the primary is delayed). To verify the local recovery stats, we have to manually remember this value in the
        // first try because the local recovery happens once and its stats is reset when the recovery fails.
        SetOnce<Integer> localRecoveredOps = new SetOnce<>();
        for (String node : nodes) {
            MockTransportService transportService = (MockTransportService) cluster().getInstance(TransportService.class, node);
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                    final RecoveryState recoveryState = cluster().getInstance(IndicesService.class, failingNode)
                        .getShardOrNull(new ShardId(resolveIndex(indexName), 0)).recoveryState();
                    assertThat(recoveryState.getTranslog().recoveredOperations()).isEqualTo(recoveryState.getTranslog().totalLocal());
                    if (startRecoveryRequestFuture.isDone()) {
                        assertThat(recoveryState.getTranslog().totalLocal()).isEqualTo(0);
                        recoveryState.getTranslog().totalLocal(localRecoveredOps.get());
                        recoveryState.getTranslog().incrementRecoveredOperations(localRecoveredOps.get());
                    } else {
                        localRecoveredOps.set(recoveryState.getTranslog().totalLocal());
                        startRecoveryRequestFuture.onResponse((StartRecoveryRequest) request);
                    }
                }
                if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                    RetentionLeases retentionLeases = cluster().getInstance(IndicesService.class, node)
                        .indexServiceSafe(resolveIndex(indexName))
                        .getShard(0).getRetentionLeases();
                    throw new AssertionError("expect an operation-based recovery:" +
                                             "retention leases" + Strings.toString(retentionLeases) + "]");
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        IndexShard shard = cluster().getInstance(IndicesService.class, failingNode)
            .getShardOrNull(new ShardId(resolveIndex(indexName), 0));
        final long lastSyncedGlobalCheckpoint = shard.getLastSyncedGlobalCheckpoint();
        final long localCheckpointOfSafeCommit;
        try(Engine.IndexCommitRef safeCommitRef = shard.acquireSafeIndexCommit()){
            localCheckpointOfSafeCommit =
                SequenceNumbers.loadSeqNoInfoFromLuceneCommit(safeCommitRef.getIndexCommit().getUserData().entrySet()).localCheckpoint;
        }
        final long maxSeqNo = shard.seqNoStats().getMaxSeqNo();
        shard.failShard("test", new IOException("simulated"));
        StartRecoveryRequest startRecoveryRequest = startRecoveryRequestFuture.get();
        logger.info("--> start recovery request: starting seq_no {}, commit {}", startRecoveryRequest.startingSeqNo(),
                    startRecoveryRequest.metadataSnapshot().getCommitUserData());
        SequenceNumbers.CommitInfo commitInfoAfterLocalRecovery = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
            startRecoveryRequest.metadataSnapshot().getCommitUserData().entrySet());
        assertThat(commitInfoAfterLocalRecovery.localCheckpoint).isEqualTo(lastSyncedGlobalCheckpoint);
        assertThat(commitInfoAfterLocalRecovery.maxSeqNo).isEqualTo(lastSyncedGlobalCheckpoint);
        assertThat(startRecoveryRequest.startingSeqNo()).isEqualTo(lastSyncedGlobalCheckpoint + 1);
        ensureGreen(indexName);
        assertThat((long) localRecoveredOps.get()).isEqualTo(lastSyncedGlobalCheckpoint - localCheckpointOfSafeCommit);
        for (var recoveryState : client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).get().shardRecoveryStates().get(indexName)) {
            if (startRecoveryRequest.targetNode().equals(recoveryState.getTargetNode())) {
                assertThat(recoveryState.getIndex().fileDetails().values())
                    .as("expect an operation-based recovery")
                    .isEmpty();
                assertThat(recoveryState.getTranslog().recoveredOperations())
                    .as("total recovered translog operations must include both local and remote recovery")
                    .isGreaterThanOrEqualTo(Math.toIntExact(maxSeqNo - localCheckpointOfSafeCommit));
            }
        }
        for (String node : nodes) {
            MockTransportService transportService = (MockTransportService) cluster().getInstance(TransportService.class, node);
            transportService.clearAllRules();
        }
    }

    @Test
    public void testUsesFileBasedRecoveryIfRetentionLeaseMissing() throws Exception {
        cluster().ensureAtLeastNumDataNodes(2);

        String indexName = "test";
        execute("CREATE TABLE doc.test (num INT)" +
                " CLUSTERED INTO 1 SHARDS" +
                " WITH (" +
                "  number_of_replicas = 1," +
                "  \"unassigned.node_left.delayed_timeout\"='12h'," +
                "  \"soft_deletes.enabled\"=true" +
                " )");
        int numDocs = randomIntBetween(1, 100);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc.test (num) VALUES (?)", args);
        ensureGreen(indexName);

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final IndexShard primary = cluster().getInstance(IndicesService.class,
                                                                 discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()).getShardOrNull(shardId);

        final ShardRouting replicaShardRouting = indexShardRoutingTable.replicaShards().get(0);
        cluster().restartNode(discoveryNodes.get(replicaShardRouting.currentNodeId()).getName(),
                                      new TestCluster.RestartCallback() {
                                          @Override
                                          public Settings onNodeStopped(String nodeName) throws Exception {
                                              assertThat(
                                                    client().admin().cluster().health(
                                                        new ClusterHealthRequest()
                                                            .waitForNodes(Integer.toString(discoveryNodes.getSize() - 1))
                                                            .waitForEvents(Priority.LANGUID)
                                                    ).get().isTimedOut()).isFalse();

                                              final PlainActionFuture<ReplicationResponse> future = new PlainActionFuture<>();
                                              primary.removeRetentionLease(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting), future);
                                              future.get();

                                              return super.onNodeStopped(nodeName);
                                          }
                                      });

        ensureGreen(indexName);

        //noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final var recoveryState = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).get()
            .shardRecoveryStates().get(indexName).stream().filter(rs -> rs.getPrimary() == false).findFirst().get();
        assertThat(recoveryState.getIndex().totalFileCount()).isGreaterThan(0);
    }

    @Test
    public void testUsesFileBasedRecoveryIfRetentionLeaseAheadOfGlobalCheckpoint() throws Exception {
        cluster().ensureAtLeastNumDataNodes(2);

        String indexName = "test";
        execute("CREATE TABLE doc.test (num INT)" +
                " CLUSTERED INTO 1 SHARDS" +
                " WITH (" +
                "  number_of_replicas = 1," +
                "  \"unassigned.node_left.delayed_timeout\"='12h'," +
                "  \"soft_deletes.enabled\"=true" +
                " )");
        int numDocs = randomIntBetween(1, 100);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc.test (num) VALUES (?)", args);
        ensureGreen(indexName);

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final IndexShard primary = cluster().getInstance(IndicesService.class,
                                                                 discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()).getShardOrNull(shardId);

        final ShardRouting replicaShardRouting = indexShardRoutingTable.replicaShards().get(0);
        cluster().restartNode(
            discoveryNodes.get(replicaShardRouting.currentNodeId()).getName(),
            new TestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertThat(
                        client().admin().cluster().health(
                            new ClusterHealthRequest()
                                .waitForNodes(Integer.toString(discoveryNodes.getSize() - 1))
                                .waitForEvents(Priority.LANGUID)
                            ).get().isTimedOut()
                    ).isFalse();

                    execute("INSERT INTO doc.test (num) VALUES (?)", args);

                    // We do not guarantee that the replica can recover locally all the way to its
                    // own global checkpoint before starting
                    // to recover from the primary, so we must be careful not to perform an
                    // operations-based recovery if this would require
                    // some operations that are not being retained. Emulate this by advancing the
                    // lease ahead of the replica's GCP:
                    primary.renewRetentionLease(
                            ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting),
                            primary.seqNoStats().getMaxSeqNo() + 1,
                            ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE);

                    return super.onNodeStopped(nodeName);
                }
            });

        ensureGreen(indexName);

        //noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final var recoveryState = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).get()
            .shardRecoveryStates().get(indexName).stream().filter(rs -> rs.getPrimary() == false).findFirst().get();
        assertThat(recoveryState.getIndex().totalFileCount()).isGreaterThan(0);
    }

    @Test
    public void testUsesFileBasedRecoveryIfOperationsBasedRecoveryWouldBeUnreasonable() throws Exception {
        cluster().ensureAtLeastNumDataNodes(2);

        String indexName = "test";
        var settings = new ArrayList<String>();
        settings.add("number_of_replicas = 1");
        settings.add("\"unassigned.node_left.delayed_timeout\"='12h'");
        settings.add("\"soft_deletes.enabled\"=true");
        settings.add("\"soft_deletes.retention_lease.sync_interval\"='100ms'");

        final double reasonableOperationsBasedRecoveryProportion;
        if (randomBoolean()) {
            reasonableOperationsBasedRecoveryProportion = biasedDoubleBetween(0.05, 0.99);
            settings.add("\"recovery.file_based_threshold\"="+reasonableOperationsBasedRecoveryProportion);
        } else {
            reasonableOperationsBasedRecoveryProportion
                = IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.get(Settings.EMPTY);
        }
        logger.info("--> performing ops-based recoveries up to [{}%] of docs", reasonableOperationsBasedRecoveryProportion * 100.0);

        execute("CREATE TABLE doc.test (num INT)" +
                " CLUSTERED INTO 1 SHARDS" +
                " WITH (" + String.join(",", settings) + ")");
        int numDocs = randomIntBetween(1, 100);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc.test (num) VALUES (?)", args);
        ensureGreen(indexName);

        execute("OPTIMIZE TABLE doc.test");
        // wait for all history to be discarded
        assertBusy(() -> {
            var indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
            for (ShardStats shardStats : indicesStats.getShards()) {
                final long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
                assertThat(shardStats.getRetentionLeaseStats().leases().leases().stream().allMatch(
                               l -> l.retainingSequenceNumber() == maxSeqNo + 1)).as(shardStats.getRetentionLeaseStats().leases() + " should discard history up to " + maxSeqNo).isTrue();
            }
        });
        execute("OPTIMIZE TABLE doc.test"); // ensure that all operations are in the safe commit

        var indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
        final ShardStats shardStats = indicesStats.getShards()[0];
        final long docCount = shardStats.getStats().docs.getCount();
        assertThat(shardStats.getStats().docs.getDeleted()).isEqualTo(0L);
        assertThat(shardStats.getSeqNoStats().getMaxSeqNo() + 1).isEqualTo(docCount);

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final ShardRouting replicaShardRouting = indexShardRoutingTable.replicaShards().get(0);
        indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
        assertThat(indicesStats.getShards()[0].getRetentionLeaseStats()
                       .leases().contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting))).as("should have lease for " + replicaShardRouting).isTrue();
        cluster().restartNode(discoveryNodes.get(replicaShardRouting.currentNodeId()).getName(),
                                      new TestCluster.RestartCallback() {
                                          @Override
                                          public Settings onNodeStopped(String nodeName) throws Exception {
                                              assertThat(
                                                    client().admin().cluster().health(
                                                        new ClusterHealthRequest()
                                                            .waitForNodes(Integer.toString(discoveryNodes.getSize() - 1))
                                                            .waitForEvents(Priority.LANGUID)
                                                    ).get().isTimedOut()).isFalse();

                                              final int newDocCount = Math.toIntExact(Math.round(Math.ceil(
                                                  (1 + Math.ceil(docCount * reasonableOperationsBasedRecoveryProportion))
                                                  / (1 - reasonableOperationsBasedRecoveryProportion))));

                                              /*
                                               *     newDocCount >= (ceil(docCount * p) + 1) / (1-p)
                                               *
                                               * ==> 0 <= newDocCount * (1-p) - ceil(docCount * p) - 1
                                               *       =  newDocCount - (newDocCount * p + ceil(docCount * p) + 1)
                                               *       <  newDocCount - (ceil(newDocCount * p) + ceil(docCount * p))
                                               *       <= newDocCount -  ceil(newDocCount * p + docCount * p)
                                               *
                                               * ==> docCount <  newDocCount + docCount - ceil((newDocCount + docCount) * p)
                                               *              == localCheckpoint + 1    - ceil((newDocCount + docCount) * p)
                                               *              == firstReasonableSeqNo
                                               *
                                               * The replica has docCount docs, i.e. has operations with seqnos [0..docCount-1], so a seqno-based recovery will start
                                               * from docCount < firstReasonableSeqNo
                                               *
                                               * ==> it is unreasonable to recover the replica using a seqno-based recovery
                                               */

                                              var args = new Object[newDocCount][];
                                              for (int i = 0; i < newDocCount; i++) {
                                                  args[i] = new Object[]{i};
                                              }
                                              execute("INSERT INTO doc.test (num) VALUES (?)", args);

                                              execute("OPTIMIZE TABLE doc.test");

                                              assertBusy(() -> {
                                                  var indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
                                                  assertThat(indicesStats.getShards()[0].getRetentionLeaseStats().leases().contains(
                                                            ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting))).as(
                                                        "should no longer have lease for " + replicaShardRouting).isFalse();
                                              });
                                              return super.onNodeStopped(nodeName);
                                          }
                                      });

        ensureGreen(indexName);

        //noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final var recoveryState = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).get()
            .shardRecoveryStates().get(indexName).stream().filter(rs -> rs.getPrimary() == false).findFirst().get();
        assertThat(recoveryState.getIndex().totalFileCount()).isGreaterThan(0);
    }

    @Test
    public void testDoesNotCopyOperationsInSafeCommit() throws Exception {
        cluster().ensureAtLeastNumDataNodes(2);

        String indexName = "test";
        execute("CREATE TABLE doc.test (num INT)" +
                " CLUSTERED INTO 1 SHARDS" +
                " WITH (" +
                "  number_of_replicas = 0," +
                "  \"soft_deletes.enabled\"=true" +
                " )");
        int numDocs = randomIntBetween(1, 100);
        var args = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            args[i] = new Object[]{i};
        }
        execute("INSERT INTO doc.test (num) VALUES (?)", args);

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final IndexShard primary = cluster().getInstance(IndicesService.class,
                                                                 discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()).getShardOrNull(shardId);
        final long maxSeqNoBeforeRecovery = primary.seqNoStats().getMaxSeqNo();
        assertBusy(() -> assertThat(primary.getLastSyncedGlobalCheckpoint()).isEqualTo(maxSeqNoBeforeRecovery));
        execute("OPTIMIZE TABLE doc.test"); // makes a safe commit

        execute("INSERT INTO doc.test (num) VALUES (?)", args);

        execute("ALTER TABLE doc.test SET (number_of_replicas = 1)");
        ensureGreen(indexName);
        final long maxSeqNoAfterRecovery = primary.seqNoStats().getMaxSeqNo();

        //noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final var recoveryState = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).get()
            .shardRecoveryStates().get(indexName).stream().filter(rs -> rs.getPrimary() == false).findFirst().get();
        assertThat((long) recoveryState.getTranslog().recoveredOperations())
            .isLessThanOrEqualTo(maxSeqNoAfterRecovery - maxSeqNoBeforeRecovery);
    }

    public static final class TestAnalysisPlugin extends Plugin implements AnalysisPlugin {
        final AtomicBoolean throwParsingError = new AtomicBoolean();
        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return singletonMap("test_token_filter",
                                (indexSettings, environment, name, settings) -> new AbstractTokenFilterFactory(indexSettings, name, settings) {
                                    @Override
                                    public TokenStream create(TokenStream tokenStream) {
                                        if (throwParsingError.get()) {
                                            throw new MapperParsingException("simulate mapping parsing error");
                                        }
                                        return tokenStream;
                                    }
                                });
        }
    }
}
