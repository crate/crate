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
import static java.util.Collections.singletonMap;
import static org.elasticsearch.node.RecoverySettingsChunkSizePlugin.CHUNK_SIZE_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
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
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.elasticsearch.test.IntegTestCase.Scope;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import org.elasticsearch.test.IntegTestCase;
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
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    private void assertRecoveryStateWithoutStage(RecoveryState state, int shardId, RecoverySource recoverySource, boolean primary,
                                                 String sourceNode, String targetNode) {
        assertThat(state.getShardId().getId(), equalTo(shardId));
        assertThat(state.getRecoverySource(), equalTo(recoverySource));
        assertThat(state.getPrimary(), equalTo(primary));
        if (sourceNode == null) {
            assertNull(state.getSourceNode());
        } else {
            assertNotNull(state.getSourceNode());
            assertThat(state.getSourceNode().getName(), equalTo(sourceNode));
        }
        if (targetNode == null) {
            assertNull(state.getTargetNode());
        } else {
            assertNotNull(state.getTargetNode());
            assertThat(state.getTargetNode().getName(), equalTo(targetNode));
        }
    }

    private void assertRecoveryState(RecoveryState state, int shardId, RecoverySource type, boolean primary, RecoveryState.Stage stage,
                                     String sourceNode, String targetNode) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage(), equalTo(stage));
    }

    private void assertOnGoingRecoveryState(RecoveryState state, int shardId, RecoverySource type, boolean primary,
                                            String sourceNode, String targetNode) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage(), not(equalTo(RecoveryState.Stage.DONE)));
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
        assertTrue(response.isAcknowledged());
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
        assertTrue(response.isAcknowledged());
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
        assertThat(response.rows()[0][0], is((long) numDocs));
    }

    private void validateIndexRecoveryState(RecoveryState.Index indexState) {
        assertThat(indexState.time(), greaterThanOrEqualTo(0L));
        assertThat(indexState.recoveredFilesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredFilesPercent(), lessThanOrEqualTo(100.0f));
        assertThat(indexState.recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredBytesPercent(), lessThanOrEqualTo(100.0f));
    }

    @Test
    public void testGatewayRecovery() throws Exception {
        logger.info("--> start nodes");
        String node = internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        var indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null);
        RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(indexName)).get();
        assertThat(response.shardRecoveryStates().size(), equalTo(SHARD_COUNT));
        assertThat(response.shardRecoveryStates().get(indexName).size(), equalTo(1));

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(indexName);
        assertThat(recoveryStates.size(), equalTo(1));

        RecoveryState recoveryState = recoveryStates.get(0);

        assertRecoveryState(recoveryState, 0, RecoverySource.ExistingStoreRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, null, node);

        validateIndexRecoveryState(recoveryState.getIndex());
    }

    @Test
    public void testGatewayRecoveryTestActiveOnly() throws Exception {
        logger.info("--> start nodes");
        internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        execute("SELECT * FROM sys.shards WHERE table_name = '" + INDEX_NAME + "'" +
                " AND recovery['stage'] != 'DONE'");
        assertThat(response.rowCount(), is(0L));  // Should not expect any responses back
    }

    @Test
    public void testReplicaRecovery() throws Exception {
        final String nodeA = internalCluster().startNode();

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
        assertThat(response.rows()[0][0], is((long) numOfDocs));

        // We do not support ALTER on a closed table
        //final boolean closedIndex = randomBoolean();
        final boolean closedIndex = false;
        if (closedIndex) {
            execute("ALTER TABLE " + INDEX_NAME + " CLOSE");
            ensureGreen();
        }

        // force a shard recovery from nodeA to nodeB
        final String nodeB = internalCluster().startNode();
        execute("ALTER TABLE " + INDEX_NAME + " SET (number_of_replicas=1)");
        ensureGreen();


        // we should now have two total shards, one primary and one replica
        execute("SELECT * FROM sys.shards WHERE table_name = '" + INDEX_NAME + "'");
        assertThat(response.rowCount(), is(2L));

        var indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null);
        final RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(indexName)).get();

        // we should now have two total shards, one primary and one replica
        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(indexName);
        assertThat(recoveryStates.size(), equalTo(2));

        List<RecoveryState> nodeAResponses = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeAResponses.size(), equalTo(1));
        List<RecoveryState> nodeBResponses = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBResponses.size(), equalTo(1));

        // validate node A recovery
        final RecoveryState nodeARecoveryState = nodeAResponses.get(0);
        final RecoverySource expectedRecoverySource;
        if (closedIndex == false) {
            expectedRecoverySource = RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        } else {
            expectedRecoverySource = RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        }
        assertRecoveryState(nodeARecoveryState, 0, expectedRecoverySource, true, RecoveryState.Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryState.getIndex());

        // validate node B recovery
        final RecoveryState nodeBRecoveryState = nodeBResponses.get(0);
        assertRecoveryState(nodeBRecoveryState, 0, RecoverySource.PeerRecoverySource.INSTANCE, false, RecoveryState.Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryState.getIndex());

        internalCluster().stopRandomNode(TestCluster.nameFilter(nodeA));

        if (closedIndex) {
            execute("ALTER TABLE " + INDEX_NAME + " OPEN");
        }
        var res = execute("SELECT COUNT(*) FROM " + INDEX_NAME);
        assertThat(res.rows()[0][0], is((long) numOfDocs));
    }

    @Test
    public void testCancelNewShardRecoveryAndUsesExistingShardCopy() throws Exception {
        logger.info("--> start node A");
        final String nodeA = internalCluster().startNode();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> start node B");
        // force a shard recovery from nodeA to nodeB
        final String nodeB = internalCluster().startNode();

        logger.info("--> add replica for {} on node: {}", INDEX_NAME, nodeB);
        execute("ALTER TABLE " + INDEX_NAME + " SET (number_of_replicas=1, \"unassigned.node_left.delayed_timeout\"=0)");
        ensureGreen();

        logger.info("--> start node C");
        final String nodeC = internalCluster().startNode();

        // do sync flush to gen sync id
        execute("OPTIMIZE TABLE " + INDEX_NAME);
        //assertThat(client().admin().indices().prepareSyncedFlush(INDEX_NAME).get().failedShards(), equalTo(0));

        // hold peer recovery on phase 2 after nodeB down
        CountDownLatch phase1ReadyBlocked = new CountDownLatch(1);
        CountDownLatch allowToCompletePhase1Latch = new CountDownLatch(1);
        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeA);
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
        internalCluster().restartNode(
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
                    assertThat(nodeCRecoveryStates.size(), equalTo(1));

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
        assertThat(startedShards.size(), equalTo(2));
        for (ShardRouting shardRouting : startedShards) {
            if (shardRouting.primary()) {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(nodeA));
            } else {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(nodeB));
            }
        }
    }

    @Test
    public void testRerouteRecovery() throws Exception {
        logger.info("--> start node A");
        final String nodeA = internalCluster().startNode();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);
        execute("SELECT size FROM sys.shards WHERE table_name = '" + INDEX_NAME + "' AND primary=true");
        long shardSize = (long) response.rows()[0][0];

        logger.info("--> start node B");
        final String nodeB = internalCluster().startNode();

        ensureGreen();

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move shard from: {} to: {}", nodeA, nodeB);
        execute("ALTER TABLE " + INDEX_NAME + " REROUTE MOVE SHARD 0 FROM '" + nodeA + "' TO '" + nodeB + "'");

        logger.info("--> waiting for recovery to start both on source and target");
        final Index index = resolveIndex(IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null));
        assertBusy(() -> {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeA);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource(),
                       equalTo(1));
            indicesService = internalCluster().getInstance(IndicesService.class, nodeB);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget(),
                       equalTo(1));
        });

        logger.info("--> request recoveries");
        RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index.getName())).get();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(index.getName());
        List<RecoveryState> nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));

        assertRecoveryState(nodeARecoveryStates.get(0), 0, RecoverySource.EmptyStoreRecoverySource.INSTANCE, true,
                            RecoveryState.Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertOnGoingRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        logger.info("--> request node recovery stats");

        IndicesService indicesServiceNodeA = internalCluster().getInstance(IndicesService.class, nodeA);
        var recoveryStatsNodeA = indicesServiceNodeA.indexServiceSafe(index).getShard(0).recoveryStats();
        assertThat("node A should have ongoing recovery as source", recoveryStatsNodeA.currentAsSource(), equalTo(1));
        assertThat("node A should not have ongoing recovery as target", recoveryStatsNodeA.currentAsTarget(), equalTo(0));
        long nodeAThrottling = recoveryStatsNodeA.throttleTime().millis();

        IndicesService indicesServiceNodeB = internalCluster().getInstance(IndicesService.class, nodeB);
        var recoveryStatsNodeB = indicesServiceNodeB.indexServiceSafe(index).getShard(0).recoveryStats();
        assertThat("node B should not have ongoing recovery as source", recoveryStatsNodeB.currentAsSource(), equalTo(0));
        assertThat("node B should have ongoing recovery as target", recoveryStatsNodeB.currentAsTarget(), equalTo(1));
        long nodeBThrottling = recoveryStatsNodeB.throttleTime().millis();

        logger.info("--> checking throttling increases");
        final long finalNodeAThrottling = nodeAThrottling;
        final long finalNodeBThrottling = nodeBThrottling;
        var indexShardNodeA = indicesServiceNodeA.indexServiceSafe(index).getShard(0);
        var indexShardNodeB = indicesServiceNodeB.indexServiceSafe(index).getShard(0);
        assertBusy(() -> {
            assertThat("node A throttling should increase", indexShardNodeA.recoveryStats().throttleTime().millis(),
                    greaterThan(finalNodeAThrottling));
            assertThat("node B throttling should increase", indexShardNodeB.recoveryStats().throttleTime().millis(),
                    greaterThan(finalNodeBThrottling));
        });


        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();

        // wait for it to be finished
        ensureGreen(index.getName());

        response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index.getName())).get();
        recoveryStates = response.shardRecoveryStates().get(index.getName());
        assertThat(recoveryStates.size(), equalTo(1));

        assertRecoveryState(recoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(recoveryStates.get(0).getIndex());
        Consumer<String> assertNodeHasThrottleTimeAndNoRecoveries = nodeName ->  {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
            var recoveryStats = indicesService.indexServiceSafe(index).getShard(0).recoveryStats();
            assertThat(recoveryStats.currentAsSource(), equalTo(0));
            assertThat(recoveryStats.currentAsTarget(), equalTo(0));
            assertThat(nodeName + " throttling should be >0", recoveryStats.throttleTime().millis(), greaterThan(0L));
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
        String nodeC = internalCluster().startNode();
        assertFalse(client().admin().cluster().health(new ClusterHealthRequest().waitForNodes("3")).get().isTimedOut());

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move replica shard from: {} to: {}", nodeA, nodeC);
        execute("ALTER TABLE " + INDEX_NAME + " REROUTE MOVE SHARD 0 FROM '" + nodeA + "' TO '" + nodeC + "'");

        response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index.getName())).get();
        recoveryStates = response.shardRecoveryStates().get(index.getName());

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(nodeARecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, RecoveryState.Stage.DONE, nodeB, nodeA);
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());

        if (randomBoolean()) {
            // shutdown node with relocation source of replica shard and check if recovery continues
            internalCluster().stopRandomNode(TestCluster.nameFilter(nodeA));
            ensureStableCluster(2);

            response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index.getName())).get();
            recoveryStates = response.shardRecoveryStates().get(index.getName());

            nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
            assertThat(nodeARecoveryStates.size(), equalTo(0));
            nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
            assertThat(nodeBRecoveryStates.size(), equalTo(1));
            nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
            assertThat(nodeCRecoveryStates.size(), equalTo(1));

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
        assertThat(nodeARecoveryStates.size(), equalTo(0));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, RecoveryState.Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, RecoveryState.Stage.DONE, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
    }

    @Test
    public void testSnapshotRecovery() throws Exception {
        logger.info("--> start node A");
        String nodeA = internalCluster().startNode();

        logger.info("--> create repository");
        execute("CREATE REPOSITORY " + REPO_NAME + " TYPE FS WITH (location = '" + randomRepoPath() + "', compress=false)");

        ensureGreen();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT, REPLICA_COUNT);

        logger.info("--> snapshot");
        var snapshotName = REPO_NAME + "." + SNAP_NAME;
        execute("CREATE SNAPSHOT " + snapshotName + " ALL WITH (wait_for_completion=true)");

        execute("SELECT state FROM sys.snapshots WHERE name = '" + SNAP_NAME + "'");
        assertThat(response.rows()[0][0], is("SUCCESS"));

        execute("ALTER TABLE " + INDEX_NAME + " CLOSE");

        logger.info("--> restore");
        execute("RESTORE SNAPSHOT " + snapshotName + " ALL WITH (wait_for_completion=true)");

        ensureGreen();

        var snapshotInfo = client()
            .execute(GetSnapshotsAction.INSTANCE, new GetSnapshotsRequest(REPO_NAME, new String[]{SNAP_NAME}))
            .get().getSnapshots().get(0);

        logger.info("--> request recoveries");
        var indexName = IndexParts.toIndexName(sqlExecutor.getCurrentSchema(), INDEX_NAME, null);
        RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(indexName)).get();

        ThreadPool threadPool = internalCluster().getMasterNodeInstance(ThreadPool.class);
        Repository repository = internalCluster().getMasterNodeInstance(RepositoriesService.class).repository(REPO_NAME);
        final RepositoryData repositoryData = PlainActionFuture.get(f ->
            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(f, repository::getRepositoryData)));
        for (Map.Entry<String, List<RecoveryState>> indexRecoveryStates : response.shardRecoveryStates().entrySet()) {

            assertThat(indexRecoveryStates.getKey(), equalTo(indexName));
            List<RecoveryState> recoveryStates = indexRecoveryStates.getValue();
            assertThat(recoveryStates.size(), equalTo(SHARD_COUNT));

            for (RecoveryState recoveryState : recoveryStates) {
                RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(
                    ((RecoverySource.SnapshotRecoverySource)recoveryState.getRecoverySource()).restoreUUID(),
                    new Snapshot(REPO_NAME, snapshotInfo.snapshotId()),
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
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().health(new ClusterHealthRequest().waitForNodes(">=3")).get();
        assertThat(response.isTimedOut(), is(false));

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
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM doc." + indexName);
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));

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
            (MockTransportService) internalCluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);

        final AtomicBoolean recoveryStarted = new AtomicBoolean(false);
        final AtomicBoolean finalizeReceived = new AtomicBoolean(false);

        final SingleStartEnforcer validator = new SingleStartEnforcer(indexName, recoveryStarted, finalizeReceived);
        redTransportService.addSendBehavior(blueTransportService, (connection, requestId, action, request, options) -> {
            validator.accept(action, request);
            connection.sendRequest(requestId, action, request, options);
        });
        Runnable connectionBreaker = () -> {
            // Always break connection from source to remote to ensure that actions are retried
            logger.info("--> closing connections from source node to target node");
            blueTransportService.disconnectFromNode(redTransportService.getLocalNode());
            if (randomBoolean()) {
                // Sometimes break connection from remote to source to ensure that recovery is re-established
                logger.info("--> closing connections from target node to source node");
                redTransportService.disconnectFromNode(blueTransportService.getLocalNode());
            }
        };
        TransientReceiveRejected handlingBehavior =
            new TransientReceiveRejected(recoveryActionToBlock, finalizeReceived, recoveryStarted, connectionBreaker);
        redTransportService.addRequestHandlingBehavior(recoveryActionToBlock, handlingBehavior);

        try {
            logger.info("--> starting recovery from blue to red");
            execute("ALTER TABLE doc." + indexName + " SET (" +
                    " number_of_replicas=1," +
                    " \"routing.allocation.include.color\" = 'red,blue'" +
                    ")");

            ensureGreen();
            // TODO: ES will use the shard routing preference here to prefer `_local` shards on that node
            var nodeRedExecutor = executor(redNodeName);
            searchResponse = nodeRedExecutor.exec("SELECT COUNT(*) FROM doc." + indexName);
            assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));
        } finally {
            blueTransportService.clearAllRules();
            redTransportService.clearAllRules();
        }
    }

    private class TransientReceiveRejected implements StubbableTransport.RequestHandlingBehavior<TransportRequest> {

        private final String actionName;
        private final AtomicBoolean recoveryStarted;
        private final AtomicBoolean finalizeReceived;
        private final Runnable connectionBreaker;
        private final AtomicInteger blocksRemaining;

        private TransientReceiveRejected(String actionName, AtomicBoolean recoveryStarted, AtomicBoolean finalizeReceived,
                                         Runnable connectionBreaker) {
            this.actionName = actionName;
            this.recoveryStarted = recoveryStarted;
            this.finalizeReceived = finalizeReceived;
            this.connectionBreaker = connectionBreaker;
            this.blocksRemaining = new AtomicInteger(randomIntBetween(1, 3));
        }

        @Override
        public void messageReceived(TransportRequestHandler<TransportRequest> handler,
                                    TransportRequest request,
                                    TransportChannel channel) throws Exception {
            recoveryStarted.set(true);
            if (actionName.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                finalizeReceived.set(true);
            }
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

    private class SingleStartEnforcer implements BiConsumer<String, TransportRequest> {

        private final AtomicBoolean recoveryStarted;
        private final AtomicBoolean finalizeReceived;
        private final String indexName;

        private SingleStartEnforcer(String indexName, AtomicBoolean recoveryStarted, AtomicBoolean finalizeReceived) {
            this.indexName = indexName;
            this.recoveryStarted = recoveryStarted;
            this.finalizeReceived = finalizeReceived;
        }

        @Override
        public void accept(String action, TransportRequest request) {
            // The cluster state applier will immediately attempt to retry the recovery on a cluster state
            // update. We want to assert that the first and only recovery attempt succeeds
            if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action)) {
                StartRecoveryRequest startRecoveryRequest = (StartRecoveryRequest) request;
                ShardId shardId = startRecoveryRequest.shardId();
                logger.info("--> attempting to send start_recovery request for shard: " + shardId);
                if (indexName.equals(shardId.getIndexName()) && recoveryStarted.get() && finalizeReceived.get() == false) {
                    throw new IllegalStateException("Recovery cannot be started twice");
                }
            }
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
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().health(new ClusterHealthRequest().waitForNodes(">=3")).get();
        assertThat(response.isTimedOut(), is(false));

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
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        refresh();
        var searchResponse = execute("SELECT COUNT(*) FROM doc." + indexName);
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));

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
            (MockTransportService) internalCluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redMockTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);
        TransportService redTransportService = internalCluster().getInstance(TransportService.class, redNodeName);
        TransportService blueTransportService = internalCluster().getInstance(TransportService.class, blueNodeName);
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
        // TODO: ES will use the shard routing preference here to prefer `_local` shards on that node
        var nodeRedExecutor = executor(redNodeName);
        searchResponse = nodeRedExecutor.exec("SELECT COUNT(*) FROM doc." + indexName);
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));
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
        String masterNodeName = internalCluster().startMasterOnlyNode(nodeSettings);

        final String blueNodeName = internalCluster()
            .startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster()
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
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));

        MockTransportService masterTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, masterNodeName);
        MockTransportService blueMockTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, blueNodeName);
        MockTransportService redMockTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);

        redMockTransportService.addSendBehavior(blueMockTransportService, new StubbableTransport.SendRequestBehavior() {
            private final AtomicInteger count = new AtomicInteger();

            @Override
            public void sendRequest(Transport.Connection connection, long requestId, String action, TransportRequest request,
                                    TransportRequestOptions options) throws IOException {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action) && count.incrementAndGet() == 1) {
                    // ensures that it's considered as valid recovery attempt by source
                    try {
                        assertBusy(() -> assertThat(
                            "Expected there to be some initializing shards",
                            client(blueNodeName).admin().cluster().state(new ClusterStateRequest().local(true)).get()
                                .getState().getRoutingTable().index(indexName).shard(0).getAllInitializingShards(), not(empty())));
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
            assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));
        }
    }

    @Test
    public void testHistoryRetention() throws Exception {
        internalCluster().startNodes(3);

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


        String firstNodeToStop = randomFrom(internalCluster().getNodeNames());
        Settings firstNodeToStopDataPathSettings = internalCluster().dataPathSettings(firstNodeToStop);
        internalCluster().stopRandomNode(TestCluster.nameFilter(firstNodeToStop));
        String secondNodeToStop = randomFrom(internalCluster().getNodeNames());
        Settings secondNodeToStopDataPathSettings = internalCluster().dataPathSettings(secondNodeToStop);
        internalCluster().stopRandomNode(TestCluster.nameFilter(secondNodeToStop));

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
        internalCluster().startNode(randomFrom(firstNodeToStopDataPathSettings, secondNodeToStopDataPathSettings));
        ensureGreen(indexName);

        final RecoveryResponse recoveryResponse = client().admin().indices().recoveries(new RecoveryRequest(indexName)).get();
        final List<RecoveryState> recoveryStates = recoveryResponse.shardRecoveryStates().get(indexName);
        recoveryStates.removeIf(r -> r.getTimer().getStartNanoTime() <= desyncNanoTime);

        assertThat(recoveryStates, hasSize(1));
        assertThat(recoveryStates.get(0).getIndex().totalFileCount(), is(0));
        assertThat(recoveryStates.get(0).getTranslog().recoveredOperations(), greaterThan(0));
    }

    @Test
    public void testDoNotInfinitelyWaitForMapping() {
        internalCluster().ensureAtLeastNumDataNodes(3);
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
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(
                TransportService.class, node.getName());
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                    if (recoveryBlocked.tryAcquire()) {
                        PluginsService pluginService = internalCluster().getInstance(PluginsService.class, node.getName());
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
        assertThat((long) searchResponse.rows()[0][0], is((long) numDocs));
    }

    /** Makes sure the new master does not repeatedly fetch index metadata from recovering replicas */
    @Test
    public void testOngoingRecoveryAndMasterFailOver() throws Exception {
        internalCluster().startNodes(2);
        String nodeWithPrimary = internalCluster().startDataOnlyNode();
        execute("CREATE TABLE doc.test (id INT)" +
                " CLUSTERED INTO 1 SHARDS" +
                " WITH (number_of_replicas = 0, \"routing.allocation.include._name\" = '" + nodeWithPrimary + "')");
        MockTransportService transport = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeWithPrimary);
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
            String nodeWithReplica = internalCluster().startDataOnlyNode();
            execute("ALTER TABLE doc.test SET (number_of_replicas = 1," +
                    " \"routing.allocation.include._name\"='" + nodeWithPrimary + "," + nodeWithReplica + "')");
            phase1ReadyBlocked.await();
            internalCluster().restartNode(clusterService().state().nodes().getMasterNode().getName(),
                                          new TestCluster.RestartCallback());
            internalCluster().ensureAtLeastNumDataNodes(3);
            execute("ALTER TABLE doc.test RESET (\"routing.allocation.include._name\")");
            execute("ALTER TABLE doc.test SET (number_of_replicas = 2)");
            assertFalse(client().admin().cluster().health(new ClusterHealthRequest("test").waitForActiveShards(2)).get().isTimedOut());
        } finally {
            allowToCompletePhase1Latch.countDown();
        }
        ensureGreen();
    }

    @Test
    public void testRecoveryFlushReplica() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
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
                assertThat(shardStats.getCommitStats().getNumDocs(), equalTo(numDocs));
                SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
                    shardStats.getCommitStats().getUserData().entrySet());
                assertThat(commitInfo.localCheckpoint, equalTo(shardStats.getSeqNoStats().getLocalCheckpoint()));
                assertThat(commitInfo.maxSeqNo, equalTo(shardStats.getSeqNoStats().getMaxSeqNo()));
            }
        }
        SyncedFlushUtil.attemptSyncedFlush(logger, internalCluster(), shardId);
        assertBusy(() -> assertThat(client().execute(SyncedFlushAction.INSTANCE, new SyncedFlushRequest(indexName)).get().failedShards(), equalTo(0)));
        execute("ALTER TABLE doc.test SET (number_of_replicas = 2)");
        ensureGreen(indexName);
        // Recovery should keep syncId if no indexing activity on the primary after synced-flush.
        indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
        Set<String> syncIds = Stream.of(indicesStats.getIndex(indexName).getShards())
            .map(shardStats -> shardStats.getCommitStats().syncId())
            .collect(Collectors.toSet());
        assertThat(syncIds, hasSize(1));
    }

    @Test
    public void testRecoveryUsingSyncedFlushWithoutRetentionLease() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
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
        assertThat(SyncedFlushUtil.attemptSyncedFlush(logger, internalCluster(), shardId).successfulShards(), equalTo(2));

        final ClusterState clusterState = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        final ShardRouting shardToResync = randomFrom(clusterState.routingTable().shardRoutingTable(shardId).activeShards());
        internalCluster().restartNode(
            clusterState.nodes().get(shardToResync.currentNodeId()).getName(),
            new TestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertBusy(() -> {
                        var indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
                        assertFalse(indicesStats.getShards()[0].getRetentionLeaseStats().leases().contains(
                            ReplicationTracker.getPeerRecoveryRetentionLeaseId(shardToResync)));
                    });
                    return super.onNodeStopped(nodeName);
                }
            });

        ensureGreen(indexName);
    }

    @Test
    public void testRecoverLocallyUpToGlobalCheckpoint() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
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
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                    final RecoveryState recoveryState = internalCluster().getInstance(IndicesService.class, failingNode)
                        .getShardOrNull(new ShardId(resolveIndex(indexName), 0)).recoveryState();
                    assertThat(recoveryState.getTranslog().recoveredOperations(), equalTo(recoveryState.getTranslog().totalLocal()));
                    if (startRecoveryRequestFuture.isDone()) {
                        assertThat(recoveryState.getTranslog().totalLocal(), equalTo(0));
                        recoveryState.getTranslog().totalLocal(localRecoveredOps.get());
                        recoveryState.getTranslog().incrementRecoveredOperations(localRecoveredOps.get());
                    } else {
                        localRecoveredOps.set(recoveryState.getTranslog().totalLocal());
                        startRecoveryRequestFuture.onResponse((StartRecoveryRequest) request);
                    }
                }
                if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                    RetentionLeases retentionLeases = internalCluster().getInstance(IndicesService.class, node)
                        .indexServiceSafe(resolveIndex(indexName))
                        .getShard(0).getRetentionLeases();
                    throw new AssertionError("expect an operation-based recovery:" +
                                             "retention leases" + Strings.toString(retentionLeases) + "]");
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        IndexShard shard = internalCluster().getInstance(IndicesService.class, failingNode)
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
        assertThat(commitInfoAfterLocalRecovery.localCheckpoint, equalTo(lastSyncedGlobalCheckpoint));
        assertThat(commitInfoAfterLocalRecovery.maxSeqNo, equalTo(lastSyncedGlobalCheckpoint));
        assertThat(startRecoveryRequest.startingSeqNo(), equalTo(lastSyncedGlobalCheckpoint + 1));
        ensureGreen(indexName);
        assertThat((long) localRecoveredOps.get(), equalTo(lastSyncedGlobalCheckpoint - localCheckpointOfSafeCommit));
        for (var recoveryState : client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).get().shardRecoveryStates().get(indexName)) {
            if (startRecoveryRequest.targetNode().equals(recoveryState.getTargetNode())) {
                assertThat("expect an operation-based recovery", recoveryState.getIndex().fileDetails().values(), empty());
                assertThat("total recovered translog operations must include both local and remote recovery",
                           recoveryState.getTranslog().recoveredOperations(),
                           greaterThanOrEqualTo(Math.toIntExact(maxSeqNo - localCheckpointOfSafeCommit)));
            }
        }
        for (String node : nodes) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            transportService.clearAllRules();
        }
    }

    @Test
    public void testUsesFileBasedRecoveryIfRetentionLeaseMissing() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

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

        final IndexShard primary = internalCluster().getInstance(IndicesService.class,
                                                                 discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()).getShardOrNull(shardId);

        final ShardRouting replicaShardRouting = indexShardRoutingTable.replicaShards().get(0);
        internalCluster().restartNode(discoveryNodes.get(replicaShardRouting.currentNodeId()).getName(),
                                      new TestCluster.RestartCallback() {
                                          @Override
                                          public Settings onNodeStopped(String nodeName) throws Exception {
                                              assertFalse(
                                                    client().admin().cluster().health(
                                                        new ClusterHealthRequest()
                                                            .waitForNodes(Integer.toString(discoveryNodes.getSize() - 1))
                                                            .waitForEvents(Priority.LANGUID)
                                                    ).get().isTimedOut());

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
        assertThat(recoveryState.getIndex().totalFileCount(), greaterThan(0));
    }

    @Test
    public void testUsesFileBasedRecoveryIfRetentionLeaseAheadOfGlobalCheckpoint() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

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

        final IndexShard primary = internalCluster().getInstance(IndicesService.class,
                                                                 discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()).getShardOrNull(shardId);

        final ShardRouting replicaShardRouting = indexShardRoutingTable.replicaShards().get(0);
        internalCluster().restartNode(
            discoveryNodes.get(replicaShardRouting.currentNodeId()).getName(),
            new TestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertFalse(
                        client().admin().cluster().health(
                            new ClusterHealthRequest()
                                .waitForNodes(Integer.toString(discoveryNodes.getSize() - 1))
                                .waitForEvents(Priority.LANGUID)
                            ).get().isTimedOut()
                    );

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
        assertThat(recoveryState.getIndex().totalFileCount(), greaterThan(0));
    }

    @Test
    public void testUsesFileBasedRecoveryIfOperationsBasedRecoveryWouldBeUnreasonable() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

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
                assertTrue(shardStats.getRetentionLeaseStats().leases() + " should discard history up to " + maxSeqNo,
                           shardStats.getRetentionLeaseStats().leases().leases().stream().allMatch(
                               l -> l.retainingSequenceNumber() == maxSeqNo + 1));
            }
        });
        execute("OPTIMIZE TABLE doc.test"); // ensure that all operations are in the safe commit

        var indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
        final ShardStats shardStats = indicesStats.getShards()[0];
        final long docCount = shardStats.getStats().docs.getCount();
        assertThat(shardStats.getStats().docs.getDeleted(), equalTo(0L));
        assertThat(shardStats.getSeqNoStats().getMaxSeqNo() + 1, equalTo(docCount));

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final ShardRouting replicaShardRouting = indexShardRoutingTable.replicaShards().get(0);
        indicesStats = client().admin().indices().stats(new IndicesStatsRequest().indices(indexName)).get();
        assertTrue("should have lease for " + replicaShardRouting, indicesStats.getShards()[0].getRetentionLeaseStats()
                       .leases().contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting)));
        internalCluster().restartNode(discoveryNodes.get(replicaShardRouting.currentNodeId()).getName(),
                                      new TestCluster.RestartCallback() {
                                          @Override
                                          public Settings onNodeStopped(String nodeName) throws Exception {
                                              assertFalse(
                                                    client().admin().cluster().health(
                                                        new ClusterHealthRequest()
                                                            .waitForNodes(Integer.toString(discoveryNodes.getSize() - 1))
                                                            .waitForEvents(Priority.LANGUID)
                                                    ).get().isTimedOut());

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
                                                  assertFalse(
                                                        "should no longer have lease for " + replicaShardRouting,
                                                        indicesStats.getShards()[0].getRetentionLeaseStats().leases().contains(
                                                            ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting)));
                                              });
                                              return super.onNodeStopped(nodeName);
                                          }
                                      });

        ensureGreen(indexName);

        //noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final var recoveryState = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).get()
            .shardRecoveryStates().get(indexName).stream().filter(rs -> rs.getPrimary() == false).findFirst().get();
        assertThat(recoveryState.getIndex().totalFileCount(), greaterThan(0));
    }

    @Test
    public void testDoesNotCopyOperationsInSafeCommit() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

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

        final IndexShard primary = internalCluster().getInstance(IndicesService.class,
                                                                 discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()).getShardOrNull(shardId);
        final long maxSeqNoBeforeRecovery = primary.seqNoStats().getMaxSeqNo();
        assertBusy(() -> assertThat(primary.getLastSyncedGlobalCheckpoint(), equalTo(maxSeqNoBeforeRecovery)));
        execute("OPTIMIZE TABLE doc.test"); // makes a safe commit

        execute("INSERT INTO doc.test (num) VALUES (?)", args);

        execute("ALTER TABLE doc.test SET (number_of_replicas = 1)");
        ensureGreen(indexName);
        final long maxSeqNoAfterRecovery = primary.seqNoStats().getMaxSeqNo();

        //noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final var recoveryState = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).get()
            .shardRecoveryStates().get(indexName).stream().filter(rs -> rs.getPrimary() == false).findFirst().get();
        assertThat((long)recoveryState.getTranslog().recoveredOperations(),
                   lessThanOrEqualTo(maxSeqNoAfterRecovery - maxSeqNoBeforeRecovery));
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
