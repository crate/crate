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

package io.crate.integrationtests.disruption.routing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Before;
import org.junit.Test;

import io.crate.common.collections.Sets;
import io.crate.metadata.IndexParts;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
@IntegTestCase.Slow
public class PrimaryAllocationIT extends IntegTestCase {

    private String schema;
    private String indexName;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    @Before
    public void setupIndexNameAndSchemaForTableT() {
        schema = sqlExecutor.getCurrentSchema();
        indexName = IndexParts.toIndexName(schema, "t", null);
    }

    private Settings createStaleReplicaScenario(String master, String schema, String indexName) throws Exception {
        execute("insert into t values ('value1')");
        refresh();

        ClusterState state = client().admin().cluster().state(new ClusterStateRequest().all()).get().getState();
        List<ShardRouting> shards = state.routingTable().allShards(indexName);
        assertThat(shards.size(), equalTo(2));

        final String primaryNode;
        final String replicaNode;
        if (shards.get(0).primary()) {
            primaryNode = state.getRoutingNodes().node(shards.get(0).currentNodeId()).node().getName();
            replicaNode = state.getRoutingNodes().node(shards.get(1).currentNodeId()).node().getName();
        } else {
            primaryNode = state.getRoutingNodes().node(shards.get(1).currentNodeId()).node().getName();
            replicaNode = state.getRoutingNodes().node(shards.get(0).currentNodeId()).node().getName();
        }

        NetworkDisruption partition = new NetworkDisruption(
            new TwoPartitions(Set.of(master, replicaNode), Collections.singleton(primaryNode)),
            new NetworkDisconnect());
        internalCluster().setDisruptionScheme(partition);
        logger.info("--> partitioning node with primary shard from rest of cluster");
        partition.startDisrupting();

        ensureStableCluster(2, master);

        logger.info("--> index a document into previous replica shard (that is now primary)");
        systemExecute("insert into t values ('value2')", schema, replicaNode);

        logger.info("--> shut down node that has new acknowledged document");
        final Settings inSyncDataPathSettings = internalCluster().dataPathSettings(replicaNode);
        internalCluster().stopRandomNode(TestCluster.nameFilter(replicaNode));

        ensureStableCluster(1, master);

        partition.stopDisrupting();

        logger.info("--> waiting for node with old primary shard to rejoin the cluster");
        ensureStableCluster(2, master);

        logger.info("--> check that old primary shard does not get promoted to primary again");
        // kick reroute and wait for all shard states to be fetched
        client(master).admin().cluster().execute(ClusterRerouteAction.INSTANCE, new ClusterRerouteRequest()).get();
        assertBusy(() -> assertThat(internalCluster().getInstance(GatewayAllocator.class, master).getNumberOfInFlightFetch(),
            equalTo(0)));
        // kick reroute a second time and check that all shards are unassigned
        var clusterRerouteResponse = client(master).admin().cluster().execute(ClusterRerouteAction.INSTANCE, new ClusterRerouteRequest()).get();
        assertThat(clusterRerouteResponse.getState().getRoutingNodes().unassigned().size(), equalTo(2));
        return inSyncDataPathSettings;
    }

    @Test
    public void testDoNotAllowStaleReplicasToBePromotedToPrimary() throws Exception {
        logger.info("--> starting 3 nodes, 1 master, 2 data");
        String master = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        execute("create table t (x string) clustered into 1 shards with (number_of_replicas = 1, \"write.wait_for_active_shards\" = 1)");
        ensureGreen();
        final Settings inSyncDataPathSettings = createStaleReplicaScenario(master, schema, indexName);

        logger.info("--> starting node that reuses data folder with the up-to-date primary shard");
        internalCluster().startDataOnlyNode(inSyncDataPathSettings);

        logger.info("--> check that the up-to-date primary shard gets promoted and that documents are available");
        ensureYellow(indexName);
        execute("select * from t");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testDoNotRemoveAllocationIdOnNodeLeave() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        execute("create table t (x string) clustered into 1 shards with (number_of_replicas = 1, " +
                "\"write.wait_for_active_shards\" = 1, \"unassigned.node_left.delayed_timeout\" = 0)");
        String replicaNode = internalCluster().startDataOnlyNode(Settings.EMPTY);
        ensureGreen();
        final Settings inSyncDataPathSettings = internalCluster().dataPathSettings(replicaNode);
        internalCluster().stopRandomNode(TestCluster.nameFilter(replicaNode));
        ensureYellow();
        assertEquals(2, client().admin().cluster().state(new ClusterStateRequest()).get().getState().metadata().index(indexName)
            .inSyncAllocationIds(0).size());
        internalCluster().restartRandomDataNode(new TestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });
        logger.info("--> wait until shard is failed and becomes unassigned again");
        assertBusy(() -> assertTrue(client().admin().cluster().state(new ClusterStateRequest()).get().getState()
            .getRoutingTable().index(indexName).allPrimaryShardsUnassigned()));
        assertEquals(2, client().admin().cluster().state(new ClusterStateRequest()).get().getState()
            .metadata().index(indexName).inSyncAllocationIds(0).size());

        logger.info("--> starting node that reuses data folder with the up-to-date shard");
        internalCluster().startDataOnlyNode(inSyncDataPathSettings);
        ensureGreen();
    }

    @Test
    public void testRemoveAllocationIdOnWriteAfterNodeLeave() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        execute("create table t (x string) clustered into 1 shards with (number_of_replicas = 1, " +
                "\"write.wait_for_active_shards\" = 1, \"unassigned.node_left.delayed_timeout\" = 0)");
        String replicaNode = internalCluster().startDataOnlyNode(Settings.EMPTY);
        final Settings inSyncDataPathSettings = internalCluster().dataPathSettings(replicaNode);
        ensureGreen();
        internalCluster().stopRandomNode(TestCluster.nameFilter(replicaNode));
        ensureYellow();
        assertEquals(2, client().admin().cluster().state(new ClusterStateRequest()).get().getState()
            .metadata().index(indexName).inSyncAllocationIds(0).size());
        logger.info("--> inserting row...");
        execute("insert into t values ('value1')");
        assertEquals(1, client().admin().cluster().state(new ClusterStateRequest()).get().getState()
            .metadata().index(indexName).inSyncAllocationIds(0).size());
        internalCluster().restartRandomDataNode(new TestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });
        logger.info("--> wait until shard is failed and becomes unassigned again");
        assertBusy(() -> assertTrue(client().admin().cluster().state(new ClusterStateRequest()).get().getState()
            .getRoutingTable().index(indexName).allPrimaryShardsUnassigned()));
        assertEquals(1, client().admin().cluster().state(new ClusterStateRequest()).get().getState()
            .metadata().index(indexName).inSyncAllocationIds(0).size());

        logger.info("--> starting node that reuses data folder with the up-to-date shard");
        internalCluster().startDataOnlyNode(inSyncDataPathSettings);
        assertBusy(() -> assertTrue(client().admin().cluster().state(new ClusterStateRequest()).get().getState()
            .getRoutingTable().index(indexName).allPrimaryShardsUnassigned()));
    }

    @Test
    public void testNotWaitForQuorumCopies() throws Exception {
        logger.info("--> starting 3 nodes");
        List<String> nodes = internalCluster().startNodes(3);
        int numberOfShards = randomIntBetween(1, 3);
        logger.info("--> creating index with {} primary and 2 replicas", numberOfShards);
        execute("create table t (x string) clustered into " + numberOfShards +
                " shards with (number_of_replicas = 2)");
        ensureGreen();
        execute("insert into t values ('value1')");
        logger.info("--> removing 2 nodes from cluster");
        internalCluster().stopRandomNode(TestCluster.nameFilter(nodes.get(1), nodes.get(2)));
        internalCluster().stopRandomNode(TestCluster.nameFilter(nodes.get(1), nodes.get(2)));
        internalCluster().restartRandomDataNode();
        logger.info("--> checking that index still gets allocated with only 1 shard copy being available");
        ensureYellow();
        execute("select * from t");
        assertThat(response.rowCount(), is(1L));
    }

    /**
     * This test ensures that for an unassigned primary shard that has a valid shard copy on at least one node,
     * we will force allocate the primary shard to one of those nodes, even if the allocation deciders all return
     * a NO decision to allocate.
     */
    @Test
    public void testForceAllocatePrimaryOnNoDecision() throws Exception {
        logger.info("--> starting 1 node");
        final String node = internalCluster().startNode();
        logger.info("--> creating index with 1 primary and 0 replicas");
        execute("create table t (x string) clustered into 1 shards with (number_of_replicas = 0)");
        logger.info("--> update the settings to prevent allocation to the data node");
        execute("set global cluster.routing.allocation.exclude._name = '" + node + "'");
        logger.info("--> full cluster restart");
        internalCluster().fullRestart();
        logger.info("--> checking that the primary shard is force allocated to the data node despite being blocked by the exclude filter");
        ensureGreen();
        assertEquals(1, client().admin().cluster().state(new ClusterStateRequest()).get().getState()
                            .routingTable().index(indexName).shardsWithState(ShardRoutingState.STARTED).size());
    }

    /**
     * This test asserts that replicas failed to execute resync operations will be failed but not marked as stale.
     */
    @TestLogging("_root:DEBUG, org.elasticsearch.cluster.routing.allocation:TRACE, org.elasticsearch.cluster.action.shard:TRACE," +
                 "org.elasticsearch.indices.recovery:TRACE, org.elasticsearch.cluster.routing.allocation.allocator:TRACE")
    @Test
    public void testPrimaryReplicaResyncFailed() throws Exception {
        String master = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        final int numberOfReplicas = between(2, 3);
        final String oldPrimary = internalCluster().startDataOnlyNode();
        execute("create table t (x string) clustered into 1 shards " +
                "with (number_of_replicas = " + numberOfReplicas + ", \"write.wait_for_active_shards\" = 1)");
        final ShardId shardId = new ShardId(clusterService().state().metadata().index(indexName).getIndex(), 0);
        final Set<String> replicaNodes = new HashSet<>(internalCluster().startDataOnlyNodes(numberOfReplicas));
        ensureGreen();
        String timeout = randomFrom("0s", "1s", "2s");
        execute("SET GLOBAL cluster.routing.allocation.enable = 'none'");
        execute("SET GLOBAL PERSISTENT indices.replication.retry_timeout = ?", new Object[] { timeout });

        logger.info("--> Indexing with gap in seqno to ensure that some operations will be replayed in resync");
        long numDocs = scaledRandomIntBetween(5, 50);
        for (int i = 0; i < numDocs; i++) {
            execute("insert into t values ('" + (numDocs + i) + "')");
        }
        final IndexShard oldPrimaryShard = internalCluster().getInstance(IndicesService.class, oldPrimary).getShardOrNull(shardId);
        EngineTestCase.generateNewSeqNo(IndexShardTestCase.getEngine(oldPrimaryShard)); // Make gap in seqno.
        long moreDocs = scaledRandomIntBetween(1, 10);
        for (int i = 0; i < moreDocs; i++) {
            execute("insert into t values ('" + (numDocs + i) + "')");
        }
        final Set<String> replicasSide1 = Set.copyOf(randomSubsetOf(between(1, numberOfReplicas - 1), replicaNodes));
        final Set<String> replicasSide2 = Sets.difference(replicaNodes, replicasSide1);
        NetworkDisruption partition = new NetworkDisruption(new TwoPartitions(replicasSide1, replicasSide2), new NetworkDisconnect());
        internalCluster().setDisruptionScheme(partition);
        logger.info("--> isolating some replicas during primary-replica resync");
        partition.startDisrupting();
        internalCluster().stopRandomNode(TestCluster.nameFilter(oldPrimary));
        // Checks that we fail replicas in one side but not mark them as stale.
        assertBusy(() -> {
            ClusterState state = client(master).admin().cluster().state(new ClusterStateRequest()).get().getState();
            final IndexShardRoutingTable shardRoutingTable = state.routingTable().shardRoutingTable(shardId);
            final String newPrimaryNode = state.getRoutingNodes().node(shardRoutingTable.primaryShard().currentNodeId()).node().getName();
            assertThat(newPrimaryNode, not(equalTo(oldPrimary)));
            Set<String> selectedPartition = replicasSide1.contains(newPrimaryNode) ? replicasSide1 : replicasSide2;
            assertThat(shardRoutingTable.activeShards(), hasSize(selectedPartition.size()));
            for (ShardRouting activeShard : shardRoutingTable.activeShards()) {
                assertThat(state.getRoutingNodes().node(activeShard.currentNodeId()).node().getName(), is(in(selectedPartition)));
            }
            assertThat(state.metadata().index(indexName).inSyncAllocationIds(shardId.id()), hasSize(numberOfReplicas + 1));
        }, 1, TimeUnit.MINUTES);
        execute("SET GLOBAL cluster.routing.allocation.enable = 'all'");
        partition.stopDisrupting();
        partition.ensureHealthy(internalCluster());
        logger.info("--> stop disrupting network and re-enable allocation");
        assertBusy(() -> {
            ClusterState state = client(master).admin().cluster().state(new ClusterStateRequest()).get().getState();
            assertThat(state.routingTable().shardRoutingTable(shardId).activeShards(), hasSize(numberOfReplicas));
            assertThat(state.metadata().index(indexName).inSyncAllocationIds(shardId.id()), hasSize(numberOfReplicas + 1));
            for (String node : replicaNodes) {
                IndexShard shard = internalCluster().getInstance(IndicesService.class, node).getShardOrNull(shardId);
                assertThat(shard.getLocalCheckpoint(), equalTo(numDocs + moreDocs));
            }
        }, 30, TimeUnit.SECONDS);
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
    }
}
