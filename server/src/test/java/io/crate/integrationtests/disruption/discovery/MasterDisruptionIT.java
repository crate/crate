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

package io.crate.integrationtests.disruption.discovery;

import static io.crate.metadata.IndexParts.toIndexName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.disruption.BlockMasterServiceOnMaster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import io.crate.common.unit.TimeValue;

/**
 * Tests relating to the loss of the master.
 */
@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
@IntegTestCase.Slow
public class MasterDisruptionIT extends AbstractDisruptionTestCase {

    /**
     * This test isolates the master from rest of the cluster, waits for a new master to be elected, restores the partition
     * and verifies that all node agree on the new cluster state
     */
    @TestLogging(
            "_root:DEBUG,"
                    + "org.elasticsearch.cluster.service:TRACE,"
                    + "org.elasticsearch.gateway:TRACE,"
                    + "org.elasticsearch.indices.store:TRACE")
    @Test
    public void testIsolateMasterAndVerifyClusterStateConsensus() throws Exception {
        final List<String> nodes = startCluster(3);

        int numberOfShards = 1 + randomInt(2);
        int numberOfReplicas = randomInt(2);
        logger.info("creating table t with {} shards and {} replicas", numberOfShards, numberOfReplicas);
        execute("create table t (id int primary key, x string) clustered into " + numberOfShards + " shards with " +
                "(number_of_replicas = " + numberOfReplicas + " )");
        ensureGreen();

        String isolatedNode = cluster().getMasterName();
        TwoPartitions partitions = isolateNode(isolatedNode);
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);
        networkDisruption.startDisrupting();

        String nonIsolatedNode = partitions.getMajoritySide().iterator().next();

        // make sure cluster reforms
        ensureStableCluster(2, nonIsolatedNode);

        // make sure isolated need picks up on things.
        assertNoMaster(isolatedNode, TimeValue.timeValueSeconds(40));

        // restore isolation
        networkDisruption.stopDisrupting();

        for (String node : nodes) {
            ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkDisruption.expectedTimeToHeal().millis()),
                    true, node);
        }

        logger.info("issue a reroute");
        // trigger a reroute now, instead of waiting for the background reroute of RerouteService
        execute("ALTER CLUSTER REROUTE RETRY FAILED");
        // and wait for it to finish and for the cluster to stabilize
        ensureGreen();

        // verify all cluster states are the same
        // use assert busy to wait for cluster states to be applied (as publish_timeout has low value)
        assertBusy(() -> {
            ClusterState state = null;
            for (String node : nodes) {
                ClusterState nodeState = getNodeClusterState(node);
                if (state == null) {
                    state = nodeState;
                    continue;
                }
                // assert nodes are identical
                try {
                    assertThat(nodeState.version()).as("unequal versions").isEqualTo(state.version());
                    assertThat(nodeState.nodes().getSize()).as("unequal node count").isEqualTo(state.nodes().getSize());
                    assertThat(nodeState.nodes().getMasterNodeId()).as("different masters ").isEqualTo(state.nodes().getMasterNodeId());
                    assertThat(nodeState.metadata().version()).as("different meta data version").isEqualTo(state.metadata().version());
                    assertThat(nodeState.routingTable().toString()).as("different routing").isEqualTo(state.routingTable().toString());
                } catch (AssertionError t) {
                    fail("failed comparing cluster state: " + t.getMessage() + "\n" +
                            "--- cluster state of node [" + nodes.get(0) + "]: ---\n" + state +
                            "\n--- cluster state [" + node + "]: ---\n" + nodeState);
                }

            }
        });
    }

    /**
     * Verify that the proper block is applied when nodes lose their master
     */
    @Test
    public void testVerifyApiBlocksDuringPartition() throws Exception {
        cluster().startNodes(3);

        logger.info("creating table t with 1 shards and 2 replicas");
        execute("create table t (id int primary key, x string) clustered into 1 shards with " +
                "(number_of_replicas = 2)");

        // Everything is stable now, it is now time to simulate evil...
        // but first make sure we have no initializing shards and all is green
        // (waiting for green here, because indexing / search in a yellow index is fine as long as no other nodes go down)
        ensureGreen();

        TwoPartitions partitions = TwoPartitions.random(random(), cluster().getNodeNames());
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);

        assertThat(partitions.getMinoritySide().size()).isEqualTo(1);
        final String isolatedNode = partitions.getMinoritySide().iterator().next();
        assertThat(partitions.getMajoritySide().size()).isEqualTo(2);
        final String nonIsolatedNode = partitions.getMajoritySide().iterator().next();

        // Simulate a network issue between the unlucky node and the rest of the cluster.
        networkDisruption.startDisrupting();

        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, NoMasterBlockService.NO_MASTER_BLOCK_WRITES, TimeValue.timeValueSeconds(30));

        logger.info("wait until elected master has been removed and a new 2 node cluster was from (via [{}])", isolatedNode);
        ensureStableCluster(2, nonIsolatedNode);

        for (String node : partitions.getMajoritySide()) {
            ClusterState nodeState = getNodeClusterState(node);
            boolean success = true;
            if (nodeState.nodes().getMasterNode() == null) {
                success = false;
            }
            if (!nodeState.blocks().global().isEmpty()) {
                success = false;
            }
            if (!success) {
                fail("node [" + node + "] has no master or has blocks, despite of being on the right side of the partition. State dump:\n"
                        + nodeState);
            }
        }

        networkDisruption.stopDisrupting();

        // Wait until the master node sees al 3 nodes again.
        ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkDisruption.expectedTimeToHeal().millis()));

        logger.info("Verify no master block with {} set to {}", NoMasterBlockService.NO_MASTER_BLOCK_SETTING.getKey(), "all");
        client().admin().cluster()
            .execute(
                ClusterUpdateSettingsAction.INSTANCE,
                new ClusterUpdateSettingsRequest()
                    .transientSettings(Settings.builder().put(NoMasterBlockService.NO_MASTER_BLOCK_SETTING.getKey(), "all"))
            )
            .get();

        networkDisruption.startDisrupting();

        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, NoMasterBlockService.NO_MASTER_BLOCK_ALL, TimeValue.timeValueSeconds(30));

        // make sure we have stable cluster & cross partition recoveries are canceled by the removal of the missing node
        // the unresponsive partition causes recoveries to only time out after 15m (default) and these will cause
        // the test to fail due to unfreed resources
        ensureStableCluster(2, nonIsolatedNode);
    }

    @Test
    public void testMappingNewFieldsTimeoutDoesntAffectCheckpoints() throws Exception {
        TestCluster internalCluster = cluster();
        internalCluster.startNodes(3);
        ensureStableCluster(3);

        logger.info("creating table t with 1 shards and 1 replica");
        execute("create table t (id int primary key, x object(dynamic)) clustered into 1 shards with " +
                "(number_of_replicas = 1, \"routing.allocation.exclude._name\" = '" + cluster().getMasterName()
                + "', \"write.wait_for_active_shards\" = 1)");
        ensureGreen();
        execute("insert into t values (?, ?)", new Object[]{1, Map.of("first field", "first value")});

        ServiceDisruptionScheme disruption = new BlockMasterServiceOnMaster(random());
        setDisruptionScheme(disruption);

        disruption.startDisrupting();

        try {
            execute("insert into t values (?, ?), (?, ?), (?, ?)",
                    new Object[]{
                        2, Map.of("2nd field", "2nd value"),
                        3, Map.of("3rd field", "3rd value"),
                        4, Map.of("4th field", "4th value"),
                    });
        } catch (Exception e) {
            // failure is acceptable
        }

        disruption.stopDisrupting();

        String indexName = toIndexName(sqlExecutor.getCurrentSchema(), "t", null);
        assertBusy(() -> {
            IndicesStatsResponse stats = FutureUtils.get(client().admin().indices().stats(new IndicesStatsRequest().indices(indexName).clear()));
            for (ShardStats shardStats : stats.getShards()) {
                assertThat(shardStats.getSeqNoStats().getGlobalCheckpoint()).as(shardStats.getShardRouting().toString()).isEqualTo(shardStats.getSeqNoStats().getLocalCheckpoint());
            }
        }, 1, TimeUnit.MINUTES);
    }
}
