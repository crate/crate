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

package io.crate.integrationtests.disruption.seqno;

import io.crate.integrationtests.SQLIntegrationTestCase;
import io.crate.integrationtests.disruption.discovery.AbstractDisruptionTestCase;
import io.crate.metadata.IndexParts;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@SQLIntegrationTestCase.Slow
public class SequenceConsistencyIT extends AbstractDisruptionTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    @Test
    public void testPrimaryTermIsIncreasedOnReplicaPromotion() throws Throwable {
        logger.info("starting 3 nodes");
        String masterNodeName = internalCluster().startMasterOnlyNode(DEFAULT_SETTINGS);
        String firstDataNodeName = internalCluster().startDataOnlyNode(DEFAULT_SETTINGS);
        String secondDataNodeName = internalCluster().startDataOnlyNode(DEFAULT_SETTINGS);

        logger.info("wait for all nodes to join the cluster");
        ensureGreen();

        execute("""
            create table registers (
                id int primary key,
                value text
            ) CLUSTERED INTO 1 shards
            with (
                number_of_replicas = 1,
                "write.wait_for_active_shards" = 'ALL',
                "unassigned.node_left.delayed_timeout" = '1s'
            )
        """);
        execute("insert into registers values (1, 'initial value')");
        refresh();

        // not setting this when creating the table because we want the replica to be initialized before we start disrupting
        // otherwise the replica might not be ready to be promoted to primary due to "primary failed while replica initializing"
        execute("alter table registers set (\"write.wait_for_active_shards\" = 1)");

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String firstDataNodeId = clusterState.getNodes().resolveNode(firstDataNodeName).getId();
        String secondDataNodeId = clusterState.getNodes().resolveNode(secondDataNodeName).getId();

        boolean firstDataNodeHasPrimary = clusterState.getRoutingNodes().node(firstDataNodeId).copyShards().get(0).primary();

        // isolate node with the primary
        String isolatedNode = firstDataNodeHasPrimary ? firstDataNodeName : secondDataNodeName;
        Set<String> otherNodes = firstDataNodeHasPrimary ? Set.of(masterNodeName, secondDataNodeName) :
            Set.of(masterNodeName, firstDataNodeName);

        NetworkDisruption partition = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(otherNodes, Set.of(isolatedNode)),
            new NetworkDisruption.NetworkDisconnect()
        );
        internalCluster().setDisruptionScheme(partition);

        logger.info("start disrupting network");
        partition.startDisrupting();

        String schema = sqlExecutor.getCurrentSchema();
        try {
            execute("update registers set value = 'value whilst disrupted' where id = 1", null, isolatedNode);
            fail("expected timeout exception");
        } catch (ElasticsearchTimeoutException elasticsearchTimeoutException) {
        }

        String nonIsolatedDataNodeId = firstDataNodeHasPrimary ? secondDataNodeId : firstDataNodeId;
        logger.info("wait for replica on the partition with the master to be promoted to primary");
        assertBusy(() -> {
            String index = IndexParts.toIndexName(schema, "registers", null);
            ShardRouting primaryShard = client(masterNodeName).admin().cluster().prepareState().get().getState().getRoutingTable()
                .index(index).shard(0).primaryShard();
            // the node that's part of the same partition as master is now the primary for the table shard
            assertThat(primaryShard.currentNodeId(), equalTo(nonIsolatedDataNodeId));
            assertTrue(primaryShard.active());
        }, 30, TimeUnit.SECONDS);

        execute("update registers set value = 'value set on master' where id = 1", null, masterNodeName);
        execute("update registers set value = 'value set on master the second time' where id = 1",
                null,
                masterNodeName);

        logger.info("heal disruption");
        partition.stopDisrupting();

        logger.info("wait for cluster to get into a green state");
        ensureGreen();

        execute("select value, _seq_no, _primary_term from registers where id = 1", null, masterNodeName);
        String finalValue = (String) response.rows()[0][0];
        long finalSequenceNumber = (long) response.rows()[0][1];
        long finalPrimaryTerm = (long) response.rows()[0][2];

        assertThat("We executed 2 updates on the new primary", finalSequenceNumber, is(2L));
        assertThat("Primary promotion should've triggered a bump in primary term", finalPrimaryTerm, equalTo(2L));
        assertThat(finalValue, equalTo("value set on master the second time"));
    }
}
