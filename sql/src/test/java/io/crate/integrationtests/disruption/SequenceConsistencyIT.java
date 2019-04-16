/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests.disruption;

import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.IndexParts;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SequenceConsistencyIT extends SQLTransportIntegrationTest {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    @Test
    public void testVersionIsUniqueForEachValueFails() throws Throwable {
        final Settings sharedSettings = Settings.builder()
            .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s") // for hitting simulated network failures quickly
            .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1") // for hitting simulated network failures quickly
            .put(ZenDiscovery.JOIN_TIMEOUT_SETTING.getKey(),
                 "10s")  // still long to induce failures but to long so test won't time out
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(),
                 "1s") // <-- for hitting simulated network failures quickly
            .putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "file")
            .build();

        logger.info("starting 3 nodes");
        String masterNode = internalCluster().startMasterOnlyNode(sharedSettings);
        String dataNode1 = internalCluster().startDataOnlyNode(sharedSettings);
        String dataNode2 = internalCluster().startDataOnlyNode(sharedSettings);

        logger.info("wait for all nodes to join the cluster");
        ensureGreen();

        execute("create table registers (id int, value string) CLUSTERED INTO 1 shards with (number_of_replicas = 1)");
        execute("insert into registers values (1, 'initial value')");
        refresh();

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String dataNode1Id = clusterState.getNodes().resolveNode(dataNode1).getId();
        String dataNode2Id = clusterState.getNodes().resolveNode(dataNode2).getId();

        boolean dataNode1HasPrimary = clusterState.getRoutingNodes().node(dataNode1Id).copyShards().get(0).primary();

        // isolate node with the primary
        String isolatedNode = dataNode1HasPrimary ? dataNode1 : dataNode2;
        Set<String> otherNodes = dataNode1HasPrimary ? Sets.newHashSet(masterNode, dataNode2) :
            Sets.newHashSet(masterNode, dataNode1);

        NetworkDisruption partition = new NetworkDisruption(new NetworkDisruption.TwoPartitions(otherNodes,
                                                                                                Set.of(isolatedNode)),
                                                            new NetworkDisruption.NetworkDisconnect());
        internalCluster().setDisruptionScheme(partition);

        logger.info("start disrupting network");
        partition.startDisrupting();

        String schema = sqlExecutor.getCurrentSchema();
        try {
            systemExecute("update registers set value = 'value whilst disrupted' where id = 1",
                          schema,
                          isolatedNode);
            fail("expected timeout exception");
        } catch (ElasticsearchTimeoutException elasticsearchTimeoutException) {
        }

        String nonIsolatedDataNodeId = dataNode1HasPrimary ? dataNode2Id : dataNode1Id;
        logger.info("wait for replica on the partition with the master to be promoted to primary");
        assertBusy(() -> {
            String index = IndexParts.toIndexName(schema, "registers", null);
            ShardRouting primaryShard = client(masterNode).admin().cluster().prepareState().get().getState().getRoutingTable()
                .index(index).shard(0).primaryShard();
            assertThat(primaryShard.currentNodeId(), equalTo(nonIsolatedDataNodeId));
            assertTrue(primaryShard.active());
        });

        systemExecute("update registers set value = 'value set on master'",
                      schema,
                      masterNode);
        systemExecute("select _seq_no, _primary_term from registers",
                      schema, masterNode);
        long sequenceNoAfterUpdate = (long) response.rows()[0][0];
        long primaryTermAfterUpdate = (long) response.rows()[0][1];

        logger.info("heal disruption");
        partition.stopDisrupting();

        logger.info("wait for cluster to get into a green state");
        ensureGreen();

        execute("select value, _seq_no, _primary_term from registers");
        String finalValue = (String) response.rows()[0][0];
        long finalSquenceNo = (long) response.rows()[0][1];
        long finalPrimaryTerm = (long) response.rows()[0][2];

        assertThat(finalValue, equalTo("somethingelse"));
        assertThat(finalSquenceNo, equalTo(sequenceNoAfterUpdate));
        assertThat(finalPrimaryTerm, equalTo(primaryTermAfterUpdate));
    }
}
