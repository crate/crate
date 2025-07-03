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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.junit.Test;

import io.crate.auth.Protocol;
import io.crate.integrationtests.disruption.discovery.AbstractDisruptionTestCase;
import io.crate.metadata.RelationName;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.session.Session;
import io.crate.session.Sessions;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@IntegTestCase.Slow
public class SequenceConsistencyIT extends AbstractDisruptionTestCase {


    @Test
    public void testPrimaryTermIsIncreasedOnReplicaPromotion() throws Throwable {
        logger.info("starting 3 nodes");
        String masterNodeName = cluster().startMasterOnlyNode(DEFAULT_SETTINGS);
        String firstDataNodeName = cluster().startDataOnlyNode(DEFAULT_SETTINGS);
        String secondDataNodeName = cluster().startDataOnlyNode(DEFAULT_SETTINGS);

        logger.info("wait for all nodes to join the cluster");
        ensureGreen();

        execute(
            """
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
        execute("refresh table registers");

        // not setting this when creating the table because we want the replica to be initialized before we start disrupting
        // otherwise the replica might not be ready to be promoted to primary due to "primary failed while replica initializing"
        execute("alter table registers set (\"write.wait_for_active_shards\" = 1)");

        ClusterState clusterState = client().state(new ClusterStateRequest()).get().getState();
        String firstDataNodeId = clusterState.nodes().resolveNode(firstDataNodeName).getId();
        String secondDataNodeId = clusterState.nodes().resolveNode(secondDataNodeName).getId();

        boolean firstDataNodeHasPrimary = clusterState.getRoutingNodes().node(firstDataNodeId).copyShards().get(0).primary();

        // isolate node with the primary
        String isolatedNode = firstDataNodeHasPrimary ? firstDataNodeName : secondDataNodeName;
        Set<String> otherNodes = firstDataNodeHasPrimary ? Set.of(masterNodeName, secondDataNodeName) :
            Set.of(masterNodeName, firstDataNodeName);

        NetworkDisruption partition = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(otherNodes, Set.of(isolatedNode)),
            new NetworkDisruption.NetworkDisconnect()
        );
        cluster().setDisruptionScheme(partition);

        logger.info("start disrupting network");
        partition.startDisrupting();

        String schema = sqlExecutor.getCurrentSchema();
        Sessions sessions = cluster().getInstance(Sessions.class, isolatedNode);
        ConnectionProperties connectionProperties = new ConnectionProperties(null, null, Protocol.HTTP, null);
        Session session = sessions.newSession(connectionProperties, schema, Role.CRATE_USER);
        CompletableFuture<SQLResponse> firstUpdate = SQLTransportExecutor.execute(
            "update registers set value = 'value whilst disrupted' where id = 1", null, session);
        firstUpdate.whenComplete((_, _) -> session.close());
        assertThat(firstUpdate).isNotDone();

        String nonIsolatedDataNodeId = firstDataNodeHasPrimary ? secondDataNodeId : firstDataNodeId;
        logger.info("wait for replica on the partition with the master to be promoted to primary");
        assertBusy(() -> {
            ClusterState state = client(masterNodeName).state(new ClusterStateRequest()).get().getState();
            String indexUUID = state.metadata()
                .getIndex(new RelationName(schema, "registers"), List.of(), true, IndexMetadata::getIndexUUID);
            ShardRouting primaryShard = state.routingTable().index(indexUUID).shard(0).primaryShard();
            // the node that's part of the same partition as master is now the primary for the table shard
            assertThat(primaryShard.currentNodeId()).isEqualTo(nonIsolatedDataNodeId);
            assertThat(primaryShard.active()).isTrue();
        }, 30, TimeUnit.SECONDS);

        execute("update registers set value = 'value set on master' where id = 1", null, masterNodeName);
        execute("update registers set value = 'value set on master the second time' where id = 1",
                null,
                masterNodeName);

        assertThat(firstUpdate).isNotDone();

        logger.info("heal disruption");
        partition.stopDisrupting();

        logger.info("wait for cluster to get into a green state");
        ensureGreen();

        execute("refresh table registers");

        assertBusy(() -> {
            execute("select value, _seq_no, _primary_term from registers where id = 1", null, masterNodeName);
            String finalValue = (String) response.rows()[0][0];
            long finalSequenceNumber = (long) response.rows()[0][1];
            long finalPrimaryTerm = (long) response.rows()[0][2];

            assertThat(finalSequenceNumber).satisfiesAnyOf(
                x -> {
                    assertThat(firstUpdate).isNotDone();
                    assertThat(x)
                        .as("We executed 2 updates on the new primary")
                        .isEqualTo(2L);
                    assertThat(finalPrimaryTerm)
                        .as("Primary promotion should've triggered a bump in primary term")
                        .isEqualTo(2L);
                    assertThat(finalValue).isEqualTo("value set on master the second time");
                },
                x -> {
                    assertThat(firstUpdate).isDone();
                    assertThat(x)
                        .as("We executed 3 updates on the new primary")
                        .isEqualTo(3L);
                    assertThat(finalPrimaryTerm)
                        .as("Primary promotion should've triggered a bump in primary term")
                        .isEqualTo(2L);
                    assertThat(finalValue).isEqualTo("value whilst disrupted");
                }
            );
        });
    }
}
