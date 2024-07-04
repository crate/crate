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
import static org.assertj.core.api.Assertions.fail;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;


/**
 * Tests for discovery during disruptions.
 */
@TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE")
@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
@IntegTestCase.Slow
public class DiscoveryDisruptionIT extends AbstractDisruptionTestCase {

    /**
     * Test cluster join with issues in cluster state publishing *
     */
    @Test
    public void testClusterJoinDespiteOfPublishingIssues() throws Exception {
        String masterNode = cluster().startMasterOnlyNode();
        String nonMasterNode = cluster().startDataOnlyNode();

        DiscoveryNodes discoveryNodes = cluster().getInstance(ClusterService.class, nonMasterNode).state().nodes();

        TransportService masterTranspotService =
                cluster().getInstance(TransportService.class, discoveryNodes.getMasterNode().getName());

        logger.info("blocking requests from non master [{}] to master [{}]", nonMasterNode, masterNode);
        MockTransportService nonMasterTransportService = (MockTransportService) cluster().getInstance(
            TransportService.class,
            nonMasterNode);
        nonMasterTransportService.addFailToSendNoConnectRule(masterTranspotService);

        assertNoMaster(nonMasterNode);

        logger.info("blocking cluster state publishing from master [{}] to non master [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService =
                (MockTransportService) cluster().getInstance(TransportService.class, masterNode);
        TransportService localTransportService =
                cluster().getInstance(TransportService.class, discoveryNodes.getLocalNode().getName());
        if (randomBoolean()) {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME);
        } else {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, PublicationTransportHandler.COMMIT_STATE_ACTION_NAME);
        }

        logger.info("allowing requests from non master [{}] to master [{}], waiting for two join request", nonMasterNode, masterNode);
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        nonMasterTransportService.addSendBehavior(masterTransportService, (connection, requestId, action, request, options) -> {
            if (action.equals(JoinHelper.JOIN_ACTION_NAME)) {
                countDownLatch.countDown();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        nonMasterTransportService.addConnectBehavior(masterTransportService, Transport::openConnection);

        countDownLatch.await();

        logger.info("waiting for cluster to reform");
        masterTransportService.clearOutboundRules(localTransportService);
        nonMasterTransportService.clearOutboundRules(localTransportService);

        ensureStableCluster(2);

        // shutting down the nodes, to avoid the leakage check tripping
        // on the states associated with the commit requests we may have dropped
        cluster().stopRandomNonMasterNode();
    }

    @Test
    public void testClusterFormingWithASlowNode() {

        SlowClusterStateProcessing disruption = new SlowClusterStateProcessing(random(), 0, 0, 1000, 2000);

        // don't wait for initial state, we want to add the disruption while the cluster is forming
        cluster().startNodes(3);

        logger.info("applying disruption while cluster is forming ...");

        cluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        ensureStableCluster(3);
    }

    @Test
    public void testElectMasterWithLatestVersion() throws Exception {
        final Set<String> nodes = new HashSet<>(cluster().startNodes(3));
        ensureStableCluster(3);
        ServiceDisruptionScheme isolateAllNodes =
                new NetworkDisruption(new NetworkDisruption.IsolateAllNodes(nodes), new NetworkDisconnect());
        cluster().setDisruptionScheme(isolateAllNodes);

        logger.info("--> forcing a complete election to make sure \"preferred\" master is elected");
        isolateAllNodes.startDisrupting();
        for (String node : nodes) {
            assertNoMaster(node);
        }
        cluster().clearDisruptionScheme();
        ensureStableCluster(3);
        final String preferredMasterName = cluster().getMasterName();
        final DiscoveryNode preferredMaster = cluster().clusterService(preferredMasterName).localNode();
        logger.info("--> preferred master is {}", preferredMaster);
        final Set<String> nonPreferredNodes = new HashSet<>(nodes);
        nonPreferredNodes.remove(preferredMasterName);
        final ServiceDisruptionScheme isolatePreferredMaster =
                new NetworkDisruption(
                        new NetworkDisruption.TwoPartitions(
                                Collections.singleton(preferredMasterName), nonPreferredNodes),
                        new NetworkDisconnect());
        cluster().setDisruptionScheme(isolatePreferredMaster);
        isolatePreferredMaster.startDisrupting();

        execute("create table t (id int primary key, x string) clustered into 1 shards " +
                "with (number_of_replicas = 0)", null, randomFrom(nonPreferredNodes));

        cluster().clearDisruptionScheme(false);
        cluster().setDisruptionScheme(isolateAllNodes);

        logger.info("--> forcing a complete election again");
        isolateAllNodes.startDisrupting();
        for (String node : nodes) {
            assertNoMaster(node);
        }

        isolateAllNodes.stopDisrupting();

        final ClusterState state = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        if (state.metadata().hasIndex(toIndexName(sqlExecutor.getCurrentSchema(), "t", null)) == false) {
            fail("index 'test' was lost. current cluster state: " + state);
        }

    }

    /**
     * Adds an asymmetric break between a master and one of the nodes and makes
     * sure that the node is removed form the cluster, that the node start pinging and that
     * the cluster reforms when healed.
     */
    @Test
    public void testNodeNotReachableFromMaster() throws Exception {
        startCluster(3);

        String masterNode = cluster().getMasterName();
        String nonMasterNode = null;
        while (nonMasterNode == null) {
            nonMasterNode = randomFrom(cluster().getNodeNames());
            if (nonMasterNode.equals(masterNode)) {
                nonMasterNode = null;
            }
        }

        logger.info("blocking request from master [{}] to [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService = (MockTransportService) cluster().getInstance(
            TransportService.class,
            masterNode);
        if (randomBoolean()) {
            masterTransportService.addUnresponsiveRule(cluster().getInstance(TransportService.class, nonMasterNode));
        } else {
            masterTransportService.addFailToSendNoConnectRule(cluster().getInstance(TransportService.class, nonMasterNode));
        }

        logger.info("waiting for [{}] to be removed from cluster", nonMasterNode);
        ensureStableCluster(2, masterNode);

        logger.info("waiting for [{}] to have no master", nonMasterNode);
        assertNoMaster(nonMasterNode);

        logger.info("healing partition and checking cluster reforms");
        masterTransportService.clearAllRules();

        ensureStableCluster(3);
    }

}
