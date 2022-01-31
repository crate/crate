/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.coordination;

import static org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.DEFAULT_DELAY_VARIABILITY;
import static org.elasticsearch.cluster.coordination.Coordinator.PUBLISH_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.CANDIDATE;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ALL;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_SETTING;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_WRITES;
import static org.elasticsearch.cluster.coordination.Reconfigurator.CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.ClusterNode;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.MockLogAppender;



@AwaitsFix(bugUrl = "https://github.com/crate/crate/issues/10811")
public class CoordinatorTests extends AbstractCoordinatorTestCase {

    /**
     * This test was added to verify that state recovery is properly reset on a node after it has become master and successfully
     * recovered a state (see {@link GatewayService}). The situation which triggers this with a decent likelihood is as follows:
     * 3 master-eligible nodes (leader, follower1, follower2), the followers are shut down (leader remains), when followers come back
     * one of them becomes leader and publishes first state (with STATE_NOT_RECOVERED_BLOCK) to old leader, which accepts it.
     * Old leader is initiating an election at the same time, and wins election. It becomes leader again, but as it previously
     * successfully completed state recovery, is never reset to a state where state recovery can be retried.
     */
    public void testStateRecoveryResetAfterPreviousLeadership() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower2 = cluster.getAnyNodeExcept(leader, follower1);

            // restart follower1 and follower2
            for (ClusterNode clusterNode : Arrays.asList(follower1, follower2)) {
                clusterNode.close();
                cluster.clusterNodes.forEach(
                    cn -> cluster.deterministicTaskQueue.scheduleNow(cn.onNode(
                        new Runnable() {
                            @Override
                            public void run() {
                                cn.transportService.disconnectFromNode(clusterNode.getLocalNode());
                            }

                            @Override
                            public String toString() {
                                return "disconnect from " + clusterNode.getLocalNode() + " after shutdown";
                            }
                        })));
                cluster.clusterNodes.replaceAll(cn -> cn == clusterNode ? cn.restartedNode() : cn);
            }

            cluster.stabilise();
        }
    }

    public void testCanUpdateClusterStateAfterStabilisation() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            long finalValue = randomLong();

            logger.info("--> submitting value [{}] to [{}]", finalValue, leader);
            leader.submitValue(finalValue);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                final String nodeId = clusterNode.getId();
                final ClusterState appliedState = clusterNode.getLastAppliedClusterState();
                assertThat(nodeId + " has the applied value", value(appliedState), is(finalValue));
            }
        }
    }

    public void testDoesNotElectNonMasterNode() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5), false, Settings.EMPTY)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            assertTrue(leader.getLocalNode().isMasterEligibleNode());
        }
    }

    public void testNodesJoinAfterStableCluster() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final long currentTerm = cluster.getAnyLeader().coordinator.getCurrentTerm();
            cluster.addNodesAndStabilise(randomIntBetween(1, 2));

            final long newTerm = cluster.getAnyLeader().coordinator.getCurrentTerm();
            assertEquals(currentTerm, newTerm);
        }
    }

    public void testExpandsConfigurationWhenGrowingFromOneNodeToThreeButDoesNotShrink() {
        try (Cluster cluster = new Cluster(1)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();

            cluster.addNodesAndStabilise(2);

            {
                assertThat(leader.coordinator.getMode(), is(Mode.LEADER));
                final VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState().getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be all nodes", lastCommittedConfiguration.getNodeIds(),
                    equalTo(cluster.clusterNodes.stream().map(ClusterNode::getId).collect(Collectors.toSet())));
            }

            final ClusterNode disconnect1 = cluster.getAnyNode();
            logger.info("--> disconnecting {}", disconnect1);
            disconnect1.disconnect();
            cluster.stabilise();

            {
                final ClusterNode newLeader = cluster.getAnyLeader();
                final VotingConfiguration lastCommittedConfiguration
                    = newLeader.getLastAppliedClusterState().getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be all nodes", lastCommittedConfiguration.getNodeIds(),
                    equalTo(cluster.clusterNodes.stream().map(ClusterNode::getId).collect(Collectors.toSet())));
            }
        }
    }

    public void testExpandsConfigurationWhenGrowingFromThreeToFiveNodesAndShrinksBackToThreeOnFailure() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();

            logger.info("setting auto-shrink reconfiguration to true");
            leader.submitSetAutoShrinkVotingConfiguration(true);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            assertTrue(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metadata().settings()));

            cluster.addNodesAndStabilise(2);

            {
                assertThat(leader.coordinator.getMode(), is(Mode.LEADER));
                final VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState().getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be all nodes", lastCommittedConfiguration.getNodeIds(),
                    equalTo(cluster.clusterNodes.stream().map(ClusterNode::getId).collect(Collectors.toSet())));
            }

            final ClusterNode disconnect1 = cluster.getAnyNode();
            final ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);
            logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
            disconnect1.disconnect();
            disconnect2.disconnect();
            cluster.stabilise();

            {
                final ClusterNode newLeader = cluster.getAnyLeader();
                final VotingConfiguration lastCommittedConfiguration
                    = newLeader.getLastAppliedClusterState().getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect1.getId()));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect2.getId()));
            }

            // we still tolerate the loss of one more node here

            final ClusterNode disconnect3 = cluster.getAnyNodeExcept(disconnect1, disconnect2);
            logger.info("--> disconnecting {}", disconnect3);
            disconnect3.disconnect();
            cluster.stabilise();

            {
                final ClusterNode newLeader = cluster.getAnyLeader();
                final VotingConfiguration lastCommittedConfiguration
                    = newLeader.getLastAppliedClusterState().getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect1.getId()));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect2.getId()));
                assertTrue(lastCommittedConfiguration.getNodeIds().contains(disconnect3.getId()));
            }

            // however we do not tolerate the loss of yet another one

            final ClusterNode disconnect4 = cluster.getAnyNodeExcept(disconnect1, disconnect2, disconnect3);
            logger.info("--> disconnecting {}", disconnect4);
            disconnect4.disconnect();
            cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(clusterNode.getId() + " should be a candidate", clusterNode.coordinator.getMode(), equalTo(Mode.CANDIDATE));
            }

            // moreover we are still stuck even if two other nodes heal
            logger.info("--> healing {} and {}", disconnect1, disconnect2);
            disconnect1.heal();
            disconnect2.heal();
            cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(clusterNode.getId() + " should be a candidate", clusterNode.coordinator.getMode(), equalTo(Mode.CANDIDATE));
            }

            // we require another node to heal to recover
            final ClusterNode toHeal = randomBoolean() ? disconnect3 : disconnect4;
            logger.info("--> healing {}", toHeal);
            toHeal.heal();
            cluster.stabilise();
        }
    }

    public void testCanShrinkFromFiveNodesToThree() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.runRandomly();
            cluster.stabilise();

            {
                final ClusterNode leader = cluster.getAnyLeader();
                logger.info("setting auto-shrink reconfiguration to false");
                leader.submitSetAutoShrinkVotingConfiguration(false);
                cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
                assertFalse(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metadata().settings()));
            }

            final ClusterNode disconnect1 = cluster.getAnyNode();
            final ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);

            logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
            disconnect1.disconnect();
            disconnect2.disconnect();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();

            {
                final VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState().getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be all nodes", lastCommittedConfiguration.getNodeIds(),
                    equalTo(cluster.clusterNodes.stream().map(ClusterNode::getId).collect(Collectors.toSet())));
            }

            logger.info("setting auto-shrink reconfiguration to true");
            leader.submitSetAutoShrinkVotingConfiguration(true);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY * 2); // allow for a reconfiguration
            assertTrue(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metadata().settings()));

            {
                final VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState().getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect1.getId()));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect2.getId()));
            }
        }
    }

    public void testDoesNotShrinkConfigurationBelowThreeNodes() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode disconnect1 = cluster.getAnyNode();

            logger.info("--> disconnecting {}", disconnect1);
            disconnect1.disconnect();
            cluster.stabilise();

            final ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);
            logger.info("--> disconnecting {}", disconnect2);
            disconnect2.disconnect();
            cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(clusterNode.getId() + " should be a candidate", clusterNode.coordinator.getMode(), equalTo(Mode.CANDIDATE));
            }

            disconnect1.heal();
            cluster.stabilise(); // would not work if disconnect1 were removed from the configuration
        }
    }

    public void testDoesNotShrinkConfigurationBelowFiveNodesIfAutoShrinkDisabled() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.runRandomly();
            cluster.stabilise();

            cluster.getAnyLeader().submitSetAutoShrinkVotingConfiguration(false);
            cluster.stabilise(DEFAULT_ELECTION_DELAY);

            final ClusterNode disconnect1 = cluster.getAnyNode();
            final ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);

            logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
            disconnect1.disconnect();
            disconnect2.disconnect();
            cluster.stabilise();

            final ClusterNode disconnect3 = cluster.getAnyNodeExcept(disconnect1, disconnect2);
            logger.info("--> disconnecting {}", disconnect3);
            disconnect3.disconnect();
            cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(clusterNode.getId() + " should be a candidate", clusterNode.coordinator.getMode(), equalTo(Mode.CANDIDATE));
            }

            disconnect1.heal();
            cluster.stabilise(); // would not work if disconnect1 were removed from the configuration
        }
    }

    public void testLeaderDisconnectionWithDisconnectEventDetectedQuickly() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode originalLeader = cluster.getAnyLeader();
            logger.info("--> disconnecting leader {}", originalLeader);
            originalLeader.disconnect();
            logger.info("--> followers get disconnect event for leader {} ", originalLeader);
            cluster.getAllNodesExcept(originalLeader).forEach(cn -> cn.onDisconnectEventFrom(originalLeader));
            // turn leader into candidate, which stabilisation asserts at the end
            cluster.getAllNodesExcept(originalLeader).forEach(originalLeader::onDisconnectEventFrom);
            cluster.stabilise(DEFAULT_DELAY_VARIABILITY // disconnect is scheduled
                // then wait for a new election
                + DEFAULT_ELECTION_DELAY
                // wait for the removal to be committed
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                // then wait for the followup reconfiguration
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
        }
    }

    public void testLeaderDisconnectionWithoutDisconnectEventDetectedQuickly() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode originalLeader = cluster.getAnyLeader();
            logger.info("--> disconnecting leader {}", originalLeader);
            originalLeader.disconnect();

            cluster.stabilise(Math.max(
                // Each follower may have just sent a leader check, which receives no response
                defaultMillis(LEADER_CHECK_TIMEOUT_SETTING)
                    // then wait for the follower to check the leader
                    + defaultMillis(LEADER_CHECK_INTERVAL_SETTING)
                    // then wait for the exception response
                    + DEFAULT_DELAY_VARIABILITY
                    // then wait for a new election
                    + DEFAULT_ELECTION_DELAY,

                // ALSO the leader may have just sent a follower check, which receives no response
                defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)
                    // wait for the leader to check its followers
                    + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING)
                    // then wait for the exception response
                    + DEFAULT_DELAY_VARIABILITY)

                // FINALLY:

                // wait for the removal to be committed
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                // then wait for the followup reconfiguration
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

            assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
        }
    }

    public void testUnresponsiveLeaderDetectedEventually() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode originalLeader = cluster.getAnyLeader();
            logger.info("--> blackholing leader {}", originalLeader);
            originalLeader.blackhole();

            // This stabilisation time bound is undesirably long. TODO try and reduce it.
            cluster.stabilise(Math.max(
                // first wait for all the followers to notice the leader has gone
                (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING))
                    * defaultInt(LEADER_CHECK_RETRY_COUNT_SETTING)
                    // then wait for a follower to be promoted to leader
                    + DEFAULT_ELECTION_DELAY
                    // and the first publication times out because of the unresponsive node
                    + defaultMillis(PUBLISH_TIMEOUT_SETTING)
                    // there might be a term bump causing another election
                    + DEFAULT_ELECTION_DELAY

                    // then wait for both of:
                    + Math.max(
                    // 1. the term bumping publication to time out
                    defaultMillis(PUBLISH_TIMEOUT_SETTING),
                    // 2. the new leader to notice that the old leader is unresponsive
                    (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
                        * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING))

                    // then wait for the new leader to commit a state without the old leader
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // then wait for the followup reconfiguration
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

                // ALSO wait for the leader to notice that its followers are unresponsive
                (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
                    * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING)
                    // then wait for the leader to try and commit a state removing them, causing it to stand down
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            ));

            assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
        }
    }

    public void testFollowerDisconnectionDetectedQuickly() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower = cluster.getAnyNodeExcept(leader);
            logger.info("--> disconnecting follower {}", follower);
            follower.disconnect();
            logger.info("--> leader {} and follower {} get disconnect event", leader, follower);
            leader.onDisconnectEventFrom(follower);
            follower.onDisconnectEventFrom(leader); // to turn follower into candidate, which stabilisation asserts at the end
            cluster.stabilise(DEFAULT_DELAY_VARIABILITY // disconnect is scheduled
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                // then wait for the followup reconfiguration
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            assertThat(cluster.getAnyLeader().getId(), equalTo(leader.getId()));
        }
    }

    public void testFollowerDisconnectionWithoutDisconnectEventDetectedQuickly() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower = cluster.getAnyNodeExcept(leader);
            logger.info("--> disconnecting follower {}", follower);
            follower.disconnect();
            cluster.stabilise(Math.max(
                // the leader may have just sent a follower check, which receives no response
                defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)
                    // wait for the leader to check the follower
                    + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING)
                    // then wait for the exception response
                    + DEFAULT_DELAY_VARIABILITY
                    // then wait for the removal to be committed
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // then wait for the followup reconfiguration
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

                // ALSO the follower may have just sent a leader check, which receives no response
                defaultMillis(LEADER_CHECK_TIMEOUT_SETTING)
                    // then wait for the follower to check the leader
                    + defaultMillis(LEADER_CHECK_INTERVAL_SETTING)
                    // then wait for the exception response, causing the follower to become a candidate
                    + DEFAULT_DELAY_VARIABILITY
            ));
            assertThat(cluster.getAnyLeader().getId(), equalTo(leader.getId()));
        }
    }

    public void testUnresponsiveFollowerDetectedEventually() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower = cluster.getAnyNodeExcept(leader);
            logger.info("--> blackholing follower {}", follower);
            follower.blackhole();

            cluster.stabilise(Math.max(
                // wait for the leader to notice that the follower is unresponsive
                (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
                    * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING)
                    // then wait for the leader to commit a state without the follower
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // then wait for the followup reconfiguration
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

                // ALSO wait for the follower to notice the leader is unresponsive
                (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING))
                    * defaultInt(LEADER_CHECK_RETRY_COUNT_SETTING)
            ));
            assertThat(cluster.getAnyLeader().getId(), equalTo(leader.getId()));
        }
    }

    public void testAckListenerReceivesAcksFromAllNodes() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                assertTrue("expected ack from " + clusterNode, ackCollector.hasAckedSuccessfully(clusterNode));
            }
            assertThat("leader should be last to ack", ackCollector.getSuccessfulAckIndex(leader),
                equalTo(cluster.clusterNodes.size() - 1));
        }
    }

    public void testAckListenerReceivesNackFromFollower() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

            follower0.allowClusterStateApplicationFailure();
            follower0.setClusterStateApplyResponse(ClusterStateApplyResponse.FAIL);
            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            assertTrue("expected ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
            assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
            assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
            assertThat("leader should be last to ack", ackCollector.getSuccessfulAckIndex(leader), equalTo(1));
        }
    }

    public void testAckListenerReceivesNackFromLeader() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);
            final long startingTerm = leader.coordinator.getCurrentTerm();

            leader.allowClusterStateApplicationFailure();
            leader.setClusterStateApplyResponse(ClusterStateApplyResponse.FAIL);
            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing value");
            assertTrue(leader.coordinator.getMode() != Coordinator.Mode.LEADER || leader.coordinator.getCurrentTerm() > startingTerm);
            leader.setClusterStateApplyResponse(ClusterStateApplyResponse.SUCCEED);
            cluster.stabilise();
            assertTrue("expected nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
            assertTrue("expected ack from " + follower0, ackCollector.hasAckedSuccessfully(follower0));
            assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
            assertTrue(leader.coordinator.getMode() != Coordinator.Mode.LEADER || leader.coordinator.getCurrentTerm() > startingTerm);
        }
    }

    public void testAckListenerReceivesNoAckFromHangingFollower() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

            logger.info("--> blocking cluster state application on {}", follower0);
            follower0.setClusterStateApplyResponse(ClusterStateApplyResponse.HANG);

            logger.info("--> publishing another value");
            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing value");

            assertTrue("expected immediate ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
            assertFalse("expected no ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
            cluster.stabilise(defaultMillis(PUBLISH_TIMEOUT_SETTING));
            assertTrue("expected eventual ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
            assertFalse("expected no ack from " + follower0, ackCollector.hasAcked(follower0));
        }
    }

    public void testAckListenerReceivesNacksIfPublicationTimesOut() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

            follower0.blackhole();
            follower1.blackhole();
            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing value");
            assertFalse("expected no immediate ack from " + leader, ackCollector.hasAcked(leader));
            assertFalse("expected no immediate ack from " + follower0, ackCollector.hasAcked(follower0));
            assertFalse("expected no immediate ack from " + follower1, ackCollector.hasAcked(follower1));
            follower0.heal();
            follower1.heal();
            cluster.stabilise();
            assertTrue("expected eventual nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
            assertTrue("expected eventual nack from " + follower1, ackCollector.hasAckedUnsuccessfully(follower1));
            assertTrue("expected eventual nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
        }
    }

    public void testAckListenerReceivesNacksIfLeaderStandsDown() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

            leader.blackhole();
            follower0.onDisconnectEventFrom(leader);
            follower1.onDisconnectEventFrom(leader);
            // let followers elect a leader among themselves before healing the leader and running the publication
            cluster.runFor(DEFAULT_DELAY_VARIABILITY // disconnect is scheduled
                + DEFAULT_ELECTION_DELAY, "elect new leader");
            // cluster has two nodes in mode LEADER, in different terms ofc, and the one in the lower term won’t be able to publish anything
            leader.heal();
            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.stabilise(); // TODO: check if can find a better bound here
            assertTrue("expected nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
            assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
            assertTrue("expected nack from " + follower1, ackCollector.hasAckedUnsuccessfully(follower1));
        }
    }

    public void testAckListenerReceivesNacksFromFollowerInHigherTerm() {
        // TODO: needs proper term bumping
//        final Cluster cluster = new Cluster(3);
//        cluster.runRandomly();
//        cluster.stabilise();
//        final ClusterNode leader = cluster.getAnyLeader();
//        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
//        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);
//
//        follower0.coordinator.joinLeaderInTerm(new StartJoinRequest(follower0.localNode, follower0.coordinator.getCurrentTerm() + 1));
//        AckCollector ackCollector = leader.submitValue(randomLong());
//        cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
//        assertTrue("expected ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
//        assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
//        assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
    }

    public void testSettingInitialConfigurationTriggersElection() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runFor(defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2 + randomLongBetween(0, 60000),
                "initial discovery phase");
            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                final String nodeId = clusterNode.getId();
                assertThat(nodeId + " is CANDIDATE", clusterNode.coordinator.getMode(), is(CANDIDATE));
                assertThat(nodeId + " is in term 0", clusterNode.coordinator.getCurrentTerm(), is(0L));
                assertThat(nodeId + " last accepted in term 0", clusterNode.coordinator.getLastAcceptedState().term(), is(0L));
                assertThat(nodeId + " last accepted version 0", clusterNode.coordinator.getLastAcceptedState().version(), is(0L));
                assertFalse(nodeId + " has not received an initial configuration", clusterNode.coordinator.isInitialConfigurationSet());
                assertTrue(nodeId + " has an empty last-accepted configuration",
                    clusterNode.coordinator.getLastAcceptedState().getLastAcceptedConfiguration().isEmpty());
                assertTrue(nodeId + " has an empty last-committed configuration",
                    clusterNode.coordinator.getLastAcceptedState().getLastCommittedConfiguration().isEmpty());

                final Set<DiscoveryNode> foundPeers = new HashSet<>();
                clusterNode.coordinator.getFoundPeers().forEach(foundPeers::add);
                assertTrue(nodeId + " should not have discovered itself", foundPeers.add(clusterNode.getLocalNode()));
                assertThat(nodeId + " should have found all peers", foundPeers, hasSize(cluster.size()));
            }

            final ClusterNode bootstrapNode = cluster.getAnyBootstrappableNode();
            bootstrapNode.applyInitialConfiguration();
            assertTrue(bootstrapNode.getId() + " has been bootstrapped", bootstrapNode.coordinator.isInitialConfigurationSet());

            cluster.stabilise(
                // the first election should succeed, because only one node knows of the initial configuration and therefore can win a
                // pre-voting round and proceed to an election, so there cannot be any collisions
                defaultMillis(ELECTION_INITIAL_TIMEOUT_SETTING) // TODO this wait is unnecessary, we could trigger the election immediately
                    // Allow two round-trip for pre-voting and voting
                    + 4 * DEFAULT_DELAY_VARIABILITY
                    // Then a commit of the new leader's first cluster state
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // Then allow time for all the other nodes to join, each of which might cause a reconfiguration
                    + (cluster.size() - 1) * 2 * DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                // TODO Investigate whether 4 publications is sufficient due to batching? A bound linear in the number of nodes isn't great.
            );
        }
    }

    public void testCannotSetInitialConfigurationTwice() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final Coordinator coordinator = cluster.getAnyNode().coordinator;
            assertFalse(coordinator.setInitialConfiguration(coordinator.getLastAcceptedState().getLastCommittedConfiguration()));
        }
    }

    public void testCannotSetInitialConfigurationWithoutQuorum() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            final Coordinator coordinator = cluster.getAnyNode().coordinator;
            final VotingConfiguration unknownNodeConfiguration = new VotingConfiguration(
                Set.of(coordinator.getLocalNode().getId(), "unknown-node"));
            final String exceptionMessage = expectThrows(CoordinationStateRejectedException.class,
                () -> coordinator.setInitialConfiguration(unknownNodeConfiguration)).getMessage();
            assertThat(exceptionMessage,
                startsWith("not enough nodes discovered to form a quorum in the initial configuration [knownNodes=["));
            assertThat(exceptionMessage, containsString("unknown-node"));
            assertThat(exceptionMessage, containsString(coordinator.getLocalNode().toString()));

            // This is VERY BAD: setting a _different_ initial configuration. Yet it works if the first attempt will never be a quorum.
            assertTrue(coordinator.setInitialConfiguration(
                new VotingConfiguration(Collections.singleton(coordinator.getLocalNode().getId()))));
            cluster.stabilise();
        }
    }

    public void testCannotSetInitialConfigurationWithoutLocalNode() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            final Coordinator coordinator = cluster.getAnyNode().coordinator;
            final VotingConfiguration unknownNodeConfiguration = new VotingConfiguration(Set.of("unknown-node"));
            final String exceptionMessage = expectThrows(CoordinationStateRejectedException.class,
                () -> coordinator.setInitialConfiguration(unknownNodeConfiguration)).getMessage();
            assertThat(exceptionMessage,
                equalTo("local node is not part of initial configuration"));
        }
    }

    public void testDiffBasedPublishing() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final long finalValue = randomLong();
            final Map<ClusterNode, PublishClusterStateStats> prePublishStats = cluster.clusterNodes.stream().collect(
                Collectors.toMap(Function.identity(), cn -> cn.coordinator.stats().getPublishStats()));
            logger.info("--> submitting value [{}] to [{}]", finalValue, leader);
            leader.submitValue(finalValue);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            final Map<ClusterNode, PublishClusterStateStats> postPublishStats = cluster.clusterNodes.stream().collect(
                Collectors.toMap(Function.identity(), cn -> cn.coordinator.stats().getPublishStats()));

            for (ClusterNode cn : cluster.clusterNodes) {
                assertThat(value(cn.getLastAppliedClusterState()), is(finalValue));
                assertEquals(cn.toString(), prePublishStats.get(cn).getFullClusterStateReceivedCount(),
                    postPublishStats.get(cn).getFullClusterStateReceivedCount());
                assertEquals(cn.toString(), prePublishStats.get(cn).getCompatibleClusterStateDiffReceivedCount() + 1,
                    postPublishStats.get(cn).getCompatibleClusterStateDiffReceivedCount());
                assertEquals(cn.toString(), prePublishStats.get(cn).getIncompatibleClusterStateDiffReceivedCount(),
                    postPublishStats.get(cn).getIncompatibleClusterStateDiffReceivedCount());
            }
        }
    }

    public void testJoiningNodeReceivesFullState() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            cluster.addNodesAndStabilise(1);
            final ClusterNode newNode = cluster.clusterNodes.get(cluster.clusterNodes.size() - 1);
            final PublishClusterStateStats newNodePublishStats = newNode.coordinator.stats().getPublishStats();
            // initial cluster state send when joining
            assertEquals(1L, newNodePublishStats.getFullClusterStateReceivedCount());
            // possible follow-up reconfiguration was published as a diff
            assertEquals(cluster.size() % 2, newNodePublishStats.getCompatibleClusterStateDiffReceivedCount());
            assertEquals(0L, newNodePublishStats.getIncompatibleClusterStateDiffReceivedCount());
        }
    }

    public void testIncompatibleDiffResendsFullState() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower = cluster.getAnyNodeExcept(leader);
            logger.info("--> blackholing {}", follower);
            follower.blackhole();
            final PublishClusterStateStats prePublishStats = follower.coordinator.stats().getPublishStats();
            logger.info("--> submitting first value to {}", leader);
            leader.submitValue(randomLong());
            cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "publish first state");
            logger.info("--> healing {}", follower);
            follower.heal();
            logger.info("--> submitting second value to {}", leader);
            leader.submitValue(randomLong());
            cluster.stabilise();
            final PublishClusterStateStats postPublishStats = follower.coordinator.stats().getPublishStats();
            assertEquals(prePublishStats.getFullClusterStateReceivedCount() + 1,
                postPublishStats.getFullClusterStateReceivedCount());
            assertEquals(prePublishStats.getCompatibleClusterStateDiffReceivedCount(),
                postPublishStats.getCompatibleClusterStateDiffReceivedCount());
            assertEquals(prePublishStats.getIncompatibleClusterStateDiffReceivedCount() + 1,
                postPublishStats.getIncompatibleClusterStateDiffReceivedCount());
        }
    }

    /**
     * Simulates a situation where a follower becomes disconnected from the leader, but only for such a short time where
     * it becomes candidate and puts up a NO_MASTER_BLOCK, but then receives a follower check from the leader. If the leader
     * does not notice the node disconnecting, it is important for the node not to be turned back into a follower but try
     * and join the leader again.
     */
    public void testStayCandidateAfterReceivingFollowerCheckFromKnownMaster() {
        try (Cluster cluster = new Cluster(2, false, Settings.EMPTY)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode nonLeader = cluster.getAnyNodeExcept(leader);
            nonLeader.onNode(() -> {
                logger.debug("forcing {} to become candidate", nonLeader.getId());
                synchronized (nonLeader.coordinator.mutex) {
                    nonLeader.coordinator.becomeCandidate("forced");
                }
                logger.debug("simulate follower check coming through from {} to {}", leader.getId(), nonLeader.getId());
                expectThrows(CoordinationStateRejectedException.class, () -> nonLeader.coordinator.onFollowerCheckRequest(
                    new FollowersChecker.FollowerCheckRequest(leader.coordinator.getCurrentTerm(), leader.getLocalNode())));
                assertThat(nonLeader.coordinator.getMode(), equalTo(CANDIDATE));
            }).run();
            cluster.stabilise();
        }
    }

    public void testAppliesNoMasterBlockWritesByDefault() {
        testAppliesNoMasterBlock(null, NO_MASTER_BLOCK_WRITES);
    }

    public void testAppliesNoMasterBlockWritesIfConfigured() {
        testAppliesNoMasterBlock("write", NO_MASTER_BLOCK_WRITES);
    }

    public void testAppliesNoMasterBlockAllIfConfigured() {
        testAppliesNoMasterBlock("all", NO_MASTER_BLOCK_ALL);
    }

    private void testAppliesNoMasterBlock(String noMasterBlockSetting, ClusterBlock expectedBlock) {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            leader.submitUpdateTask("update NO_MASTER_BLOCK_SETTING", cs -> {
                final Builder settingsBuilder = Settings.builder().put(cs.metadata().persistentSettings());
                settingsBuilder.put(NO_MASTER_BLOCK_SETTING.getKey(), noMasterBlockSetting);
                return
                    ClusterState.builder(cs).metadata(Metadata.builder(cs.metadata()).persistentSettings(settingsBuilder.build())).build();
            }, (source, e) -> {});
            cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing setting update");

            leader.disconnect();
            cluster.runFor(defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING) + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING)
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "detecting disconnection");

            assertThat(leader.getLastAppliedClusterState().blocks().global(), hasItem(expectedBlock));

            // TODO reboot the leader and verify that the same block is applied when it restarts
        }
    }

    public void testNodeCannotJoinIfJoinValidationFailsOnMaster() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 3))) {
            cluster.runRandomly();
            cluster.stabilise();

            // check that if node join validation fails on master, the nodes can't join
            List<ClusterNode> addedNodes = cluster.addNodes(randomIntBetween(1, 2));
            final Set<DiscoveryNode> validatedNodes = new HashSet<>();
            cluster.getAnyLeader().extraJoinValidators.add((discoveryNode, clusterState) -> {
                validatedNodes.add(discoveryNode);
                throw new IllegalArgumentException("join validation failed");
            });
            final long previousClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
            cluster.runFor(10000, "failing join validation");
            assertEquals(validatedNodes, addedNodes.stream().map(ClusterNode::getLocalNode).collect(Collectors.toSet()));
            assertTrue(addedNodes.stream().allMatch(ClusterNode::isCandidate));
            final long newClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
            assertEquals(previousClusterStateVersion, newClusterStateVersion);

            cluster.getAnyLeader().extraJoinValidators.clear();
            cluster.stabilise();
        }
    }

    public void testNodeCannotJoinIfJoinValidationFailsOnJoiningNode() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 3))) {
            cluster.runRandomly();
            cluster.stabilise();

            // check that if node join validation fails on joining node, the nodes can't join
            List<ClusterNode> addedNodes = cluster.addNodes(randomIntBetween(1, 2));
            final Set<DiscoveryNode> validatedNodes = new HashSet<>();
            addedNodes.forEach(cn -> cn.extraJoinValidators.add((discoveryNode, clusterState) -> {
                validatedNodes.add(discoveryNode);
                throw new IllegalArgumentException("join validation failed");
            }));
            final long previousClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
            cluster.runFor(10000, "failing join validation");
            assertEquals(validatedNodes, addedNodes.stream().map(ClusterNode::getLocalNode).collect(Collectors.toSet()));
            assertTrue(addedNodes.stream().allMatch(ClusterNode::isCandidate));
            final long newClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
            assertEquals(previousClusterStateVersion, newClusterStateVersion);

            addedNodes.forEach(cn -> cn.extraJoinValidators.clear());
            cluster.stabilise();
        }
    }

    public void testClusterCannotFormWithFailingJoinValidation() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            // fail join validation on a majority of nodes in the initial configuration
            randomValueOtherThanMany(nodes ->
                    cluster.initialConfiguration.hasQuorum(
                        nodes.stream().map(ClusterNode::getLocalNode).map(DiscoveryNode::getId).collect(Collectors.toSet())) == false,
                () -> randomSubsetOf(cluster.clusterNodes))
                .forEach(cn -> cn.extraJoinValidators.add((discoveryNode, clusterState) -> {
                    throw new IllegalArgumentException("join validation failed");
                }));
            cluster.bootstrapIfNecessary();
            cluster.runFor(10000, "failing join validation");
            assertTrue(cluster.clusterNodes.stream().allMatch(cn -> cn.getLastAppliedClusterState().version() == 0));
        }
    }

    public void testCannotJoinClusterWithDifferentUUID() throws IllegalAccessException {
        try (Cluster cluster1 = new Cluster(randomIntBetween(1, 3))) {
            cluster1.runRandomly();
            cluster1.stabilise();

            final ClusterNode nodeInOtherCluster;
            try (Cluster cluster2 = new Cluster(3)) {
                cluster2.runRandomly();
                cluster2.stabilise();
                nodeInOtherCluster = randomFrom(cluster2.clusterNodes);
            }

            final ClusterNode newNode = cluster1.new ClusterNode(nextNodeIndex.getAndIncrement(),
                nodeInOtherCluster.getLocalNode(), n -> cluster1.new MockPersistedState(n, nodeInOtherCluster.persistedState,
                Function.identity(), Function.identity()), nodeInOtherCluster.nodeSettings);

            cluster1.clusterNodes.add(newNode);

            MockLogAppender mockAppender = new MockLogAppender();
            mockAppender.start();
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test1",
                    JoinHelper.class.getCanonicalName(),
                    Level.INFO,
                    "*failed to join*"));
            Logger joinLogger = LogManager.getLogger(JoinHelper.class);
            Loggers.addAppender(joinLogger, mockAppender);
            cluster1.runFor(DEFAULT_STABILISATION_TIME, "failing join validation");
            try {
                mockAppender.assertAllExpectationsMatched();
            } finally {
                Loggers.removeAppender(joinLogger, mockAppender);
                mockAppender.stop();
            }
            assertEquals(0, newNode.getLastAppliedClusterState().version());

            newNode.close();
            final ClusterNode detachedNode = newNode.restartedNode(
                DetachClusterCommand::updateMetadata,
                term -> DetachClusterCommand.updateCurrentTerm(), newNode.nodeSettings);
            cluster1.clusterNodes.replaceAll(cn -> cn == newNode ? detachedNode : cn);
            cluster1.stabilise();
        }
    }

    public void testDiscoveryUsesNodesFromLastClusterState() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode partitionedNode = cluster.getAnyNode();
            if (randomBoolean()) {
                logger.info("--> blackholing {}", partitionedNode);
                partitionedNode.blackhole();
            } else {
                logger.info("--> disconnecting {}", partitionedNode);
                partitionedNode.disconnect();
            }
            cluster.setEmptySeedHostsList();
            cluster.stabilise();

            partitionedNode.heal();
            cluster.runRandomly(false);
            cluster.stabilise();
        }
    }

    public void testFollowerRemovedIfUnableToSendRequestsToMaster() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode otherNode = cluster.getAnyNodeExcept(leader);

            cluster.blackholeConnectionsFrom(otherNode, leader);

            cluster.runFor(
                (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
                    * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING)
                    + (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + DEFAULT_DELAY_VARIABILITY)
                    * defaultInt(LEADER_CHECK_RETRY_COUNT_SETTING)
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,
                "awaiting removal of asymmetrically-partitioned node");

            assertThat(leader.getLastAppliedClusterState().nodes().toString(),
                leader.getLastAppliedClusterState().nodes().getSize(), equalTo(2));

            cluster.clearBlackholedConnections();

            cluster.stabilise(
                // time for the disconnected node to find the master again
                defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2
                    // time for joining
                    + 4 * DEFAULT_DELAY_VARIABILITY
                    // Then a commit of the updated cluster state
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
        }
    }

    public void testSingleNodeDiscoveryWithoutQuorum() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode clusterNode = cluster.getAnyNode();
            logger.debug("rebooting [{}]", clusterNode.getId());
            clusterNode.close();
            cluster.clusterNodes.forEach(
                cn -> cluster.deterministicTaskQueue.scheduleNow(cn.onNode(
                    new Runnable() {
                        @Override
                        public void run() {
                            cn.transportService.disconnectFromNode(clusterNode.getLocalNode());
                        }

                        @Override
                        public String toString() {
                            return "disconnect from " + clusterNode.getLocalNode() + " after shutdown";
                        }
                    })));
            IllegalStateException ise = expectThrows(IllegalStateException.class,
                () -> cluster.clusterNodes.replaceAll(cn -> cn == clusterNode ?
                    cn.restartedNode(Function.identity(), Function.identity(), Settings.builder()
                        .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE).build()) :
                    cn));
            assertThat(ise.getMessage(), allOf(
                containsString("cannot start with [discovery.type] set to [single-node] when local node"),
                containsString("does not have quorum in voting configuration")));

            cluster.clusterNodes.remove(clusterNode); // to avoid closing it twice
        }
    }

    public void testSingleNodeDiscoveryWithQuorum() {
        try (Cluster cluster = new Cluster(1, randomBoolean(), Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE).build())) {

            cluster.runRandomly();
            cluster.stabilise();
        }
    }

    private static class BrokenCustom extends AbstractDiffable<ClusterState.Custom> implements ClusterState.Custom {

        static final String EXCEPTION_MESSAGE = "simulated";

        @Override
        public String getWriteableName() {
            return "broken";
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_EMPTY;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new ElasticsearchException(EXCEPTION_MESSAGE);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

    }

    public void testClusterRecoversAfterExceptionDuringSerialization() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader1 = cluster.getAnyLeader();

            logger.info("--> submitting broken task to [{}]", leader1);

            final AtomicBoolean failed = new AtomicBoolean();
            leader1.submitUpdateTask("broken-task",
                cs -> ClusterState.builder(cs).putCustom("broken", new BrokenCustom()).build(),
                (source, e) -> {
                    assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
                    assertThat(e.getCause().getMessage(), equalTo(BrokenCustom.EXCEPTION_MESSAGE));
                    failed.set(true);
                });
            cluster.runFor(DEFAULT_DELAY_VARIABILITY + 1, "processing broken task");
            assertTrue(failed.get());

            cluster.stabilise();

            final ClusterNode leader2 = cluster.getAnyLeader();
            long finalValue = randomLong();

            logger.info("--> submitting value [{}] to [{}]", finalValue, leader2);
            leader2.submitValue(finalValue);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                final String nodeId = clusterNode.getId();
                final ClusterState appliedState = clusterNode.getLastAppliedClusterState();
                assertThat(nodeId + " has the applied value", value(appliedState), is(finalValue));
            }
        }
    }

    public void testLogsWarningPeriodicallyIfClusterNotFormed() throws IllegalAccessException {
        final long warningDelayMillis;
        final Settings settings;
        if (randomBoolean()) {
            settings = Settings.EMPTY;
            warningDelayMillis = ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.get(settings).millis();
        } else {
            warningDelayMillis = randomLongBetween(1, 100000);
            settings = Settings.builder()
                .put(ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.getKey(), warningDelayMillis + "ms")
                .build();
        }
        logger.info("--> emitting warnings every [{}ms]", warningDelayMillis);

        try (Cluster cluster = new Cluster(3, true, settings)) {
            cluster.runRandomly();
            cluster.stabilise();

            logger.info("--> disconnecting all nodes");

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                clusterNode.disconnect();
            }

            cluster.runFor(defaultMillis(LEADER_CHECK_TIMEOUT_SETTING) // to wait for any in-flight check to time out
                    + defaultMillis(LEADER_CHECK_INTERVAL_SETTING) // to wait for the next check to be sent
                    + 2 * DEFAULT_DELAY_VARIABILITY, // to send the failing check and receive the disconnection response
                "waiting for leader failure");

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(clusterNode.getId() + " is CANDIDATE", clusterNode.coordinator.getMode(), is(CANDIDATE));
            }

            for (int i = scaledRandomIntBetween(1, 10); i >= 0; i--) {
                final MockLogAppender mockLogAppender = new MockLogAppender();
                try {
                    mockLogAppender.start();
                    Loggers.addAppender(LogManager.getLogger(ClusterFormationFailureHelper.class), mockLogAppender);
                    mockLogAppender.addExpectation(new MockLogAppender.LoggingExpectation() {
                        final Set<DiscoveryNode> nodesLogged = new HashSet<>();

                        @Override
                        public void match(LogEvent event) {
                            final String message = event.getMessage().getFormattedMessage();
                            assertThat(message,
                                startsWith("master not discovered or elected yet, an election requires at least 2 nodes with ids from ["));

                            final List<ClusterNode> matchingNodes = cluster.clusterNodes.stream()
                                .filter(n -> event.getContextData().<String>getValue(NODE_ID_LOG_CONTEXT_KEY)
                                    .equals(getNodeIdForLogContext(n.getLocalNode()))).collect(Collectors.toList());
                            assertThat(matchingNodes, hasSize(1));

                            assertTrue(Regex.simpleMatch(
                                "*have discovered *" + matchingNodes.get(0).toString() + "*discovery will continue*",
                                message));

                            nodesLogged.add(matchingNodes.get(0).getLocalNode());
                        }

                        @Override
                        public void assertMatched() {
                            assertThat(nodesLogged + " vs " + cluster.clusterNodes, nodesLogged,
                                equalTo(cluster.clusterNodes.stream().map(ClusterNode::getLocalNode).collect(Collectors.toSet())));
                        }
                    });
                    cluster.runFor(warningDelayMillis + DEFAULT_DELAY_VARIABILITY, "waiting for warning to be emitted");
                    mockLogAppender.assertAllExpectationsMatched();
                } finally {
                    Loggers.removeAppender(LogManager.getLogger(ClusterFormationFailureHelper.class), mockLogAppender);
                    mockLogAppender.stop();
                }
            }
        }
    }

    public void testLogsMessagesIfPublicationDelayed() throws IllegalAccessException {
        try (Cluster cluster = new Cluster(between(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode brokenNode = cluster.getAnyNodeExcept(cluster.getAnyLeader());

            final MockLogAppender mockLogAppender = new MockLogAppender();
            try {
                mockLogAppender.start();
                Loggers.addAppender(LogManager.getLogger(Coordinator.CoordinatorPublication.class), mockLogAppender);
                Loggers.addAppender(LogManager.getLogger(LagDetector.class), mockLogAppender);

                mockLogAppender.addExpectation(new MockLogAppender.SeenEventExpectation("publication info message",
                    Coordinator.CoordinatorPublication.class.getCanonicalName(), Level.INFO,
                    "after [*] publication of cluster state version [*] is still waiting for " + brokenNode.getLocalNode() + " ["
                        + Publication.PublicationTargetState.SENT_PUBLISH_REQUEST + ']'));

                mockLogAppender.addExpectation(new MockLogAppender.SeenEventExpectation("publication warning",
                    Coordinator.CoordinatorPublication.class.getCanonicalName(), Level.WARN,
                    "after [*] publication of cluster state version [*] is still waiting for " + brokenNode.getLocalNode() + " ["
                        + Publication.PublicationTargetState.SENT_PUBLISH_REQUEST + ']'));

                mockLogAppender.addExpectation(new MockLogAppender.SeenEventExpectation("lag warning",
                    LagDetector.class.getCanonicalName(), Level.WARN,
                    "node [" + brokenNode + "] is lagging at cluster state version [*], " +
                        "although publication of cluster state version [*] completed [*] ago"));

                // drop the publication messages to one node, but then restore connectivity so it remains in the cluster and does not fail
                // health checks
                brokenNode.blackhole();
                cluster.deterministicTaskQueue.scheduleAt(
                    cluster.deterministicTaskQueue.getCurrentTimeMillis() + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,
                    new Runnable() {
                        @Override
                        public void run() {
                            brokenNode.heal();
                        }

                        @Override
                        public String toString() {
                            return "healing " + brokenNode;
                        }
                    });
                cluster.getAnyLeader().submitValue(randomLong());
                cluster.runFor(defaultMillis(PUBLISH_TIMEOUT_SETTING) + 2 * DEFAULT_DELAY_VARIABILITY
                        + defaultMillis(LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING),
                    "waiting for messages to be emitted");

                mockLogAppender.assertAllExpectationsMatched();
            } finally {
                Loggers.removeAppender(LogManager.getLogger(Coordinator.CoordinatorPublication.class), mockLogAppender);
                Loggers.removeAppender(LogManager.getLogger(LagDetector.class), mockLogAppender);
                mockLogAppender.stop();
            }
        }
    }

    public void testReconfiguresToExcludeMasterIneligibleNodesInVotingConfig() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode chosenNode = cluster.getAnyNode();

            assertThat(cluster.getAnyLeader().getLastAppliedClusterState().getLastCommittedConfiguration().getNodeIds(),
                hasItem(chosenNode.getId()));
            assertThat(cluster.getAnyLeader().getLastAppliedClusterState().getLastAcceptedConfiguration().getNodeIds(),
                hasItem(chosenNode.getId()));

            final boolean chosenNodeIsLeader = chosenNode == cluster.getAnyLeader();
            final long termBeforeRestart = cluster.getAnyNode().coordinator.getCurrentTerm();

            logger.info("--> restarting [{}] as a master-ineligible node", chosenNode);

            chosenNode.close();
            cluster.clusterNodes.replaceAll(cn -> cn == chosenNode ? cn.restartedNode(Function.identity(), Function.identity(),
                Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false).build()) : cn);
            cluster.stabilise();

            if (chosenNodeIsLeader == false) {
                assertThat("term did not change", cluster.getAnyNode().coordinator.getCurrentTerm(), is(termBeforeRestart));
            }

            assertThat(cluster.getAnyLeader().getLastAppliedClusterState().getLastCommittedConfiguration().getNodeIds(),
                not(hasItem(chosenNode.getId())));
            assertThat(cluster.getAnyLeader().getLastAppliedClusterState().getLastAcceptedConfiguration().getNodeIds(),
                not(hasItem(chosenNode.getId())));
        }
    }

    public void testDoesNotPerformElectionWhenRestartingFollower() {
        try (Cluster cluster = new Cluster(randomIntBetween(2, 5), false, Settings.EMPTY)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final long expectedTerm = leader.coordinator.getCurrentTerm();

            if (cluster.clusterNodes.stream().filter(n -> n.getLocalNode().isMasterEligibleNode()).count() == 2) {
                // in the 2-node case, auto-shrinking the voting configuration is required to reduce the voting configuration down to just
                // the leader, otherwise restarting the other master-eligible node triggers an election
                leader.submitSetAutoShrinkVotingConfiguration(true);
                cluster.stabilise(2 * DEFAULT_CLUSTER_STATE_UPDATE_DELAY); // 1st delay for the setting update, 2nd for the reconfiguration
            }

            for (final ClusterNode clusterNode : cluster.getAllNodesExcept(leader)) {
                logger.info("--> restarting {}", clusterNode);
                clusterNode.close();
                cluster.clusterNodes.replaceAll(cn ->
                    cn == clusterNode ? cn.restartedNode(Function.identity(), Function.identity(), Settings.EMPTY) : cn);
                cluster.stabilise();
                assertThat("term should not change", cluster.getAnyNode().coordinator.getCurrentTerm(), is(expectedTerm));
            }
        }
    }

}
