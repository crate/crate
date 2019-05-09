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

import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CoordinationStateTests extends ESTestCase {

    private DiscoveryNode node1;
    private DiscoveryNode node2;
    private DiscoveryNode node3;

    private ClusterState initialStateNode1;

    private PersistedState ps1;

    private CoordinationState cs1;
    private CoordinationState cs2;
    private CoordinationState cs3;

    @Before
    public void setupNodes() {
        node1 = createNode("node1");
        node2 = createNode("node2");
        node3 = createNode("node3");

        initialStateNode1 = clusterState(0L, 0L, node1, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 42L);
        ClusterState initialStateNode2 =
            clusterState(0L, 0L, node2, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 42L);
        ClusterState initialStateNode3 =
            clusterState(0L, 0L, node3, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 42L);

        ps1 = new InMemoryPersistedState(0L, initialStateNode1);

        cs1 = createCoordinationState(ps1, node1);
        cs2 = createCoordinationState(new InMemoryPersistedState(0L, initialStateNode2), node2);
        cs3 = createCoordinationState(new InMemoryPersistedState(0L, initialStateNode3), node3);
    }

    public static DiscoveryNode createNode(String id) {
        final TransportAddress address = buildNewFakeTransportAddress();
        return new DiscoveryNode("", id,
            UUIDs.randomBase64UUID(random()), // generated deterministically for repeatable tests
            address.address().getHostString(), address.getAddress(), address, Collections.emptyMap(),
            EnumSet.allOf(Role.class), Version.CURRENT);
    }

    public void testSetInitialState() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        cs1.setInitialState(state1);
        assertThat(cs1.getLastAcceptedState(), equalTo(state1));
    }

    public void testSetInitialStateWhenAlreadySet() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        cs1.setInitialState(state1);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.setInitialState(state1)).getMessage(),
            containsString("initial state already set"));
    }

    public void testStartJoinBeforeBootstrap() {
        assertThat(cs1.getCurrentTerm(), equalTo(0L));
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(randomFrom(node1, node2), randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(v1.getTargetNode(), equalTo(startJoinRequest1.getSourceNode()));
        assertThat(v1.getSourceNode(), equalTo(node1));
        assertThat(v1.getTerm(), equalTo(startJoinRequest1.getTerm()));
        assertThat(v1.getLastAcceptedTerm(), equalTo(initialStateNode1.term()));
        assertThat(v1.getLastAcceptedVersion(), equalTo(initialStateNode1.version()));
        assertThat(cs1.getCurrentTerm(), equalTo(startJoinRequest1.getTerm()));

        StartJoinRequest startJoinRequest2 = new StartJoinRequest(randomFrom(node1, node2),
            randomLongBetween(0, startJoinRequest1.getTerm()));
        expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleStartJoin(startJoinRequest2));
    }

    public void testStartJoinAfterBootstrap() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(randomFrom(node1, node2), randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(v1.getTargetNode(), equalTo(startJoinRequest1.getSourceNode()));
        assertThat(v1.getSourceNode(), equalTo(node1));
        assertThat(v1.getTerm(), equalTo(startJoinRequest1.getTerm()));
        assertThat(v1.getLastAcceptedTerm(), equalTo(state1.term()));
        assertThat(v1.getLastAcceptedVersion(), equalTo(state1.version()));
        assertThat(cs1.getCurrentTerm(), equalTo(startJoinRequest1.getTerm()));

        StartJoinRequest startJoinRequest2 = new StartJoinRequest(randomFrom(node1, node2),
            randomLongBetween(0, startJoinRequest1.getTerm()));
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleStartJoin(startJoinRequest2)).getMessage(),
            containsString("not greater than current term"));
        StartJoinRequest startJoinRequest3 = new StartJoinRequest(randomFrom(node1, node2),
            startJoinRequest1.getTerm());
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleStartJoin(startJoinRequest3)).getMessage(),
            containsString("not greater than current term"));
    }

    public void testJoinBeforeBootstrap() {
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(v1)).getMessage(),
            containsString("this node has not received its initial configuration yet"));
    }

    public void testJoinWithNoStartJoinAfterReboot() {
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        cs1 = createCoordinationState(ps1, node1);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(v1)).getMessage(),
            containsString("ignored join as term has not been incremented yet after reboot"));
    }

    public void testJoinWithWrongTarget() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(expectThrows(AssertionError.class, () -> cs1.handleJoin(v1)).getMessage(),
            containsString("wrong node"));
    }

    public void testJoinWithBadCurrentTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        Join badJoin = new Join(randomFrom(node1, node2), node1, randomLongBetween(0, startJoinRequest1.getTerm() - 1),
            randomNonNegativeLong(), randomNonNegativeLong());
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(badJoin)).getMessage(),
            containsString("does not match current term"));
    }

    public void testJoinWithHigherAcceptedTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join badJoin = new Join(randomFrom(node1, node2), node1, v1.getTerm(), randomLongBetween(state2.term() + 1, 30),
            randomNonNegativeLong());
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(badJoin)).getMessage(),
            containsString("higher than current last accepted term"));
    }

    public void testJoinWithSameAcceptedTermButHigherVersion() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join badJoin = new Join(randomFrom(node1, node2), node1, v1.getTerm(), state2.term(),
            randomLongBetween(state2.version() + 1, 30));
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(badJoin)).getMessage(),
            containsString("higher than current last accepted version"));
    }

    public void testJoinWithLowerLastAcceptedTermWinsElection() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node1, node1, v1.getTerm(), randomLongBetween(0, state2.term() - 1), randomLongBetween(0, 20));
        assertTrue(cs1.handleJoin(join));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.containsJoinVoteFor(node1));
        assertFalse(cs1.containsJoinVoteFor(node2));
        assertEquals(cs1.getLastPublishedVersion(), cs1.getLastAcceptedVersion());
        assertFalse(cs1.handleJoin(join));
    }

    public void testJoinWithSameLastAcceptedTermButLowerOrSameVersionWinsElection() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node1, node1, v1.getTerm(), state2.term(), randomLongBetween(0, state2.version()));
        assertTrue(cs1.handleJoin(join));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.containsJoinVoteFor(node1));
        assertFalse(cs1.containsJoinVoteFor(node2));
        assertEquals(cs1.getLastPublishedVersion(), cs1.getLastAcceptedVersion());
        assertFalse(cs1.handleJoin(join));
    }

    public void testJoinDoesNotWinElection() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node2, node1, v1.getTerm(), randomLongBetween(0, state2.term()), randomLongBetween(0, state2.version()));
        assertTrue(cs1.handleJoin(join));
        assertFalse(cs1.electionWon());
        assertEquals(cs1.getLastPublishedVersion(), 0L);
        assertFalse(cs1.handleJoin(join));
    }

    public void testJoinDoesNotWinElectionWhenOnlyCommittedConfigQuorum() {
        VotingConfiguration configNode1 = new VotingConfiguration(Collections.singleton(node1.getId()));
        VotingConfiguration configNode2 = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, configNode1, configNode2, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join join = cs1.handleStartJoin(startJoinRequest);
        assertTrue(cs1.handleJoin(join));
        assertFalse(cs1.electionWon());
        assertEquals(cs1.getLastPublishedVersion(), 0L);
        assertFalse(cs1.handleJoin(join));
    }

    public void testJoinDoesNotWinElectionWhenOnlyLastAcceptedConfigQuorum() {
        VotingConfiguration configNode1 = new VotingConfiguration(Collections.singleton(node1.getId()));
        VotingConfiguration configNode2 = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, configNode2, configNode1, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join join = cs1.handleStartJoin(startJoinRequest);
        assertTrue(cs1.handleJoin(join));
        assertFalse(cs1.electionWon());
        assertEquals(cs1.getLastPublishedVersion(), 0L);
        assertFalse(cs1.handleJoin(join));
    }

    public void testHandleClientValue() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.handleJoin(v2));

        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        assertThat(publishRequest.getAcceptedState(), equalTo(state2));
        assertThat(cs1.getLastPublishedVersion(), equalTo(state2.version()));
        // check that another join does not mess with lastPublishedVersion
        Join v3 = cs3.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v3));
        assertThat(cs1.getLastPublishedVersion(), equalTo(state2.version()));
    }

    public void testHandleClientValueWhenElectionNotWon() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        if (randomBoolean()) {
            cs1.setInitialState(state1);
        }
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state1)).getMessage(),
            containsString("election not won"));
    }

    public void testHandleClientValueDuringOngoingPublication() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        cs1.handleClientValue(state2);

        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state3)).getMessage(),
            containsString("cannot start publishing next value before accepting previous one"));
    }

    public void testHandleClientValueWithBadTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(3, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());

        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        ClusterState state2 = clusterState(term, 2L, node1, initialConfig, initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state2)).getMessage(),
            containsString("does not match current term"));
    }

    public void testHandleClientValueWithOldVersion() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 0L, node1, initialConfig, initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state2)).getMessage(),
            containsString("lower or equal to last published version"));
    }

    public void testHandleClientValueWithDifferentReconfigurationWhileAlreadyReconfiguring() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.handleJoin(v2));

        VotingConfiguration newConfig1 = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig1, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        cs1.handlePublishRequest(publishRequest);
        VotingConfiguration newConfig2 = new VotingConfiguration(Collections.singleton(node3.getId()));
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, newConfig2, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state3)).getMessage(),
            containsString("only allow reconfiguration while not already reconfiguring"));
    }

    public void testHandleClientValueWithSameReconfigurationWhileAlreadyReconfiguring() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.handleJoin(v2));

        VotingConfiguration newConfig1 = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig1, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        cs1.handlePublishRequest(publishRequest);
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, newConfig1, 42L);
        cs1.handleClientValue(state3);
    }

    public void testHandleClientValueWithIllegalCommittedConfigurationChange() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.handleJoin(v2));

        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, newConfig, newConfig, 42L);
        assertThat(expectThrows(AssertionError.class, () -> cs1.handleClientValue(state2)).getMessage(),
            containsString("last committed configuration should not change"));
    }

    public void testHandleClientValueWithConfigurationChangeButNoJoinQuorum() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());

        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state2)).getMessage(),
            containsString("only allow reconfiguration if joinVotes have quorum for new config"));
    }

    public void testHandlePublishRequest() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertTrue(cs1.handleJoin(v1));
            assertTrue(cs1.electionWon());
        }
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(1, 10), node1, initialConfig, initialConfig, 13L);
        PublishResponse publishResponse = cs1.handlePublishRequest(new PublishRequest(state2));
        assertThat(publishResponse.getTerm(), equalTo(state2.term()));
        assertThat(publishResponse.getVersion(), equalTo(state2.version()));
        assertThat(cs1.getLastAcceptedState(), equalTo(state2));
        assertThat(value(cs1.getLastAcceptedState()), equalTo(13L));
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(state2.getVersion() + 1, 20), node1,
            initialConfig, initialConfig, 13L);
        cs1.handlePublishRequest(new PublishRequest(state3));
    }

    public void testHandlePublishRequestWithBadTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertTrue(cs1.handleJoin(v1));
            assertTrue(cs1.electionWon());
        }
        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        ClusterState state2 = clusterState(term, 2L, node1, initialConfig, initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> cs1.handlePublishRequest(new PublishRequest(state2))).getMessage(),
            containsString("does not match current term"));
    }

    // scenario when handling a publish request from a master that we already received a newer state from
    public void testHandlePublishRequestWithSameTermButOlderOrSameVersion() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertTrue(cs1.handleJoin(v1));
            assertTrue(cs1.electionWon());
        }
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(0, state2.version()), node1, initialConfig,
            initialConfig, 42L);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> cs1.handlePublishRequest(new PublishRequest(state3))).getMessage(),
            containsString("lower or equal to current version"));
    }

    // scenario when handling a publish request from a fresh master
    public void testHandlePublishRequestWithTermHigherThanLastAcceptedTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        ClusterState state1 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        cs2.handleStartJoin(startJoinRequest1);
        cs2.handlePublishRequest(new PublishRequest(state1));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node1, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        cs2.handleStartJoin(startJoinRequest2);
        ClusterState state2 = clusterState(startJoinRequest2.getTerm(), randomLongBetween(0, 20), node1, initialConfig,
            initialConfig, 42L);
        cs2.handlePublishRequest(new PublishRequest(state2));
    }

    public void testHandlePublishResponseWithCommit() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node1, publishResponse);
        assertTrue(applyCommit.isPresent());
        assertThat(applyCommit.get().getSourceNode(), equalTo(node1));
        assertThat(applyCommit.get().getTerm(), equalTo(state2.term()));
        assertThat(applyCommit.get().getVersion(), equalTo(state2.version()));
    }

    public void testHandlePublishResponseWhenSteppedDownAsLeader() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node1, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        cs1.handleStartJoin(startJoinRequest2);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> cs1.handlePublishResponse(randomFrom(node1, node2, node3), publishResponse)).getMessage(),
            containsString("election not won"));
    }

    public void testHandlePublishResponseWithoutPublishConfigQuorum() {
        VotingConfiguration configNode1 = new VotingConfiguration(Collections.singleton(node1.getId()));
        VotingConfiguration configNode2 = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, configNode1, configNode1, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v2));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, configNode1, configNode2, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node1, publishResponse);
        assertFalse(applyCommit.isPresent());
    }

    public void testHandlePublishResponseWithoutCommitedConfigQuorum() {
        VotingConfiguration configNode1 = new VotingConfiguration(Collections.singleton(node1.getId()));
        VotingConfiguration configNode2 = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, configNode1, configNode1, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v2));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, configNode1, configNode2, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs2.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node2, publishResponse);
        assertFalse(applyCommit.isPresent());
    }

    public void testHandlePublishResponseWithoutCommit() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node2, publishResponse);
        assertFalse(applyCommit.isPresent());
    }

    public void testHandlePublishResponseWithBadTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        PublishResponse publishResponse = cs1.handlePublishRequest(new PublishRequest(state2));
        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> cs1.handlePublishResponse(randomFrom(node1, node2, node3),
                new PublishResponse(term, publishResponse.getVersion()))).getMessage(),
            containsString("does not match current term"));
    }

    public void testHandlePublishResponseWithVersionMismatch() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        PublishResponse publishResponse = cs1.handlePublishRequest(new PublishRequest(state2));
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> cs1.handlePublishResponse(randomFrom(node1, node2, node3), publishResponse)).getMessage(),
            containsString("does not match current version"));
    }

    public void testHandleCommit() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v2));
        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 7L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        cs1.handlePublishResponse(node1, publishResponse);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node2, publishResponse);
        assertTrue(applyCommit.isPresent());
        assertThat(cs1.getLastCommittedConfiguration(), equalTo(initialConfig));
        cs1.handleCommit(applyCommit.get());
        assertThat(cs1.getLastCommittedConfiguration(), equalTo(newConfig));
    }

    public void testHandleCommitWithBadCurrentTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 7L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        cs1.handlePublishResponse(node1, publishResponse);
        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> cs1.handleCommit(new ApplyCommitRequest(node1, term, 2L))).getMessage(),
            containsString("does not match current term"));
    }

    public void testHandleCommitWithBadLastAcceptedTerm() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> cs1.handleCommit(new ApplyCommitRequest(node1, startJoinRequest1.getTerm(), 2L))).getMessage(),
            containsString("does not match last accepted term"));
    }

    public void testHandleCommitWithBadVersion() {
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node1.getId()));
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 7L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        cs1.handlePublishRequest(publishRequest);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> cs1.handleCommit(new ApplyCommitRequest(node1, startJoinRequest1.getTerm(), randomLongBetween(3, 10)))).getMessage(),
            containsString("does not match current version"));
    }

    public void testVoteCollection() {
        final CoordinationState.VoteCollection voteCollection = new CoordinationState.VoteCollection();
        assertTrue(voteCollection.isEmpty());
        voteCollection.addVote(node1);
        assertFalse(voteCollection.isEmpty());
        assertTrue(voteCollection.containsVoteFor(node1));
        assertFalse(voteCollection.containsVoteFor(node2));
        assertFalse(voteCollection.containsVoteFor(node3));
        voteCollection.addVote(node2);
        assertTrue(voteCollection.containsVoteFor(node1));
        assertTrue(voteCollection.containsVoteFor(node2));
        assertFalse(voteCollection.containsVoteFor(node3));
        assertTrue(voteCollection.isQuorum(new VotingConfiguration(Sets.newHashSet(node1.getId(), node2.getId()))));
        assertTrue(voteCollection.isQuorum(new VotingConfiguration(Sets.newHashSet(node1.getId()))));
        assertFalse(voteCollection.isQuorum(new VotingConfiguration(Sets.newHashSet(node3.getId()))));

        EqualsHashCodeTestUtils.CopyFunction<CoordinationState.VoteCollection> copyFunction =
            vc -> {
                CoordinationState.VoteCollection voteCollection1 = new CoordinationState.VoteCollection();
                for (DiscoveryNode node : vc.nodes()) {
                    voteCollection1.addVote(node);
                }
                return voteCollection1;
            };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(voteCollection, copyFunction,
            vc -> {
                CoordinationState.VoteCollection copy = copyFunction.copy(vc);
                copy.addVote(createNode(randomAlphaOfLength(10)));
                return copy;
            });
    }

    public void testSafety() {
        new Cluster(randomIntBetween(1, 5)).runRandomly();
    }

    public static CoordinationState createCoordinationState(PersistedState storage, DiscoveryNode localNode) {
        final Settings initialSettings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), localNode.getId()).build();
        return new CoordinationState(initialSettings, localNode, storage);
    }

    public static ClusterState clusterState(long term, long version, DiscoveryNode localNode, VotingConfiguration lastCommittedConfig,
                                            VotingConfiguration lastAcceptedConfig, long value) {
        return clusterState(term, version, DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build(),
            lastCommittedConfig, lastAcceptedConfig, value);
    }

    public static ClusterState clusterState(long term, long version, DiscoveryNodes discoveryNodes, VotingConfiguration lastCommittedConfig,
                                            VotingConfiguration lastAcceptedConfig, long value) {
        return setValue(ClusterState.builder(ClusterName.DEFAULT)
            .version(version)
            .nodes(discoveryNodes)
            .metaData(MetaData.builder()
                .clusterUUID(UUIDs.randomBase64UUID(random())) // generate cluster UUID deterministically for repeatable tests
                .coordinationMetaData(CoordinationMetaData.builder()
                        .term(term)
                        .lastCommittedConfiguration(lastCommittedConfig)
                        .lastAcceptedConfiguration(lastAcceptedConfig)
                        .build()))
            .stateUUID(UUIDs.randomBase64UUID(random())) // generate cluster state UUID deterministically for repeatable tests
            .build(), value);
    }

    public static ClusterState setValue(ClusterState clusterState, long value) {
        return ClusterState.builder(clusterState).metaData(
            MetaData.builder(clusterState.metaData())
                .persistentSettings(Settings.builder()
                    .put(clusterState.metaData().persistentSettings())
                    .put("value", value)
                    .build())
                .build())
            .build();
    }

    public static long value(ClusterState clusterState) {
        return clusterState.metaData().persistentSettings().getAsLong("value", 0L);
    }
    
    static class ClusterNode {

        final DiscoveryNode localNode;
        final PersistedState persistedState;
        CoordinationState state;

        ClusterNode(DiscoveryNode localNode) {
            this.localNode = localNode;
            persistedState = new InMemoryPersistedState(0L,
                clusterState(0L, 0L, localNode, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 0L));
            state = new CoordinationState(Settings.EMPTY, localNode, persistedState);
        }

        void reboot() {
            state = new CoordinationState(Settings.EMPTY, localNode, persistedState);
        }

        void setInitialState(VotingConfiguration initialConfig, long initialValue) {
            final ClusterState.Builder builder = ClusterState.builder(state.getLastAcceptedState());
            builder.metaData(MetaData.builder()
                    .coordinationMetaData(CoordinationMetaData.builder()
                        .lastAcceptedConfiguration(initialConfig)
                        .lastCommittedConfiguration(initialConfig)
                    .build()));
            state.setInitialState(setValue(builder.build(), initialValue));
        }
    }

    static class Cluster {

        final List<Message> messages;
        final List<ClusterNode> clusterNodes;
        final VotingConfiguration initialConfiguration;
        final long initialValue;

        Cluster(int numNodes) {
            messages = new ArrayList<>();

            clusterNodes = IntStream.range(0, numNodes)
                .mapToObj(i -> new DiscoveryNode("node_" + i, buildNewFakeTransportAddress(), Version.CURRENT))
                .map(ClusterNode::new)
                .collect(Collectors.toList());

            initialConfiguration = randomVotingConfig();
            initialValue = randomLong();
        }

        static class Message {
            final DiscoveryNode sourceNode;
            final DiscoveryNode targetNode;
            final Object payload;

            Message(DiscoveryNode sourceNode, DiscoveryNode targetNode, Object payload) {
                this.sourceNode = sourceNode;
                this.targetNode = targetNode;
                this.payload = payload;
            }
        }

        void reply(Message m, Object payload) {
            messages.add(new Message(m.targetNode, m.sourceNode, payload));
        }

        void broadcast(DiscoveryNode sourceNode, Object payload) {
            messages.addAll(clusterNodes.stream().map(cn -> new Message(sourceNode, cn.localNode, payload)).collect(Collectors.toList()));
        }

        Optional<ClusterNode> getNode(DiscoveryNode node) {
            return clusterNodes.stream().filter(cn -> cn.localNode.equals(node)).findFirst();
        }

        VotingConfiguration randomVotingConfig() {
            return new VotingConfiguration(
                randomSubsetOf(randomIntBetween(1, clusterNodes.size()), clusterNodes).stream()
                    .map(cn -> cn.localNode.getId()).collect(toSet()));
        }

        void applyMessage(Message message) {
            final Optional<ClusterNode> maybeNode = getNode(message.targetNode);
            if (maybeNode.isPresent() == false) {
                throw new CoordinationStateRejectedException("node not available");
            } else {
                final Object payload = message.payload;
                if (payload instanceof StartJoinRequest) {
                    reply(message, maybeNode.get().state.handleStartJoin((StartJoinRequest) payload));
                } else if (payload instanceof Join) {
                    maybeNode.get().state.handleJoin((Join) payload);
                } else if (payload instanceof PublishRequest) {
                    reply(message, maybeNode.get().state.handlePublishRequest((PublishRequest) payload));
                } else if (payload instanceof PublishResponse) {
                    maybeNode.get().state.handlePublishResponse(message.sourceNode, (PublishResponse) payload)
                        .ifPresent(ac -> broadcast(message.targetNode, ac));
                } else if (payload instanceof ApplyCommitRequest) {
                    maybeNode.get().state.handleCommit((ApplyCommitRequest) payload);
                } else {
                    throw new AssertionError("unknown message type");
                }
            }
        }

        void runRandomly() {
            final int iterations = 10000;
            final long maxTerm = 4;
            long nextTerm = 1;
            for (int i = 0; i < iterations; i++) {
                try {
                    if (rarely() && nextTerm < maxTerm) {
                        final long term = rarely() ? randomLongBetween(0, maxTerm + 1) : nextTerm++;
                        final StartJoinRequest startJoinRequest = new StartJoinRequest(randomFrom(clusterNodes).localNode, term);
                        broadcast(startJoinRequest.getSourceNode(), startJoinRequest);
                    } else if (rarely()) {
                        randomFrom(clusterNodes).setInitialState(initialConfiguration, initialValue);
                    } else if (rarely() && rarely()) {
                        randomFrom(clusterNodes).reboot();
                    } else if (rarely()) {
                        final List<ClusterNode> masterNodes = clusterNodes.stream().filter(cn -> cn.state.electionWon())
                            .collect(Collectors.toList());
                        if (masterNodes.isEmpty() == false) {
                            final ClusterNode clusterNode = randomFrom(masterNodes);
                            final long term = rarely() ? randomLongBetween(0, maxTerm + 1) : clusterNode.state.getCurrentTerm();
                            final long version = rarely() ? randomIntBetween(0, 5) : clusterNode.state.getLastPublishedVersion() + 1;
                            final VotingConfiguration acceptedConfig = rarely() ? randomVotingConfig() :
                                clusterNode.state.getLastAcceptedConfiguration();
                            final PublishRequest publishRequest = clusterNode.state.handleClientValue(
                                clusterState(term, version, clusterNode.localNode, clusterNode.state.getLastCommittedConfiguration(),
                                    acceptedConfig, randomLong()));
                            broadcast(clusterNode.localNode, publishRequest);
                        }
                    } else if (messages.isEmpty() == false) {
                        applyMessage(randomFrom(messages));
                    }

                    // check node invariants after each iteration
                    clusterNodes.forEach(cn -> cn.state.invariant());
                } catch (CoordinationStateRejectedException e) {
                    // ignore
                }
            }

            // check system invariants. It's sufficient to do this at the end as these invariants are monotonic.
            invariant();
        }

        void invariant() {
            // one master per term
            messages.stream().filter(m -> m.payload instanceof PublishRequest)
                .collect(Collectors.groupingBy(m -> ((PublishRequest) m.payload).getAcceptedState().term()))
                .forEach((term, publishMessages) -> {
                    Set<DiscoveryNode> mastersForTerm = publishMessages.stream().collect(Collectors.groupingBy(m -> m.sourceNode)).keySet();
                    assertThat("Multiple masters " + mastersForTerm + " for term " + term, mastersForTerm, hasSize(1));
                });

            // unique cluster state per (term, version) pair
            messages.stream().filter(m -> m.payload instanceof PublishRequest)
                .map(m -> ((PublishRequest) m.payload).getAcceptedState())
                .collect(Collectors.groupingBy(ClusterState::term))
                .forEach((term, clusterStates) -> {
                    clusterStates.stream().collect(Collectors.groupingBy(ClusterState::version))
                    .forEach((version, clusterStates1) -> {
                        Set<String> clusterStateUUIDsForTermAndVersion = clusterStates1.stream().collect(Collectors.groupingBy(
                            ClusterState::stateUUID
                        )).keySet();
                        assertThat("Multiple cluster states " + clusterStates1 + " for term " + term + " and version " + version,
                            clusterStateUUIDsForTermAndVersion, hasSize(1));

                        Set<Long> clusterStateValuesForTermAndVersion = clusterStates1.stream().collect(Collectors.groupingBy(
                            CoordinationStateTests::value
                        )).keySet();

                        assertThat("Multiple cluster states " + clusterStates1 + " for term " + term + " and version " + version,
                            clusterStateValuesForTermAndVersion, hasSize(1));
                    });
                });
        }

    }
}
