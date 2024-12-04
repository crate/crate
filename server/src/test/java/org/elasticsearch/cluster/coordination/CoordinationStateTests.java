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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.junit.Before;

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
            DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
    }

    public void testSetInitialState() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        assertThat(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId()))).isTrue();
        assertThat(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId()))).isTrue();
        cs1.setInitialState(state1);
        assertThat(cs1.getLastAcceptedState()).isEqualTo(state1);
    }

    public void testSetInitialStateWhenAlreadySet() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        assertThat(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId()))).isTrue();
        assertThat(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId()))).isTrue();
        cs1.setInitialState(state1);
        assertThatThrownBy(() -> cs1.setInitialState(state1))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("initial state already set");
    }

    public void testStartJoinBeforeBootstrap() {
        assertThat(cs1.getCurrentTerm()).isEqualTo(0L);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(randomFrom(node1, node2), randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(v1.getTargetNode()).isEqualTo(startJoinRequest1.getSourceNode());
        assertThat(v1.getSourceNode()).isEqualTo(node1);
        assertThat(v1.getTerm()).isEqualTo(startJoinRequest1.getTerm());
        assertThat(v1.getLastAcceptedTerm()).isEqualTo(initialStateNode1.term());
        assertThat(v1.getLastAcceptedVersion()).isEqualTo(initialStateNode1.version());
        assertThat(cs1.getCurrentTerm()).isEqualTo(startJoinRequest1.getTerm());

        StartJoinRequest startJoinRequest2 = new StartJoinRequest(randomFrom(node1, node2),
            randomLongBetween(0, startJoinRequest1.getTerm()));
        assertThatThrownBy(() -> cs1.handleStartJoin(startJoinRequest2))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class);
    }

    public void testStartJoinAfterBootstrap() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        assertThat(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId()))).isTrue();
        assertThat(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId()))).isTrue();
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(randomFrom(node1, node2), randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(v1.getTargetNode()).isEqualTo(startJoinRequest1.getSourceNode());
        assertThat(v1.getSourceNode()).isEqualTo(node1);
        assertThat(v1.getTerm()).isEqualTo(startJoinRequest1.getTerm());
        assertThat(v1.getLastAcceptedTerm()).isEqualTo(state1.term());
        assertThat(v1.getLastAcceptedVersion()).isEqualTo(state1.version());
        assertThat(cs1.getCurrentTerm()).isEqualTo(startJoinRequest1.getTerm());

        StartJoinRequest startJoinRequest2 = new StartJoinRequest(randomFrom(node1, node2),
            randomLongBetween(0, startJoinRequest1.getTerm()));
        assertThatThrownBy(() -> cs1.handleStartJoin(startJoinRequest2))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("not greater than current term");
        StartJoinRequest startJoinRequest3 = new StartJoinRequest(randomFrom(node1, node2),
            startJoinRequest1.getTerm());
        assertThatThrownBy(() -> cs1.handleStartJoin(startJoinRequest3))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("not greater than current term");
    }

    public void testJoinBeforeBootstrap() {
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThatThrownBy(() -> cs1.handleJoin(v1))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("this node has not received its initial configuration yet");
    }

    public void testJoinWithNoStartJoinAfterReboot() {
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        cs1 = createCoordinationState(ps1, node1);
        assertThatThrownBy(() -> cs1.handleJoin(v1))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("ignored join as term has not been incremented yet after reboot");
    }

    public void testJoinWithWrongTarget() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThatThrownBy(() -> cs1.handleJoin(v1))
            .isExactlyInstanceOf(AssertionError.class)
            .hasMessageContaining("wrong node");
    }

    public void testJoinWithBadCurrentTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        Join badJoin = new Join(randomFrom(node1, node2), node1, randomLongBetween(0, startJoinRequest1.getTerm() - 1),
            randomNonNegativeLong(), randomNonNegativeLong());
        assertThatThrownBy(() -> cs1.handleJoin(badJoin))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("does not match current term");
    }

    public void testJoinWithHigherAcceptedTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
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
        assertThatThrownBy(() -> cs1.handleJoin(badJoin))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("higher than current last accepted term");
    }

    public void testJoinWithSameAcceptedTermButHigherVersion() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
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
        assertThatThrownBy(() -> cs1.handleJoin(badJoin))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("higher than current last accepted version");
    }

    public void testJoinWithLowerLastAcceptedTermWinsElection() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node1, node1, v1.getTerm(), randomLongBetween(0, state2.term() - 1), randomLongBetween(0, 20));
        assertThat(cs1.handleJoin(join)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        assertThat(cs1.containsJoinVoteFor(node1)).isTrue();
        assertThat(cs1.containsJoin(join)).isTrue();
        assertThat(cs1.containsJoinVoteFor(node2)).isFalse();
        assertThat(cs1.getLastAcceptedVersion()).isEqualTo(cs1.getLastPublishedVersion());
        assertThat(cs1.handleJoin(join)).isFalse();
    }

    public void testJoinWithSameLastAcceptedTermButLowerOrSameVersionWinsElection() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node1, node1, v1.getTerm(), state2.term(), randomLongBetween(0, state2.version()));
        assertThat(cs1.handleJoin(join)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        assertThat(cs1.containsJoinVoteFor(node1)).isTrue();
        assertThat(cs1.containsJoinVoteFor(node2)).isFalse();
        assertThat(cs1.getLastAcceptedVersion()).isEqualTo(cs1.getLastPublishedVersion());
        assertThat(cs1.handleJoin(join)).isFalse();
    }

    public void testJoinDoesNotWinElection() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node2, node1, v1.getTerm(), randomLongBetween(0, state2.term()), randomLongBetween(0, state2.version()));
        assertThat(cs1.handleJoin(join)).isTrue();
        assertThat(cs1.electionWon()).isFalse();
        assertThat(0L).isEqualTo(cs1.getLastPublishedVersion());
        assertThat(cs1.handleJoin(join)).isFalse();
    }

    public void testJoinDoesNotWinElectionWhenOnlyCommittedConfigQuorum() {
        VotingConfiguration configNode1 = VotingConfiguration.of(node1);
        VotingConfiguration configNode2 = VotingConfiguration.of(node2);
        ClusterState state1 = clusterState(0L, 0L, node1, configNode1, configNode2, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join join = cs1.handleStartJoin(startJoinRequest);
        assertThat(cs1.handleJoin(join)).isTrue();
        assertThat(cs1.electionWon()).isFalse();
        assertThat(0L).isEqualTo(cs1.getLastPublishedVersion());
        assertThat(cs1.handleJoin(join)).isFalse();
    }

    public void testJoinDoesNotWinElectionWhenOnlyLastAcceptedConfigQuorum() {
        VotingConfiguration configNode1 = VotingConfiguration.of(node1);
        VotingConfiguration configNode2 = VotingConfiguration.of(node2);
        ClusterState state1 = clusterState(0L, 0L, node1, configNode2, configNode1, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join join = cs1.handleStartJoin(startJoinRequest);
        assertThat(cs1.handleJoin(join)).isTrue();
        assertThat(cs1.electionWon()).isFalse();
        assertThat(0L).isEqualTo(cs1.getLastPublishedVersion());
        assertThat(cs1.handleJoin(join)).isFalse();
    }

    public void testHandleClientValue() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        assertThat(cs1.containsJoin(v1)).isTrue();
        assertThat(cs1.containsJoin(v2)).isFalse();
        assertThat(cs1.handleJoin(v2)).isTrue();
        assertThat(cs1.containsJoin(v2)).isTrue();

        VotingConfiguration newConfig = VotingConfiguration.of(node2);

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        assertThat(publishRequest.getAcceptedState()).isEqualTo(state2);
        assertThat(cs1.getLastPublishedVersion()).isEqualTo(state2.version());
        // check that another join does not mess with lastPublishedVersion
        Join v3 = cs3.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v3)).isTrue();
        assertThat(cs1.getLastPublishedVersion()).isEqualTo(state2.version());
    }

    public void testHandleClientValueWhenElectionNotWon() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        if (randomBoolean()) {
            cs1.setInitialState(state1);
        }
        assertThatThrownBy(() -> cs1.handleClientValue(state1))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("election not won");
    }

    public void testHandleClientValueDuringOngoingPublication() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        cs1.handleClientValue(state2);

        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, initialConfig, 42L);
        assertThatThrownBy(() -> cs1.handleClientValue(state3))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("cannot start publishing next value before accepting previous one");
    }

    public void testHandleClientValueWithBadTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(3, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();

        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        ClusterState state2 = clusterState(term, 2L, node1, initialConfig, initialConfig, 42L);
        assertThatThrownBy(() -> cs1.handleClientValue(state2))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("does not match current term");
    }

    public void testHandleClientValueWithOldVersion() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 0L, node1, initialConfig, initialConfig, 42L);
        assertThatThrownBy(() -> cs1.handleClientValue(state2))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("lower or equal to last published version");
    }

    public void testHandleClientValueWithDifferentReconfigurationWhileAlreadyReconfiguring() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        assertThat(cs1.handleJoin(v2)).isTrue();

        VotingConfiguration newConfig1 = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig1, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        cs1.handlePublishRequest(publishRequest);
        VotingConfiguration newConfig2 = VotingConfiguration.of(node3);
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, newConfig2, 42L);
        assertThatThrownBy(() -> cs1.handleClientValue(state3))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("only allow reconfiguration while not already reconfiguring");
    }

    public void testHandleClientValueWithSameReconfigurationWhileAlreadyReconfiguring() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        assertThat(cs1.handleJoin(v2)).isTrue();

        VotingConfiguration newConfig1 = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig1, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        cs1.handlePublishRequest(publishRequest);
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, newConfig1, 42L);
        cs1.handleClientValue(state3);
    }

    public void testHandleClientValueWithIllegalCommittedConfigurationChange() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        assertThat(cs1.handleJoin(v2)).isTrue();

        VotingConfiguration newConfig = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, newConfig, newConfig, 42L);
        assertThatThrownBy(() -> cs1.handleClientValue(state2))
            .isExactlyInstanceOf(AssertionError.class)
            .hasMessageContaining("last committed configuration should not change");
    }

    public void testHandleClientValueWithConfigurationChangeButNoJoinQuorum() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();

        VotingConfiguration newConfig = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 42L);
        assertThatThrownBy(() -> cs1.handleClientValue(state2))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("only allow reconfiguration if joinVotes have quorum for new config");
    }

    public void testHandlePublishRequest() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertThat(cs1.handleJoin(v1)).isTrue();
            assertThat(cs1.electionWon()).isTrue();
        }
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(1, 10), node1, initialConfig, initialConfig, 13L);
        PublishResponse publishResponse = cs1.handlePublishRequest(new PublishRequest(state2));
        assertThat(publishResponse.getTerm()).isEqualTo(state2.term());
        assertThat(publishResponse.getVersion()).isEqualTo(state2.version());
        assertThat(cs1.getLastAcceptedState()).isEqualTo(state2);
        assertThat(value(cs1.getLastAcceptedState())).isEqualTo(13L);
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(state2.version() + 1, 20), node1,
            initialConfig, initialConfig, 13L);
        cs1.handlePublishRequest(new PublishRequest(state3));
    }

    public void testHandlePublishRequestWithBadTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertThat(cs1.handleJoin(v1)).isTrue();
            assertThat(cs1.electionWon()).isTrue();
        }
        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        ClusterState state2 = clusterState(term, 2L, node1, initialConfig, initialConfig, 42L);
        assertThatThrownBy(() -> cs1.handlePublishRequest(new PublishRequest(state2)))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("does not match current term");
    }

    // scenario when handling a publish request from a master that we already received a newer state from
    public void testHandlePublishRequestWithSameTermButOlderOrSameVersion() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertThat(cs1.handleJoin(v1)).isTrue();
            assertThat(cs1.electionWon()).isTrue();
        }
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(0, state2.version()), node1, initialConfig,
            initialConfig, 42L);
        assertThatThrownBy(() -> cs1.handlePublishRequest(new PublishRequest(state3)))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("lower or equal to current version");
    }

    // scenario when handling a publish request from a fresh master
    public void testHandlePublishRequestWithTermHigherThanLastAcceptedTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
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
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node1, publishResponse);
        assertThat(applyCommit.isPresent()).isTrue();
        assertThat(applyCommit.get().getSourceNode()).isEqualTo(node1);
        assertThat(applyCommit.get().getTerm()).isEqualTo(state2.term());
        assertThat(applyCommit.get().getVersion()).isEqualTo(state2.version());
    }

    public void testHandlePublishResponseWhenSteppedDownAsLeader() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node1, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        cs1.handleStartJoin(startJoinRequest2);
        assertThatThrownBy(() -> cs1.handlePublishResponse(randomFrom(node1, node2, node3), publishResponse))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("election not won");
    }

    public void testHandlePublishResponseWithoutPublishConfigQuorum() {
        VotingConfiguration configNode1 = VotingConfiguration.of(node1);
        VotingConfiguration configNode2 = VotingConfiguration.of(node2);
        ClusterState state1 = clusterState(0L, 0L, node1, configNode1, configNode1, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v2)).isTrue();
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, configNode1, configNode2, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node1, publishResponse);
        assertThat(applyCommit.isPresent()).isFalse();
    }

    public void testHandlePublishResponseWithoutCommitedConfigQuorum() {
        VotingConfiguration configNode1 = VotingConfiguration.of(node1);
        VotingConfiguration configNode2 = VotingConfiguration.of(node2);
        ClusterState state1 = clusterState(0L, 0L, node1, configNode1, configNode1, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v2)).isTrue();
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, configNode1, configNode2, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs2.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node2, publishResponse);
        assertThat(applyCommit.isPresent()).isFalse();
    }

    public void testHandlePublishResponseWithoutCommit() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node2, publishResponse);
        assertThat(applyCommit.isPresent()).isFalse();
    }

    public void testHandlePublishResponseWithBadTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        PublishResponse publishResponse = cs1.handlePublishRequest(new PublishRequest(state2));
        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        assertThatThrownBy(() -> cs1.handlePublishResponse(randomFrom(node1, node2, node3),
                                    new PublishResponse(term, publishResponse.getVersion())))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("does not match current term");
    }

    public void testHandlePublishResponseWithVersionMismatch() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        PublishResponse publishResponse = cs1.handlePublishRequest(new PublishRequest(state2));
        assertThatThrownBy(() -> cs1.handlePublishResponse(randomFrom(node1, node2, node3), publishResponse))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("does not match current version");
    }

    public void testHandleCommit() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v2)).isTrue();
        VotingConfiguration newConfig = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 7L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        cs1.handlePublishResponse(node1, publishResponse);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node2, publishResponse);
        assertThat(applyCommit.isPresent()).isTrue();
        assertThat(cs1.getLastCommittedConfiguration()).isEqualTo(initialConfig);
        cs1.handleCommit(applyCommit.get());
        assertThat(cs1.getLastCommittedConfiguration()).isEqualTo(newConfig);
    }

    public void testHandleCommitWithBadCurrentTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 7L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        cs1.handlePublishResponse(node1, publishResponse);
        long term = randomBoolean() ?
            randomLongBetween(startJoinRequest1.getTerm() + 1, 10) :
            randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        assertThatThrownBy(() -> cs1.handleCommit(new ApplyCommitRequest(node1, term, 2L)))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("does not match current term");
    }

    public void testHandleCommitWithBadLastAcceptedTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        assertThatThrownBy(() -> cs1.handleCommit(new ApplyCommitRequest(node1, startJoinRequest1.getTerm(), 2L)))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("does not match last accepted term");
    }

    public void testHandleCommitWithBadVersion() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(cs1.handleJoin(v1)).isTrue();
        assertThat(cs1.electionWon()).isTrue();
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 7L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        cs1.handlePublishRequest(publishRequest);
        assertThatThrownBy(() -> cs1.handleCommit(new ApplyCommitRequest(node1, startJoinRequest1.getTerm(), randomLongBetween(3, 10))))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("does not match current version");
    }

    public void testVoteCollection() {
        final CoordinationState.VoteCollection voteCollection = new CoordinationState.VoteCollection();
        assertThat(voteCollection.isEmpty()).isTrue();

        assertThat(voteCollection.addVote(
            new DiscoveryNode("master-ineligible", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT))).isFalse();
        assertThat(voteCollection.isEmpty()).isTrue();

        voteCollection.addVote(node1);
        assertThat(voteCollection.isEmpty()).isFalse();
        assertThat(voteCollection.containsVoteFor(node1)).isTrue();
        assertThat(voteCollection.containsVoteFor(node2)).isFalse();
        assertThat(voteCollection.containsVoteFor(node3)).isFalse();
        voteCollection.addVote(node2);
        assertThat(voteCollection.containsVoteFor(node1)).isTrue();
        assertThat(voteCollection.containsVoteFor(node2)).isTrue();
        assertThat(voteCollection.containsVoteFor(node3)).isFalse();
        assertThat(voteCollection.isQuorum(VotingConfiguration.of(node1, node2))).isTrue();
        assertThat(voteCollection.isQuorum(VotingConfiguration.of(node1))).isTrue();
        assertThat(voteCollection.isQuorum(VotingConfiguration.of(node3))).isFalse();

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
        new CoordinationStateTestCluster(IntStream.range(0, randomIntBetween(1, 5))
            .mapToObj(i -> new DiscoveryNode("node_" + i, buildNewFakeTransportAddress(), Version.CURRENT))
            .collect(Collectors.toList()), ElectionStrategy.DEFAULT_INSTANCE)
            .runRandomly();
    }

    public static CoordinationState createCoordinationState(PersistedState storage, DiscoveryNode localNode) {
        return new CoordinationState(localNode, storage, ElectionStrategy.DEFAULT_INSTANCE);
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
            .metadata(Metadata.builder()
                .clusterUUID(UUIDs.randomBase64UUID(random())) // generate cluster UUID deterministically for repeatable tests
                .coordinationMetadata(CoordinationMetadata.builder()
                        .term(term)
                        .lastCommittedConfiguration(lastCommittedConfig)
                        .lastAcceptedConfiguration(lastAcceptedConfig)
                        .build()))
            .stateUUID(UUIDs.randomBase64UUID(random())) // generate cluster state UUID deterministically for repeatable tests
            .build(), value);
    }

    public static ClusterState setValue(ClusterState clusterState, long value) {
        return ClusterState.builder(clusterState).metadata(
            Metadata.builder(clusterState.metadata())
                .persistentSettings(Settings.builder()
                    .put(clusterState.metadata().persistentSettings())
                    .put("value", value)
                    .build())
                .build())
            .build();
    }

    public static long value(ClusterState clusterState) {
        return clusterState.metadata().persistentSettings().getAsLong("value", 0L);
    }

    static class ClusterNode {

        final DiscoveryNode localNode;
        final PersistedState persistedState;
        CoordinationState state;

        ClusterNode(DiscoveryNode localNode) {
            this.localNode = localNode;
            persistedState = new InMemoryPersistedState(0L,
                clusterState(0L, 0L, localNode, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 0L));
            state = new CoordinationState(localNode, persistedState, ElectionStrategy.DEFAULT_INSTANCE);
        }

        void reboot() {
            state = new CoordinationState(localNode, persistedState, ElectionStrategy.DEFAULT_INSTANCE);
        }

        void setInitialState(VotingConfiguration initialConfig, long initialValue) {
            final ClusterState.Builder builder = ClusterState.builder(state.getLastAcceptedState());
            builder.metadata(Metadata.builder()
                    .coordinationMetadata(CoordinationMetadata.builder()
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
                    .map(cn -> cn.localNode.getId()).collect(Collectors.toSet()));
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
                    assertThat(mastersForTerm)
                        .as("Multiple masters " + mastersForTerm + " for term " + term)
                        .hasSize(1);
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
                        assertThat(clusterStateUUIDsForTermAndVersion)
                            .as("Multiple cluster states " + clusterStates1 + " for term " + term + " and version " + version)
                            .hasSize(1);

                        Set<Long> clusterStateValuesForTermAndVersion = clusterStates1.stream().collect(Collectors.groupingBy(
                            CoordinationStateTests::value
                        )).keySet();

                        assertThat(clusterStateValuesForTermAndVersion)
                            .as("Multiple cluster states " + clusterStates1 + " for term " + term + " and version " + version)
                            .hasSize(1);
                    });
                });
        }

    }
}
