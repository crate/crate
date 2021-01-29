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

package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class GatewayMetaStatePersistedStateTests extends ESTestCase {
    private NodeEnvironment nodeEnvironment;
    private ClusterName clusterName;
    private Settings settings;
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        nodeEnvironment = newNodeEnvironment();
        localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(),
                Sets.newHashSet(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        clusterName = new ClusterName(randomAlphaOfLength(10));
        settings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value()).build();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        nodeEnvironment.close();
        super.tearDown();
    }

    private CoordinationState.PersistedState newGatewayPersistedState() {
        final MockGatewayMetaState gateway = new MockGatewayMetaState(localNode);
        gateway.start(settings, nodeEnvironment, xContentRegistry());
        final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
        assertThat(persistedState, not(instanceOf(InMemoryPersistedState.class)));
        return persistedState;
    }

    private CoordinationState.PersistedState maybeNew(CoordinationState.PersistedState persistedState) {
        if (randomBoolean()) {
            return newGatewayPersistedState();
        }
        return persistedState;
    }

    public void testInitialState() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();
        ClusterState state = gateway.getLastAcceptedState();
        assertThat(state.getClusterName(), equalTo(clusterName));
        assertTrue(Metadata.isGlobalStateEquals(state.metadata(), Metadata.EMPTY_METADATA));
        assertThat(state.getVersion(), equalTo(Manifest.empty().getClusterStateVersion()));
        assertThat(state.getNodes().getLocalNode(), equalTo(localNode));

        long currentTerm = gateway.getCurrentTerm();
        assertThat(currentTerm, equalTo(Manifest.empty().getCurrentTerm()));
    }

    public void testSetCurrentTerm() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            final long currentTerm = randomNonNegativeLong();
            gateway.setCurrentTerm(currentTerm);
            gateway = maybeNew(gateway);
            assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
        }
    }

    private ClusterState createClusterState(long version, Metadata metadata) {
        return ClusterState.builder(clusterName).
                nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build()).
                version(version).
                metadata(metadata).
                build();
    }

    private CoordinationMetadata createCoordinationMetadata(long term) {
        CoordinationMetadata.Builder builder = CoordinationMetadata.builder();
        builder.term(term);
        builder.lastAcceptedConfiguration(
                new CoordinationMetadata.VotingConfiguration(
                        Sets.newHashSet(generateRandomStringArray(10, 10, false))));
        builder.lastCommittedConfiguration(
                new CoordinationMetadata.VotingConfiguration(
                        Sets.newHashSet(generateRandomStringArray(10, 10, false))));
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            builder.addVotingConfigExclusion(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }

        return builder.build();
    }

    private IndexMetadata createIndexMetadata(String indexName, int numberOfShards, long version) {
        return IndexMetadata.builder(indexName).settings(
                Settings.builder()
                        .put(IndexMetadata.SETTING_INDEX_UUID, indexName)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .build()
        ).version(version).build();
    }

    private void assertClusterStateEqual(ClusterState expected, ClusterState actual) {
        assertThat(actual.version(), equalTo(expected.version()));
        assertTrue(Metadata.isGlobalStateEquals(actual.metadata(), expected.metadata()));
        for (IndexMetadata indexMetadata : expected.metadata()) {
            assertThat(actual.metadata().index(indexMetadata.getIndex()), equalTo(indexMetadata));
        }
    }

    public void testSetLastAcceptedState() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();
        final long term = randomNonNegativeLong();

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            final long version = randomNonNegativeLong();
            final String indexName = randomAlphaOfLength(10);
            final IndexMetadata indexMetadata = createIndexMetadata(indexName, randomIntBetween(1,5), randomNonNegativeLong());
            final Metadata metadata = Metadata.builder().
                    persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build()).
                    coordinationMetadata(createCoordinationMetadata(term)).
                    put(indexMetadata, false).
                    build();
            ClusterState state = createClusterState(version, metadata);

            gateway.setLastAcceptedState(state);
            gateway = maybeNew(gateway);

            ClusterState lastAcceptedState = gateway.getLastAcceptedState();
            assertClusterStateEqual(state, lastAcceptedState);
        }
    }

    public void testSetLastAcceptedStateTermChanged() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();

        final String indexName = randomAlphaOfLength(10);
        final int numberOfShards = randomIntBetween(1, 5);
        final long version = randomNonNegativeLong();
        final long term = randomNonNegativeLong();
        final IndexMetadata indexMetadata = createIndexMetadata(indexName, numberOfShards, version);
        final ClusterState state = createClusterState(randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(createCoordinationMetadata(term)).put(indexMetadata, false).build());
        gateway.setLastAcceptedState(state);

        gateway = maybeNew(gateway);
        final long newTerm = randomValueOtherThan(term, ESTestCase::randomNonNegativeLong);
        final int newNumberOfShards = randomValueOtherThan(numberOfShards, () -> randomIntBetween(1,5));
        final IndexMetadata newIndexMetadata = createIndexMetadata(indexName, newNumberOfShards, version);
        final ClusterState newClusterState = createClusterState(randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(createCoordinationMetadata(newTerm)).put(newIndexMetadata, false).build());
        gateway.setLastAcceptedState(newClusterState);

        gateway = maybeNew(gateway);
        assertThat(gateway.getLastAcceptedState().metadata().index(indexName), equalTo(newIndexMetadata));
    }

    public void testCurrentTermAndTermAreDifferent() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();

        long currentTerm = randomNonNegativeLong();
        long term  = randomValueOtherThan(currentTerm, ESTestCase::randomNonNegativeLong);

        gateway.setCurrentTerm(currentTerm);
        gateway.setLastAcceptedState(createClusterState(randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(term).build()).build()));

        gateway = maybeNew(gateway);
        assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
        assertThat(gateway.getLastAcceptedState().coordinationMetadata().term(), equalTo(term));
    }

    public void testMarkAcceptedConfigAsCommitted() {
        CoordinationState.PersistedState gateway = newGatewayPersistedState();

        //generate random coordinationMetadata with different lastAcceptedConfiguration and lastCommittedConfiguration
        CoordinationMetadata coordinationMetadata;
        do {
            coordinationMetadata = createCoordinationMetadata(randomNonNegativeLong());
        } while (coordinationMetadata.getLastAcceptedConfiguration().equals(coordinationMetadata.getLastCommittedConfiguration()));

        ClusterState state = createClusterState(randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(coordinationMetadata)
                    .clusterUUID(randomAlphaOfLength(10)).build());
        gateway.setLastAcceptedState(state);

        gateway = maybeNew(gateway);
        assertThat(gateway.getLastAcceptedState().getLastAcceptedConfiguration(),
                not(equalTo(gateway.getLastAcceptedState().getLastCommittedConfiguration())));
        gateway.markLastAcceptedStateAsCommitted();

        CoordinationMetadata expectedCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
                .lastCommittedConfiguration(coordinationMetadata.getLastAcceptedConfiguration()).build();
        ClusterState expectedClusterState =
                ClusterState.builder(state).metadata(Metadata.builder().coordinationMetadata(expectedCoordinationMetadata)
                    .clusterUUID(state.metadata().clusterUUID()).clusterUUIDCommitted(true).build()).build();

        gateway = maybeNew(gateway);
        assertClusterStateEqual(expectedClusterState, gateway.getLastAcceptedState());
        gateway.markLastAcceptedStateAsCommitted();

        gateway = maybeNew(gateway);
        assertClusterStateEqual(expectedClusterState, gateway.getLastAcceptedState());
    }
}
