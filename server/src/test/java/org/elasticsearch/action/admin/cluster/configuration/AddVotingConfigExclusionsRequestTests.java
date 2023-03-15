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
package org.elasticsearch.action.admin.cluster.configuration;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.common.unit.TimeValue;

public class AddVotingConfigExclusionsRequestTests extends ESTestCase {
    private static final String NODE_IDENTIFIERS_INCORRECTLY_SET_MSG = "Please set node identifiers correctly. " +
                                                            "One and only one of [node_name], [node_names] and [node_ids] has to be set";

    @Before
    public void resetDeprecationLogger() {
        AddVotingConfigExclusionsRequest.DEPRECATION_LOGGER.resetLRU();
    }

    public void testSerialization() throws IOException {
        int descriptionCount = between(1, 5);
        String[] descriptions = new String[descriptionCount];
        for (int i = 0; i < descriptionCount; i++) {
            descriptions[i] = randomAlphaOfLength(10);
        }
        TimeValue timeout = TimeValue.timeValueMillis(between(0, 30000));
        final AddVotingConfigExclusionsRequest originalRequest = new AddVotingConfigExclusionsRequest(descriptions, Strings.EMPTY_ARRAY,
                                                                                                        Strings.EMPTY_ARRAY, timeout);
        final AddVotingConfigExclusionsRequest deserialized = copyWriteable(originalRequest, writableRegistry(),
            AddVotingConfigExclusionsRequest::new);
        assertThat(deserialized.getNodeDescriptions()).isEqualTo(originalRequest.getNodeDescriptions());
        assertThat(deserialized.getTimeout()).isEqualTo(originalRequest.getTimeout());
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testSerializationForNodeIdOrNodeName() throws IOException {
        AddVotingConfigExclusionsRequest originalRequest = new AddVotingConfigExclusionsRequest(Strings.EMPTY_ARRAY,
                                                                new String[]{"nodeId1", "nodeId2"}, Strings.EMPTY_ARRAY, TimeValue.ZERO);
        AddVotingConfigExclusionsRequest deserialized = copyWriteable(originalRequest, writableRegistry(),
                                                                        AddVotingConfigExclusionsRequest::new);

        assertThat(deserialized.getNodeDescriptions()).isEqualTo(originalRequest.getNodeDescriptions());
        assertThat(deserialized.getNodeIds()).isEqualTo(originalRequest.getNodeIds());
        assertThat(deserialized.getNodeNames()).isEqualTo(originalRequest.getNodeNames());
        assertThat(deserialized.getTimeout()).isEqualTo(originalRequest.getTimeout());

        originalRequest = new AddVotingConfigExclusionsRequest("nodeName1", "nodeName2");
        deserialized = copyWriteable(originalRequest, writableRegistry(), AddVotingConfigExclusionsRequest::new);

        assertThat(deserialized.getNodeDescriptions()).isEqualTo(originalRequest.getNodeDescriptions());
        assertThat(deserialized.getNodeIds()).isEqualTo(originalRequest.getNodeIds());
        assertThat(deserialized.getNodeNames()).isEqualTo(originalRequest.getNodeNames());
        assertThat(deserialized.getTimeout()).isEqualTo(originalRequest.getTimeout());
    }

    @Test
    public void testResolve() {
        final DiscoveryNode localNode = new DiscoveryNode(
                "local",
                "local",
                buildNewFakeTransportAddress(),
                emptyMap(),
                singleton(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion localNodeExclusion = new VotingConfigExclusion(localNode);
        final DiscoveryNode otherNode1 = new DiscoveryNode(
                "other1",
                "other1",
                buildNewFakeTransportAddress(),
                emptyMap(),
                singleton(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        final DiscoveryNode otherNode2 = new DiscoveryNode(
                "other2",
                "other2",
                buildNewFakeTransportAddress(),
                emptyMap(),
                singleton(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion otherNode2Exclusion = new VotingConfigExclusion(otherNode2);
        final DiscoveryNode otherDataNode
            = new DiscoveryNode("data", "data", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster")).nodes(new Builder()
            .add(localNode).add(otherNode1).add(otherNode2).add(otherDataNode).localNodeId(localNode.getId())).build();

        assertThat(makeRequestWithNodeDescriptions("_all").resolveVotingConfigExclusions(clusterState))
            .containsExactlyInAnyOrder(localNodeExclusion, otherNode1Exclusion, otherNode2Exclusion);
        assertThat(makeRequestWithNodeDescriptions("_local").resolveVotingConfigExclusions(clusterState))
            .containsExactly(localNodeExclusion);
        assertThat(makeRequestWithNodeDescriptions("other*").resolveVotingConfigExclusions(clusterState))
            .containsExactlyInAnyOrder(otherNode1Exclusion, otherNode2Exclusion);

        assertThatThrownBy(() -> makeRequestWithNodeDescriptions("not-a-node").resolveVotingConfigExclusions(clusterState))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("add voting config exclusions request for [not-a-node] matched no master-eligible nodes");
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    @Test
    public void testResolveAllNodeIdentifiersNullOrEmpty() {
        assertThatThrownBy(() -> new AddVotingConfigExclusionsRequest(
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            TimeValue.ZERO)
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(NODE_IDENTIFIERS_INCORRECTLY_SET_MSG);
    }

    @Test
    public void testResolveMoreThanOneNodeIdentifiersSet() {
        assertThatThrownBy(() -> new AddVotingConfigExclusionsRequest(
            new String[]{"local"},
            new String[]{"nodeId"},
            Strings.EMPTY_ARRAY,
            TimeValue.ZERO)
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(NODE_IDENTIFIERS_INCORRECTLY_SET_MSG);

        assertThatThrownBy(() -> new AddVotingConfigExclusionsRequest(
            new String[]{"local"},
            Strings.EMPTY_ARRAY,
            new String[]{"nodeName"},
            TimeValue.ZERO)
        ).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(NODE_IDENTIFIERS_INCORRECTLY_SET_MSG);

        assertThatThrownBy(() -> new AddVotingConfigExclusionsRequest(
            Strings.EMPTY_ARRAY,
            new String[]{"nodeId"},
            new String[]{"nodeName"},
            TimeValue.ZERO
        )).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(NODE_IDENTIFIERS_INCORRECTLY_SET_MSG);

        assertThatThrownBy(() -> new AddVotingConfigExclusionsRequest(
            new String[]{"local"},
            new String[]{"nodeId"},
            new String[]{"nodeName"},
            TimeValue.ZERO
        )).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(NODE_IDENTIFIERS_INCORRECTLY_SET_MSG);
    }

    public void testResolveByNodeIds() {
        final DiscoveryNode node1 = new DiscoveryNode(
            "nodeName1",
            "nodeId1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT);
        final VotingConfigExclusion node1Exclusion = new VotingConfigExclusion(node1);

        final DiscoveryNode node2 = new DiscoveryNode(
            "nodeName2",
            "nodeId2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT);
        final VotingConfigExclusion node2Exclusion = new VotingConfigExclusion(node2);

        final DiscoveryNode node3 = new DiscoveryNode(
            "nodeName3",
            "nodeId3",
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT);

        final VotingConfigExclusion unresolvableVotingConfigExclusion = new VotingConfigExclusion("unresolvableNodeId",
                                                                                                VotingConfigExclusion.MISSING_VALUE_MARKER);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
                    .nodes(new Builder().add(node1).add(node2).add(node3).localNodeId(node1.getId())).build();

        assertThat(new AddVotingConfigExclusionsRequest(
            Strings.EMPTY_ARRAY,
            new String[]{"nodeId1", "nodeId2"},
            Strings.EMPTY_ARRAY, TimeValue.ZERO
        ).resolveVotingConfigExclusions(clusterState))
            .containsExactlyInAnyOrder(node1Exclusion, node2Exclusion);

        assertThat(
            new AddVotingConfigExclusionsRequest(
                Strings.EMPTY_ARRAY,
                new String[]{"nodeId1", "unresolvableNodeId"},
                Strings.EMPTY_ARRAY,
                TimeValue.ZERO
            ).resolveVotingConfigExclusions(clusterState))
            .containsExactlyInAnyOrder(node1Exclusion, unresolvableVotingConfigExclusion);
    }

    public void testResolveByNodeNames() {
        final DiscoveryNode node1 = new DiscoveryNode("nodeName1",
                                                        "nodeId1",
                                                        buildNewFakeTransportAddress(),
                                                        emptyMap(),
                                                        singleton(DiscoveryNodeRole.MASTER_ROLE),
                                                        Version.CURRENT);
        final VotingConfigExclusion node1Exclusion = new VotingConfigExclusion(node1);

        final DiscoveryNode node2 = new DiscoveryNode("nodeName2",
                                                        "nodeId2",
                                                        buildNewFakeTransportAddress(),
                                                        emptyMap(),
                                                        singleton(DiscoveryNodeRole.MASTER_ROLE),
                                                        Version.CURRENT);
        final VotingConfigExclusion node2Exclusion = new VotingConfigExclusion(node2);

        final DiscoveryNode node3 = new DiscoveryNode("nodeName3",
                                                        "nodeId3",
                                                        buildNewFakeTransportAddress(),
                                                        emptyMap(),
                                                        singleton(DiscoveryNodeRole.MASTER_ROLE),
                                                        Version.CURRENT);

        final VotingConfigExclusion unresolvableVotingConfigExclusion = new VotingConfigExclusion(
                                                                        VotingConfigExclusion.MISSING_VALUE_MARKER, "unresolvableNodeName");

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
                                                .nodes(new Builder().add(node1).add(node2).add(node3).localNodeId(node1.getId())).build();

        assertThat(new AddVotingConfigExclusionsRequest("nodeName1", "nodeName2").resolveVotingConfigExclusions(clusterState))
            .containsExactlyInAnyOrder(node1Exclusion, node2Exclusion);

        assertThat(new AddVotingConfigExclusionsRequest("nodeName1", "unresolvableNodeName").resolveVotingConfigExclusions(clusterState))
            .containsExactlyInAnyOrder(node1Exclusion, unresolvableVotingConfigExclusion);
    }

    public void testResolveRemoveExistingVotingConfigExclusions() {
        final DiscoveryNode node1 = new DiscoveryNode("nodeName1",
                                                        "nodeId1",
                                                        buildNewFakeTransportAddress(),
                                                        emptyMap(),
                                                        singleton(DiscoveryNodeRole.MASTER_ROLE),
                                                        Version.CURRENT);

        final DiscoveryNode node2 = new DiscoveryNode("nodeName2",
                                                        "nodeId2",
                                                        buildNewFakeTransportAddress(),
                                                        emptyMap(),
                                                        singleton(DiscoveryNodeRole.MASTER_ROLE),
                                                        Version.CURRENT);
        final VotingConfigExclusion node2Exclusion = new VotingConfigExclusion(node2);

        final DiscoveryNode node3 = new DiscoveryNode("nodeName3",
                                                        "nodeId3",
                                                        buildNewFakeTransportAddress(),
                                                        emptyMap(),
                                                        singleton(DiscoveryNodeRole.MASTER_ROLE),
                                                        Version.CURRENT);

        final VotingConfigExclusion existingVotingConfigExclusion = new VotingConfigExclusion(node1);

        Metadata metadata = Metadata.builder()
                                    .coordinationMetadata(CoordinationMetadata.builder()
                                        .addVotingConfigExclusion(existingVotingConfigExclusion).build())
                                    .build();

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster")).metadata(metadata)
                                                .nodes(new Builder().add(node1).add(node2).add(node3).localNodeId(node1.getId())).build();

        assertThat(new AddVotingConfigExclusionsRequest(Strings.EMPTY_ARRAY, new String[]{"nodeId1", "nodeId2"},
                                                        Strings.EMPTY_ARRAY, TimeValue.ZERO).resolveVotingConfigExclusions(clusterState))
            .containsExactly(node2Exclusion);
    }

    public void testResolveAndCheckMaximum() {
        final DiscoveryNode localNode = new DiscoveryNode(
                "local",
                "local",
                buildNewFakeTransportAddress(),
                emptyMap(),
                singleton(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion localNodeExclusion = new VotingConfigExclusion(localNode);
        final DiscoveryNode otherNode1 = new DiscoveryNode(
                "other1",
                "other1",
                buildNewFakeTransportAddress(),
                emptyMap(),
                singleton(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
        final VotingConfigExclusion otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        final DiscoveryNode otherNode2 = new DiscoveryNode(
                "other2",
                "other2",
                buildNewFakeTransportAddress(),
                emptyMap(),
                singleton(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);

        final ClusterState.Builder builder = ClusterState.builder(new ClusterName("cluster")).nodes(new Builder()
            .add(localNode).add(otherNode1).add(otherNode2).localNodeId(localNode.getId()));
        builder.metadata(Metadata.builder()
                .coordinationMetadata(CoordinationMetadata.builder().addVotingConfigExclusion(otherNode1Exclusion).build()));
        final ClusterState clusterState = builder.build();

        assertThat(makeRequestWithNodeDescriptions("_local").resolveVotingConfigExclusionsAndCheckMaximum(clusterState, 2, "setting.name"))
            .containsExactly(localNodeExclusion);
        assertThatThrownBy(() ->
            makeRequestWithNodeDescriptions("_local").resolveVotingConfigExclusionsAndCheckMaximum(clusterState, 1, "setting.name"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "add voting config exclusions request for [_local] would add [1] exclusions to the existing [1] which would " +
                "exceed the maximum of [1] set by [setting.name]");
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    private static AddVotingConfigExclusionsRequest makeRequestWithNodeDescriptions(String... nodeDescriptions) {
        return new AddVotingConfigExclusionsRequest(nodeDescriptions, Strings.EMPTY_ARRAY,
                                                    Strings.EMPTY_ARRAY, TimeValue.timeValueSeconds(30));
    }
}
