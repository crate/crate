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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.BOOTSTRAP_PLACEHOLDER_PREFIX;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;

public class ClusterBootstrapServiceTests extends ESTestCase {

    private DiscoveryNode localNode, otherNode1, otherNode2;
    private DeterministicTaskQueue deterministicTaskQueue;
    private TransportService transportService;

    @Before
    public void createServices() {
        localNode = newDiscoveryNode("local");
        otherNode1 = newDiscoveryNode("other1");
        otherNode2 = newDiscoveryNode("other2");

        deterministicTaskQueue = new DeterministicTaskQueue(builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                throw new AssertionError("unexpected " + action);
            }
        };

        transportService = transport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            boundTransportAddress -> localNode,
            null
        );
    }

    private DiscoveryNode newDiscoveryNode(String nodeName) {
        return new DiscoveryNode(
                nodeName,
                randomAlphaOfLength(10),
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
    }

    public void testBootstrapsAutomaticallyWithDefaultConfiguration() {
        final Settings.Builder settings = Settings.builder();
        final long timeout;
        if (randomBoolean()) {
            timeout = UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        } else {
            timeout = randomLongBetween(1, 10000);
            settings.put(UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING.getKey(), timeout + "ms");
        }

        final AtomicReference<Supplier<Iterable<DiscoveryNode>>> discoveredNodesSupplier = new AtomicReference<>(() -> {
            throw new AssertionError("should not be called yet");
        });

        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService
            = new ClusterBootstrapService(settings.build(), transportService, () -> discoveredNodesSupplier.get().get(),
            () -> false, vc -> {
            assertThat(bootstrapped.compareAndSet(false, true)).isTrue();
            assertThat(vc.getNodeIds()).isEqualTo(Stream.of(localNode, otherNode1, otherNode2).map(DiscoveryNode::getId).collect(Collectors.toSet()));
            assertThat(deterministicTaskQueue.getCurrentTimeMillis()).isGreaterThanOrEqualTo(timeout);
        });

        deterministicTaskQueue.scheduleAt(timeout - 1,
            () -> discoveredNodesSupplier.set(() -> Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toSet())));

        transportService.start();
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(bootstrapped.get()).isTrue();
    }

    public void testDoesNothingByDefaultIfHostsProviderConfigured() {
        testDoesNothingWithSettings(builder().putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey()));
    }

    public void testDoesNothingByDefaultIfSeedHostsConfigured() {
        testDoesNothingWithSettings(builder().putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()));
    }

    public void testDoesNothingByDefaultIfMasterNodesConfigured() {
        testDoesNothingWithSettings(builder().putList(INITIAL_MASTER_NODES_SETTING.getKey()));
    }

    public void testDoesNothingByDefaultOnMasterIneligibleNodes() {
        localNode = new DiscoveryNode("local", randomAlphaOfLength(10), buildNewFakeTransportAddress(), emptyMap(), emptySet(),
            Version.CURRENT);
        testDoesNothingWithSettings(Settings.builder());
    }

    private void testDoesNothingWithSettings(Settings.Builder builder) {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(builder.build(), transportService, () -> {
            throw new AssertionError("should not be called");
        }, () -> false, vc -> {
            throw new AssertionError("should not be called");
        });
        transportService.start();
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
        deterministicTaskQueue.runAllTasks();
    }

    @Test
    public void testThrowsExceptionOnDuplicates() {
        assertThatThrownBy(() -> {
            new ClusterBootstrapService(builder().putList(
                INITIAL_MASTER_NODES_SETTING.getKey(), "duplicate-requirement", "duplicate-requirement").build(),
                transportService, Collections::emptyList, () -> false, vc -> {
                throw new AssertionError("should not be called");
            });
        }).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(INITIAL_MASTER_NODES_SETTING.getKey())
            .hasMessageContaining("duplicate-requirement");
    }

    public void testBootstrapsOnDiscoveryOfAllRequiredNodes() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();

        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService, () -> Stream.of(otherNode1, otherNode2).collect(Collectors.toList()), () -> false, vc -> {
            assertThat(bootstrapped.compareAndSet(false, true)).isTrue();
            assertThat(vc.getNodeIds()).containsExactlyInAnyOrder(localNode.getId(), otherNode1.getId(), otherNode2.getId());
            assertThat(vc.getNodeIds()).allSatisfy(
                nId -> assertThat(nId).doesNotContain("placeholder"));
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isTrue();

        bootstrapped.set(false);
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isFalse(); // should only bootstrap once
    }

    public void testBootstrapsOnDiscoveryOfTwoOfThreeRequiredNodes() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();

        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService, () -> singletonList(otherNode1), () -> false, vc -> {
            assertThat(bootstrapped.compareAndSet(false, true)).isTrue();
            assertThat(vc.getNodeIds())
                .contains(localNode.getId())
                .contains(otherNode1.getId())
                .anySatisfy(nId -> assertThat(nId)
                    .startsWith(BOOTSTRAP_PLACEHOLDER_PREFIX)
                    .contains(otherNode2.getName())
            );
            assertThat(vc.hasQuorum(Stream.of(localNode, otherNode1).map(DiscoveryNode::getId).toList())).isTrue();
            assertThat(vc.hasQuorum(singletonList(localNode.getId()))).isFalse();
            assertThat(vc.hasQuorum(singletonList(otherNode1.getId()))).isFalse();
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isTrue();

        bootstrapped.set(false);
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isFalse(); // should only bootstrap once
    }

    public void testBootstrapsOnDiscoveryOfThreeOfFiveRequiredNodes() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();

        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName(),
            "missing-node-1", "missing-node-2").build(),
            transportService, () -> Stream.of(otherNode1, otherNode2).collect(Collectors.toList()), () -> false, vc -> {
            assertThat(bootstrapped.compareAndSet(false, true)).isTrue();
            assertThat(vc.getNodeIds()).hasSize(5);
            assertThat(vc.getNodeIds()).contains(localNode.getId());
            assertThat(vc.getNodeIds()).contains(otherNode1.getId());
            assertThat(vc.getNodeIds()).contains(otherNode2.getId());

            final List<String> placeholders
                = vc.getNodeIds().stream().filter(ClusterBootstrapService::isBootstrapPlaceholder).collect(Collectors.toList());
            assertThat(placeholders).hasSize(2);
            assertThat(placeholders.get(0)).isNotEqualTo(placeholders.get(1));
            assertThat(placeholders).satisfiesExactlyInAnyOrder(
                ph -> assertThat(ph).contains("missing-node-1"),
                ph -> assertThat(ph).contains("missing-node-2"));

            assertThat(vc.hasQuorum(Stream.of(localNode, otherNode1, otherNode2).map(DiscoveryNode::getId).collect(Collectors.toList()))).isTrue();
            assertThat(vc.hasQuorum(Stream.of(localNode, otherNode1).map(DiscoveryNode::getId).collect(Collectors.toList()))).isFalse();
            assertThat(vc.hasQuorum(Stream.of(localNode, otherNode1).map(DiscoveryNode::getId).collect(Collectors.toList()))).isFalse();
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isTrue();

        bootstrapped.set(false);
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isFalse(); // should only bootstrap once
    }

    public void testDoesNotBootstrapIfNoNodesDiscovered() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService, Collections::emptyList, () -> true, vc -> {
            throw new AssertionError("should not be called");
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapIfTwoOfFiveNodesDiscovered() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(),
            localNode.getName(), otherNode1.getName(), otherNode2.getName(), "not-a-node-1", "not-a-node-2").build(),
            transportService, () -> Stream.of(otherNode1).collect(Collectors.toList()), () -> false, vc -> {
            throw new AssertionError("should not be called");
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapIfThreeOfSixNodesDiscovered() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(),
            localNode.getName(), otherNode1.getName(), otherNode2.getName(), "not-a-node-1", "not-a-node-2", "not-a-node-3").build(),
            transportService, () -> Stream.of(otherNode1, otherNode2).collect(Collectors.toList()), () -> false, vc -> {
            throw new AssertionError("should not be called");
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapIfAlreadyBootstrapped() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService, () -> Stream.of(otherNode1, otherNode2).collect(Collectors.toList()), () -> true, vc -> {
            throw new AssertionError("should not be called");
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapsOnNonMasterNode() {
        localNode = new DiscoveryNode("local", randomAlphaOfLength(10), buildNewFakeTransportAddress(), emptyMap(), emptySet(),
            Version.CURRENT);
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService, () ->
            Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toList()), () -> false, vc -> {
            throw new AssertionError("should not be called");
        });
        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapsIfLocalNodeNotInInitialMasterNodes() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService, () ->
            Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toList()), () -> false, vc -> {
            throw new AssertionError("should not be called");
        });
        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapsIfNotConfigured() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey()).build(), transportService,
            () -> Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toList()), () -> false, vc -> {
            throw new AssertionError("should not be called");
        });
        transportService.start();
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testRetriesBootstrappingOnException() {
        final AtomicLong bootstrappingAttempts = new AtomicLong();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService, () -> Stream.of(otherNode1, otherNode2).collect(Collectors.toList()), () -> false, vc -> {
            bootstrappingAttempts.incrementAndGet();
            if (bootstrappingAttempts.get() < 5L) {
                throw new ElasticsearchException("test");
            }
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrappingAttempts.get()).isGreaterThanOrEqualTo(5L);
        assertThat(deterministicTaskQueue.getCurrentTimeMillis()).isGreaterThanOrEqualTo(40000L);
    }

    public void testCancelsBootstrapIfRequirementMatchesMultipleNodes() {
        AtomicReference<Iterable<DiscoveryNode>> discoveredNodes
            = new AtomicReference<>(Stream.of(otherNode1, otherNode2).collect(Collectors.toList()));
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getAddress().getAddress()).build(),
            transportService, discoveredNodes::get, () -> false, vc -> {
            throw new AssertionError("should not be called");
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();

        discoveredNodes.set(emptyList());
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testCancelsBootstrapIfNodeMatchesMultipleRequirements() {
        AtomicReference<Iterable<DiscoveryNode>> discoveredNodes
            = new AtomicReference<>(Stream.of(otherNode1, otherNode2).collect(Collectors.toList()));
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), otherNode1.getAddress().toString(), otherNode1.getName())
                .build(),
            transportService, discoveredNodes::get, () -> false, vc -> {
            throw new AssertionError("should not be called");
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();

        discoveredNodes.set(Stream.of(
                new DiscoveryNode(
                        otherNode1.getName(),
                        randomAlphaOfLength(10),
                        buildNewFakeTransportAddress(),
                        emptyMap(),
                        Set.of(DiscoveryNodeRole.MASTER_ROLE),
                        Version.CURRENT),
                new DiscoveryNode(
                        "yet-another-node",
                        randomAlphaOfLength(10),
                        otherNode1.getAddress(),
                        emptyMap(),
                        Set.of(DiscoveryNodeRole.MASTER_ROLE),
                        Version.CURRENT))
                .collect(Collectors.toList()));

        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testMatchesOnNodeName() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName()).build(), transportService,
            Collections::emptyList, () -> false, vc -> assertThat(bootstrapped.compareAndSet(false, true)).isTrue());

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isTrue();
    }

    public void testMatchesOnHostName() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getHostName()).build(), transportService,
            Collections::emptyList, () -> false, vc -> assertThat(bootstrapped.compareAndSet(false, true)).isTrue());

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isTrue();
    }

    public void testMatchesOnNodeAddress() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getAddress().toString()).build(), transportService,
            Collections::emptyList, () -> false, vc -> assertThat(bootstrapped.compareAndSet(false, true)).isTrue());

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isTrue();
    }

    public void testMatchesOnNodeHostAddress() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getAddress().getAddress()).build(),
            transportService, Collections::emptyList, () -> false, vc -> assertThat(bootstrapped.compareAndSet(false, true)).isTrue());

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isTrue();
    }

    public void testDoesNotJustMatchEverything() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), randomAlphaOfLength(10)).build(), transportService,
            Collections::emptyList, () -> false, vc -> {
            throw new AssertionError("should not be called");
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotIncludeExtraNodes() {
        final DiscoveryNode extraNode = newDiscoveryNode("extra-node");
        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(Settings.builder().putList(
            INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService, () -> Stream.of(otherNode1, otherNode2, extraNode).collect(Collectors.toList()), () -> false,
            vc -> {
                assertThat(bootstrapped.compareAndSet(false, true)).isTrue();
                assertThat(vc.getNodeIds()).doesNotContain(extraNode.getId());
            });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isTrue();
    }

    public void testBootstrapsAutomaticallyWithSingleNodeDiscovery() {
        final Settings.Builder settings = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE)
            .put(NODE_NAME_SETTING.getKey(), localNode.getName());
        final AtomicBoolean bootstrapped = new AtomicBoolean();

        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(settings.build(),
            transportService, () -> emptyList(), () -> false, vc -> {
            assertThat(bootstrapped.compareAndSet(false, true)).isTrue();
            assertThat(vc.getNodeIds()).containsExactly(localNode.getId());
            assertThat(vc.hasQuorum(singletonList(localNode.getId()))).isTrue();
        });

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isTrue();

        bootstrapped.set(false);
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrapped.get()).isFalse(); // should only bootstrap once
    }

    @Test
    public void testFailBootstrapWithBothSingleNodeDiscoveryAndInitialMasterNodes() {
        final Settings.Builder settings = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE)
            .put(NODE_NAME_SETTING.getKey(), localNode.getName())
            .put(INITIAL_MASTER_NODES_SETTING.getKey(), "test");

        assertThatThrownBy(() -> new ClusterBootstrapService(settings.build(),
                                transportService, () -> emptyList(), () -> false, vc -> fail()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "setting [" + INITIAL_MASTER_NODES_SETTING.getKey() + "] is not allowed when [discovery.type] is set " +
                "to [single-node]");
    }

    @Test
    public void testFailBootstrapNonMasterEligibleNodeWithSingleNodeDiscovery() {
        final Settings.Builder settings = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE)
            .put(NODE_NAME_SETTING.getKey(), localNode.getName())
            .put(Node.NODE_MASTER_SETTING.getKey(), false);

        assertThatThrownBy(() -> new ClusterBootstrapService(settings.build(),
                                    transportService, () -> emptyList(), () -> false, vc -> fail()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("node with [discovery.type] set to [single-node] must be master-eligible");
    }
}
