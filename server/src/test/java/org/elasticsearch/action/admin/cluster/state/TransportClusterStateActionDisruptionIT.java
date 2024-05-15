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
package org.elasticsearch.action.admin.cluster.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.common.unit.TimeValue;

@IntegTestCase.ClusterScope(numDataNodes = 0, scope = IntegTestCase.Scope.TEST)
public class TransportClusterStateActionDisruptionIT extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testNonLocalRequestAlwaysFindsMaster() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            ClusterStateRequest request = new ClusterStateRequest()
                .clear()
                .nodes(true)
                .masterNodeTimeout(TimeValue.timeValueMillis(100));
            final ClusterStateResponse clusterStateResponse;
            try {
                clusterStateResponse = FutureUtils.get(client().admin().cluster().state(request));
            } catch (MasterNotDiscoveredException e) {
                return; // ok, we hit the disconnected node
            }
            assertNotNull("should always contain a master node", clusterStateResponse.getState().nodes().getMasterNodeId());
        });
    }

    public void testLocalRequestAlwaysSucceeds() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(cluster().getNodeNames());
            var discoveryNodes = FutureUtils.get(
                client(node).admin().cluster().state(
                    new ClusterStateRequest()
                        .clear()
                        .local(true)
                        .nodes(true)
                        .masterNodeTimeout(TimeValue.timeValueMillis(100))
                )).getState().nodes();
            for (DiscoveryNode discoveryNode : discoveryNodes) {
                if (discoveryNode.getName().equals(node)) {
                    return;
                }
            }
            fail("nodes did not contain [" + node + "]: " + discoveryNodes);
        });
    }

    @Test
    public void testNonLocalRequestAlwaysFindsMasterAndWaitsForMetadata() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(cluster().getNodeNames());
            final long metadataVersion
                = cluster().getInstance(ClusterService.class, node).getClusterApplierService().state().metadata().version();
            final long waitForMetadataVersion = randomLongBetween(Math.max(1, metadataVersion - 3), metadataVersion + 5);
            var clusterStateRequest = new ClusterStateRequest()
                .clear()
                .nodes(true)
                .metadata(true)
                .masterNodeTimeout(TimeValue.timeValueMillis(100))
                .waitForTimeout(TimeValue.timeValueMillis(100))
                .waitForMetadataVersion(waitForMetadataVersion);
            final ClusterStateResponse clusterStateResponse;
            try {
                clusterStateResponse = FutureUtils.get(client().admin().cluster().state(clusterStateRequest));
            } catch (MasterNotDiscoveredException e) {
                return; // ok, we hit the disconnected node
            }
            if (clusterStateResponse.isWaitForTimedOut() == false) {
                final ClusterState state = clusterStateResponse.getState();
                assertNotNull("should always contain a master node", state.nodes().getMasterNodeId());
                assertThat("waited for metadata version", state.metadata().version(), greaterThanOrEqualTo(waitForMetadataVersion));
            }
        });
    }

    public void testLocalRequestWaitsForMetadata() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(cluster().getNodeNames());
            final long metadataVersion
                = cluster().getInstance(ClusterService.class, node).getClusterApplierService().state().metadata().version();
            final long waitForMetadataVersion = randomLongBetween(Math.max(1, metadataVersion - 3), metadataVersion + 5);
            final ClusterStateResponse clusterStateResponse = FutureUtils.get(client(node).admin().cluster()
                .state(
                    new ClusterStateRequest()
                        .clear()
                        .local(true)
                        .metadata(true)
                        .waitForMetadataVersion(waitForMetadataVersion)
                        .masterNodeTimeout(TimeValue.timeValueMillis(100))
                        .waitForTimeout(TimeValue.timeValueMillis(100))
                ));
            if (clusterStateResponse.isWaitForTimedOut() == false) {
                final Metadata metadata = clusterStateResponse.getState().metadata();
                assertThat("waited for metadata version " + waitForMetadataVersion + " with node " + node,
                    metadata.version(), greaterThanOrEqualTo(waitForMetadataVersion));
            }
        });
    }

    public void runRepeatedlyWhileChangingMaster(Runnable runnable) throws Exception {
        cluster().startNodes(3);

        assertBusy(() -> {
            var request = new ClusterStateRequest().clear().metadata(true);
            var stateResponse = client().admin().cluster().state(request).get();
            var nodes = stateResponse
                .getState()
                .getLastCommittedConfiguration()
                .getNodeIds()
                .stream()
                .filter(n -> ClusterBootstrapService.isBootstrapPlaceholder(n) == false)
                .collect(Collectors.toSet());
            assertThat(nodes, hasSize(3));
        });

        final String masterName = cluster().getMasterName();

        final AtomicBoolean shutdown = new AtomicBoolean();
        final Thread assertingThread = new Thread(() -> {
            while (shutdown.get() == false) {
                runnable.run();
            }
        }, "asserting thread");

        final Thread updatingThread = new Thread(() -> {
            String value = "none";
            while (shutdown.get() == false) {
                value = "none".equals(value) ? "all" : "none";
                final String nonMasterNode = randomValueOtherThan(masterName, () -> randomFrom(cluster().getNodeNames()));

                var response = FutureUtils.get(client(nonMasterNode).admin().cluster().execute(
                    ClusterUpdateSettingsAction.INSTANCE,
                    new ClusterUpdateSettingsRequest().persistentSettings(Settings.builder()
                        .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), value)
                    )));
                assertAcked(response);
            }
        }, "updating thread");

        final List<MockTransportService> mockTransportServices
            = StreamSupport.stream(cluster().getInstances(TransportService.class).spliterator(), false)
            .map(ts -> (MockTransportService) ts).collect(Collectors.toList());

        assertingThread.start();
        updatingThread.start();

        final MockTransportService masterTransportService
            = (MockTransportService) cluster().getInstance(TransportService.class, masterName);

        for (MockTransportService mockTransportService : mockTransportServices) {
            if (masterTransportService != mockTransportService) {
                masterTransportService.addFailToSendNoConnectRule(mockTransportService);
                mockTransportService.addFailToSendNoConnectRule(masterTransportService);
            }
        }

        assertBusy(() -> {
            final String nonMasterNode = randomValueOtherThan(masterName, () -> randomFrom(cluster().getNodeNames()));
            final String claimedMasterName = cluster().getMasterName(nonMasterNode);
            assertThat(claimedMasterName).isNotEqualTo(masterName);
        });

        shutdown.set(true);
        assertingThread.join();
        updatingThread.join();
        cluster().close();
    }

}
