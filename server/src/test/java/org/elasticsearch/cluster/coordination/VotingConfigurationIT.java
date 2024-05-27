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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class VotingConfigurationIT extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testAbdicateAfterVotingConfigExclusionAdded() throws ExecutionException, InterruptedException {
        cluster().setBootstrapMasterNodeIndex(0);
        cluster().startNodes(2);
        final String originalMaster = cluster().getMasterName();

        logger.info("--> excluding master node {}", originalMaster);
        client().execute(AddVotingConfigExclusionsAction.INSTANCE,
            new AddVotingConfigExclusionsRequest(new String[]{originalMaster})).get();
        FutureUtils.get(client().admin().cluster().health(new ClusterHealthRequest().waitForEvents(Priority.LANGUID)));
        assertNotEquals(originalMaster, cluster().getMasterName());
    }

    public void testElectsNodeNotInVotingConfiguration() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);
        final List<String> nodeNames = cluster().startNodes(4);

        // a 4-node cluster settles on a 3-node configuration; we then prevent the nodes in the configuration from winning an election
        // by failing at the pre-voting stage, so that the extra node must be elected instead when the master shuts down. This extra node
        // should then add itself into the voting configuration.

        var clusterHealthResponse = FutureUtils.get(cluster().client().admin().cluster().health(
            new ClusterHealthRequest()
                .waitForNodes("4")
                .waitForEvents(Priority.LANGUID)
            ));
        assertThat(clusterHealthResponse.isTimedOut()).isFalse();

        String excludedNodeName = null;
        final ClusterState clusterState = cluster().client().admin().cluster().state(
            new ClusterStateRequest()
                .clear()
                .nodes(true)
                .metadata(true)
            ).get().getState();
        final Set<String> votingConfiguration = clusterState.getLastCommittedConfiguration().getNodeIds();
        assertThat(votingConfiguration).hasSize(3);
        assertThat(clusterState.nodes().getSize()).isEqualTo(4);
        assertThat(votingConfiguration, hasItem(clusterState.nodes().getMasterNodeId()));
        for (DiscoveryNode discoveryNode : clusterState.nodes()) {
            if (votingConfiguration.contains(discoveryNode.getId()) == false) {
                assertThat(excludedNodeName).isNull();
                excludedNodeName = discoveryNode.getName();
            }
        }

        for (final String sender : nodeNames) {
            if (sender.equals(excludedNodeName)) {
                continue;
            }
            final MockTransportService senderTransportService
                = (MockTransportService) cluster().getInstance(TransportService.class, sender);
            for (final String receiver : nodeNames) {
                senderTransportService.addSendBehavior(cluster().getInstance(TransportService.class, receiver),
                    (connection, requestId, action, request, options) -> {
                        if (action.equals(PreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)) {
                            throw new ElasticsearchException("rejected");
                        }
                        connection.sendRequest(requestId, action, request, options);
                    });
            }
        }

        cluster().stopCurrentMasterNode();
        clusterHealthResponse = FutureUtils.get(cluster().client().admin().cluster().health(
            new ClusterHealthRequest()
                .waitForNodes("3")
                .waitForEvents(Priority.LANGUID)
            ));
        assertThat(clusterHealthResponse.isTimedOut()).isFalse();

        final ClusterState newClusterState = cluster().client().admin().cluster().state(
            new ClusterStateRequest()
                .clear()
                .nodes(true)
                .metadata(true)
            ).get().getState();
        assertThat(newClusterState.nodes().getMasterNode().getName()).isEqualTo(excludedNodeName);
        assertThat(newClusterState.getLastCommittedConfiguration().getNodeIds(), hasItem(newClusterState.nodes().getMasterNodeId()));
    }
}
