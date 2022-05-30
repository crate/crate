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

import static org.apache.lucene.tests.util.LuceneTestCase.random;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;

public class CoordinationStateTestCluster {

    public static ClusterState clusterState(long term, long version, DiscoveryNode localNode,
                                            CoordinationMetadata.VotingConfiguration lastCommittedConfig,
                                            CoordinationMetadata.VotingConfiguration lastAcceptedConfig, long value) {
        return clusterState(term, version, DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build(),
            lastCommittedConfig, lastAcceptedConfig, value);
    }

    public static ClusterState clusterState(long term, long version, DiscoveryNodes discoveryNodes,
                                            CoordinationMetadata.VotingConfiguration lastCommittedConfig,
                                            CoordinationMetadata.VotingConfiguration lastAcceptedConfig, long value) {
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
}
