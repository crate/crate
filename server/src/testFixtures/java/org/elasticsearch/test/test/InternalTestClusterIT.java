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
package org.elasticsearch.test.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.elasticsearch.test.TestCluster;
import org.junit.Test;

@ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class InternalTestClusterIT extends IntegTestCase {

    @Test
    public void testStartingAndStoppingNodes() throws IOException {
        logger.info("--> cluster has [{}] nodes", cluster().size());
        if (cluster().size() < 5) {
            final int nodesToStart = randomIntBetween(Math.max(2, cluster().size() + 1), 5);
            logger.info("--> growing to [{}] nodes", nodesToStart);
            cluster().startNodes(nodesToStart);
        }
        ensureGreen();

        while (cluster().size() > 1) {
            final int nodesToRemain = randomIntBetween(1, cluster().size() - 1);
            logger.info("--> reducing to [{}] nodes", nodesToRemain);
            cluster().ensureAtMostNumDataNodes(nodesToRemain);
            assertThat(cluster().size()).isLessThanOrEqualTo(nodesToRemain);
        }

        ensureGreen();
    }

    public void testStoppingNodesOneByOne() throws IOException {
        // In a 5+ node cluster there must be at least one reconfiguration as the nodes are shut down one-by-one before we drop to 2 nodes.
        // If the nodes shut down too quickly then this reconfiguration does not have time to occur and the quorum is lost in the 3->2
        // transition, even though in a stable cluster the 3->2 transition requires no special treatment.

        cluster().startNodes(5);
        ensureGreen();

        while (cluster().size() > 1) {
            cluster().stopRandomNode(s -> true);
        }

        ensureGreen();
    }

    public void testOperationsDuringRestart() throws Exception {
        cluster().startMasterOnlyNode();
        cluster().startDataOnlyNodes(2);
        cluster().restartRandomDataNode(new TestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureGreen();
                cluster().validateClusterFormed();
                assertThat(cluster().getInstance(NodeClient.class)).isNotNull();
                cluster().restartRandomDataNode(new TestCluster.RestartCallback() {
                    @Override
                    public Settings onNodeStopped(String nodeName) throws Exception {
                        ensureGreen();
                        cluster().validateClusterFormed();
                        return super.onNodeStopped(nodeName);
                    }
                });
                return super.onNodeStopped(nodeName);
            }
        });
    }
}
