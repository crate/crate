/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import io.crate.session.Session;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SysNodeResiliencyIntegrationTest extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> nodePlugins = new ArrayList<>(super.nodePlugins());
        nodePlugins.add(MockTransportService.TestPlugin.class);
        return nodePlugins;
    }

    /**
     * Test that basic information from cluster state is used if a sys node
     * request is timing out
     */
    @Test
    public void testTimingOutNode() throws Exception {
        // wait until no master cluster state tasks are pending, otherwise this test may fail due to master task timeouts
        waitNoPendingTasksOnAll();

        String[] nodeNames = cluster().getNodeNames();
        String n1 = nodeNames[0];
        String n2 = nodeNames[1];

        NetworkDisruption partition = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(n1, n2), new NetworkDisruption.NetworkUnresponsive());
        setDisruptionScheme(partition);
        partition.startDisrupting();
        try {

            try (Session session = createSessionOnNode(n1)) {
                execute(
                    "select version['number'], hostname, id, name from sys.nodes where name = ?",
                    new Object[]{n2},
                    session
                );
            }
            assertThat(response.rowCount()).isEqualTo(1L);
            assertThat(response.rows()[0][0]).isNull();
            assertThat(response.rows()[0][1]).isNull();
            assertThat(response.rows()[0][2]).isNotNull();
            assertThat(response.rows()[0][3]).isEqualTo(n2);
        } finally {
            partition.stopDisrupting();
            cluster().clearDisruptionScheme(true);
            waitNoPendingTasksOnAll();
        }
    }
}
