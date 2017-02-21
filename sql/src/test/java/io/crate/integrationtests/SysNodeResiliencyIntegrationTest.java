/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2, transportClientRatio = 0)
public class SysNodeResiliencyIntegrationTest extends SQLTransportIntegrationTest {

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
        String[] nodeNames = internalCluster().getNodeNames();
        String n1 = nodeNames[0];
        String n2 = nodeNames[1];

        NetworkDisruption partition = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(n1, n2), new NetworkDisruption.NetworkUnresponsive());
        setDisruptionScheme(partition);
        partition.startDisrupting();

        execute("select version, hostname, id, name from sys.nodes where name = ?",
                 new Object[]{n2},
                 createSessionOnNode(n1));

        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(nullValue()));
        assertThat(response.rows()[0][1], is(nullValue()));
        assertThat(response.rows()[0][2], is(notNullValue()));
        assertThat(response.rows()[0][3], is(n2));
    }
}
