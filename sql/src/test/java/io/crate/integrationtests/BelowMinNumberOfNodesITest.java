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

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, supportsDedicatedMasters = false, autoMinMasterNodes = false, numClientNodes = 0)
public class BelowMinNumberOfNodesITest extends SQLTransportIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("http.enabled", true)
            .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2)
            .build();
    }

    @Test
    public void testSysQueriesAreResponsiveIfBelowMinimumMasterNodes() throws IOException {
        execute("create table t1 (x int) with (number_of_replicas = 1) ");

        internalCluster().stopRandomNonMasterNode();

        try {
            // we only test that this does not throw an error
            execute("select count(*) from sys.checks");

            Object[][] rows = execute("select port['http'] from sys.nodes order by 1").rows();
            assertThat(rows[0][0], notNullValue());
            assertThat(rows[1][0], nullValue());

            assertThat(getRestStatus(), is(RestStatus.SERVICE_UNAVAILABLE.getStatus()));
        } finally {
            // satisfy min_master_nodes again; otherwise the teardown blocks
            internalCluster().startNode();
        }
    }

    private int getRestStatus() throws IOException {
        HttpServerTransport httpTransport = internalCluster().getInstance(HttpServerTransport.class);
        InetSocketAddress address = httpTransport.boundAddress().publishAddress().address();
        HttpGet httpGet = new HttpGet("http://" + address.getHostName() + ":" + address.getPort() + "/");
        return HttpClients.createDefault().execute(httpGet).getStatusLine().getStatusCode();
    }
}
