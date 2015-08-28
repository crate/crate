/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.udc.ping;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.ClusterId;
import io.crate.ClusterIdService;
import io.crate.http.HttpTestServer;
import io.crate.test.integration.CrateUnitTest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.monitor.network.NetworkInfo;
import org.elasticsearch.node.service.NodeService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PingTaskTest extends CrateUnitTest {

    private ClusterService clusterService;
    private NodeService nodeService;
    private ClusterIdService clusterIdService;

    private HttpTestServer testServer;

    private String macAddr() {
        InetAddress ip;
        try {
            ip = InetAddress.getLocalHost();
            NetworkInterface networkInterface = NetworkInterface.getByInetAddress(ip);
            if (networkInterface.getName().equals("lo"))
                return "loopback";
            byte[] mac = networkInterface.getHardwareAddress();
            StringBuilder sb = new StringBuilder(18);
            for (byte b : mac) {
                if (sb.length() > 0)
                    sb.append(':');
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return null;
        }
    }

    @After
    public void cleanUp() {
        if (testServer != null) {
            testServer.shutDown();
        }
    }

    @Before
    public void prepare() throws Exception {
        clusterService = mock(ClusterService.class);
        clusterIdService = mock(ClusterIdService.class);
        nodeService = mock(NodeService.class);
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);

        SettableFuture<ClusterId> clusterIdFuture = SettableFuture.create();
        clusterIdFuture.set(new ClusterId());
        when(clusterIdService.clusterId()).thenReturn(clusterIdFuture);
        when(clusterService.localNode()).thenReturn(discoveryNode);
        when(discoveryNode.isMasterNode()).thenReturn(true);
        NodeInfo nodeInfo = mock(NodeInfo.class);
        NetworkInfo networkInfo = mock(NetworkInfo.class);
        NetworkInfo.Interface iface = mock(NetworkInfo.Interface.class);
        when(iface.getMacAddress()).thenReturn(macAddr());
        when(networkInfo.getPrimaryInterface()).thenReturn(iface);
        when(nodeInfo.getNetwork()).thenReturn(networkInfo);
        when(nodeService.info()).thenReturn(nodeInfo);
    }

    @Test
    public void testSuccessfulPingTaskRun() throws Exception {
        testServer = new HttpTestServer(18080, false);
        testServer.run();

        PingTask task = new PingTask(
                clusterService, clusterIdService, nodeService, "http://localhost:18080/");
        task.run();
        assertThat(testServer.responses.size(), is(1));
        task.run();
        assertThat(testServer.responses.size(), is(2));
        for (long i=0; i< testServer.responses.size(); i++) {
            String json = testServer.responses.get((int)i);
            Map<String,String> map = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            try {
                //convert JSON string to Map
                map = mapper.readValue(json,
                        new TypeReference<HashMap<String,String>>(){});

            } catch (Exception e) {
                e.printStackTrace();
            }

            assertThat(map, hasKey("kernel"));
            assertThat(map.get("kernel"), is(notNullValue()));
            assertThat(map, hasKey("cluster_id"));
            assertThat(map.get("cluster_id"), is(notNullValue()));
            assertThat(map, hasKey("master"));
            assertThat(map.get("master"), is(notNullValue()));
            assertThat(map, hasKey("ping_count"));
            assertThat(map.get("ping_count"), is(notNullValue()));
            Map<String, Long> pingCountMap;
            pingCountMap = mapper.readValue(map.get("ping_count"), new TypeReference<Map<String, Long>>(){});

            assertThat(pingCountMap.get("success"), is(i));
            assertThat(pingCountMap.get("failure"), is(0L));
            if (task.getHardwareAddress() != null) {
                assertThat(map, hasKey("hardware_address"));
                assertThat(map.get("hardware_address"), is(notNullValue()));
            }
            assertThat(map, hasKey("crate_version"));
            assertThat(map.get("crate_version"), is(notNullValue()));
            assertThat(map, hasKey("java_version"));
            assertThat(map.get("java_version"), is(notNullValue()));
        }
    }

    @Test
    public void testUnsuccessfulPingTaskRun() throws Exception {
        testServer = new HttpTestServer(18081, true);
        testServer.run();
        PingTask task = new PingTask(
                clusterService, clusterIdService, nodeService, "http://localhost:18081/");
        task.run();
        assertThat(testServer.responses.size(), is(1));
        task.run();
        assertThat(testServer.responses.size(), is(2));

        for (long i=0; i< testServer.responses.size(); i++) {
            String json = testServer.responses.get((int)i);
            Map<String,String> map = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            try {
                //convert JSON string to Map
                map = mapper.readValue(json,
                        new TypeReference<HashMap<String,String>>(){});

            } catch (Exception e) {
                e.printStackTrace();
            }

            assertThat(map, hasKey("kernel"));
            assertThat(map.get("kernel"), is(notNullValue()));
            assertThat(map, hasKey("cluster_id"));
            assertThat(map.get("cluster_id"), is(notNullValue()));
            assertThat(map, hasKey("master"));
            assertThat(map.get("master"), is(notNullValue()));
            assertThat(map, hasKey("ping_count"));
            assertThat(map.get("ping_count"), is(notNullValue()));
            Map<String, Long> pingCountMap;
            pingCountMap = mapper.readValue(map.get("ping_count"), new TypeReference<Map<String, Long>>(){});

            assertThat(pingCountMap.get("success"), is(0L));
            assertThat(pingCountMap.get("failure"), is(i));

            if (task.getHardwareAddress() != null) {
                assertThat(map, hasKey("hardware_address"));
                assertThat(map.get("hardware_address"), is(notNullValue()));
            }
            assertThat(map, hasKey("crate_version"));
            assertThat(map.get("crate_version"), is(notNullValue()));
            assertThat(map, hasKey("java_version"));
            assertThat(map.get("java_version"), is(notNullValue()));
        }
    }
}
