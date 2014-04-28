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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PingTaskTest {

    private ClusterService clusterService;
    private HttpServerTransport httpServerTransport;
    private ClusterIdService clusterIdService;

    @Before
    public void setUp() throws Exception {
        clusterService = mock(ClusterService.class);
        clusterIdService = mock(ClusterIdService.class);
        httpServerTransport = mock(HttpServerTransport.class);
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
        BoundTransportAddress boundAddress = mock(BoundTransportAddress.class);
        TransportAddress transportAddress = new InetSocketTransportAddress(
                InetAddress.getLocalHost().getHostName(), 4200);

        SettableFuture<ClusterId> clusterIdFuture = SettableFuture.create();
        clusterIdFuture.set(new ClusterId(UUID.randomUUID()));
        when(clusterIdService.clusterId()).thenReturn(clusterIdFuture);
        when(clusterService.localNode()).thenReturn(discoveryNode);
        when(discoveryNode.isMasterNode()).thenReturn(true);
        when(httpServerTransport.boundAddress()).thenReturn(boundAddress);
        when(boundAddress.publishAddress()).thenReturn(transportAddress);
    }

    @Test
    public void testSuccessfulPingTaskRun() throws Exception {
        HttpTestServer testServer = new HttpTestServer(18080, false);
        testServer.run();

        PingTask task = new PingTask(
                clusterService, clusterIdService, httpServerTransport, "http://localhost:18080/");
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

            assertTrue(map.containsKey("kernel"));
            assertNotNull(map.get("kernel"));
            assertTrue(map.containsKey("cluster_id"));
            assertNotNull(map.get("cluster_id"));
            assertTrue(map.containsKey("master"));
            assertNotNull(map.get("master"));
            assertTrue(map.containsKey("ping_count"));
            assertNotNull(map.get("ping_count"));
            Map<String, Long> pingCountMap;
            pingCountMap = mapper.readValue(map.get("ping_count"), new TypeReference<Map<String, Long>>(){});

            assertThat(pingCountMap.get("success"), is(i));
            assertThat(pingCountMap.get("failure"), is(0L));

            assertTrue(map.containsKey("hardware_address"));
            assertNotNull(map.get("hardware_address"));
            assertTrue(map.containsKey("crate_version"));
            assertNotNull(map.get("crate_version"));
            assertTrue(map.containsKey("java_version"));
            assertNotNull(map.get("java_version"));
        }
        testServer.shutDown();
    }

    @Test
    public void testUnsuccessfulPingTaskRun() throws Exception {
        HttpTestServer testServer = new HttpTestServer(18081, true);
        testServer.run();
        PingTask task = new PingTask(
                clusterService, clusterIdService, httpServerTransport, "http://localhost:18081/");
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

            assertTrue(map.containsKey("kernel"));
            assertNotNull(map.get("kernel"));
            assertTrue(map.containsKey("cluster_id"));
            assertNotNull(map.get("cluster_id"));
            assertTrue(map.containsKey("master"));
            assertNotNull(map.get("master"));
            assertTrue(map.containsKey("ping_count"));
            assertNotNull(map.get("ping_count"));
            Map<String, Long> pingCountMap;
            pingCountMap = mapper.readValue(map.get("ping_count"), new TypeReference<Map<String, Long>>(){});

            assertThat(pingCountMap.get("success"), is(0L));
            assertThat(pingCountMap.get("failure"), is(i));

            assertTrue(map.containsKey("hardware_address"));
            assertNotNull(map.get("hardware_address"));
            assertTrue(map.containsKey("crate_version"));
            assertNotNull(map.get("crate_version"));
            assertTrue(map.containsKey("java_version"));
            assertNotNull(map.get("java_version"));
        }

        testServer.shutDown();
    }
}
