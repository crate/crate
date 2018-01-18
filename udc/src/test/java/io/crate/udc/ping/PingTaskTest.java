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

import io.crate.ClusterIdService;
import io.crate.http.HttpTestServer;
import io.crate.monitor.ExtendedNetworkInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.monitor.ExtendedOsInfo;
import io.crate.monitor.SysInfo;
import io.crate.settings.SharedSettings;
import io.crate.test.integration.CrateUnitTest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PingTaskTest extends CrateUnitTest {

    private ClusterService clusterService;
    private ClusterIdService clusterIdService;
    private ExtendedNodeInfo extendedNodeInfo;
    private ClusterSettings clusterSettings = new ClusterSettings(
        Settings.EMPTY,
        Sets.newHashSet(SharedSettings.LICENSE_IDENT_SETTING.setting()));

    private HttpTestServer testServer;

    private PingTask createPingTask(String pingUrl, Settings settings) {
        return new PingTask(
            clusterService,
            clusterIdService,
            extendedNodeInfo,
            pingUrl,
            clusterSettings,
            settings
        );
    }

    private PingTask createPingTask() {
        return createPingTask("http://dummy", Settings.EMPTY);
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
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);

        CompletableFuture<String> clusterIdFuture = CompletableFuture.completedFuture(UUID.randomUUID().toString());
        when(clusterIdService.clusterId()).thenReturn(clusterIdFuture);
        when(clusterService.localNode()).thenReturn(discoveryNode);
        when(discoveryNode.isMasterNode()).thenReturn(true);

        extendedNodeInfo = mock(ExtendedNodeInfo.class);
        when(extendedNodeInfo.networkInfo()).thenReturn(new ExtendedNetworkInfo(ExtendedNetworkInfo.NA_INTERFACE));
        when(extendedNodeInfo.osInfo()).thenReturn(new ExtendedOsInfo(SysInfo.gather()));
    }

    @Test
    public void testGetHardwareAddressMacAddrNull() throws Exception {
        PingTask pingTask = createPingTask();
        assertThat(pingTask.getHardwareAddress(), Matchers.nullValue());
    }

    @Test
    public void testIsEnterpriseField() throws Exception {
        PingTask pingTask = createPingTask();
        assertThat(pingTask.isEnterprise(), is(SharedSettings.ENTERPRISE_LICENSE_SETTING.getDefault().toString()));

        pingTask = createPingTask(
            "http://dummy",
            Settings.builder().put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), false).build()
        );
        assertThat(pingTask.isEnterprise(), is("false"));
    }

    @Test
    public void testLicenseIdent() throws Exception {
        PingTask pingTask = createPingTask();
        // If the setting is not set an empty string is sent
        assertThat(pingTask.getLicenseIdent(), is(""));
        pingTask = createPingTask(
            "http://dummy",
            Settings.builder().put(SharedSettings.LICENSE_IDENT_SETTING.getKey(), "my-license-ident").build()
        );
        assertThat(pingTask.getLicenseIdent(), is("my-license-ident"));
    }

    @Test
    public void testLicenseIdentUpdate() throws Exception {
        PingTask pingTask = createPingTask();
        // change license ident
        Settings newSettings = Settings.builder().put("license.ident", "new license").build();
        clusterSettings.applySettings(newSettings);
        assertThat(pingTask.getLicenseIdent(), is("new license"));
    }

    @Test
    public void testSuccessfulPingTaskRun() throws Exception {
        testServer = new HttpTestServer(18080, false);
        testServer.run();

        PingTask task = createPingTask("http://localhost:18080/", Settings.EMPTY);
        task.run();
        assertThat(testServer.responses.size(), is(1));
        task.run();
        assertThat(testServer.responses.size(), is(2));
        for (long i = 0; i < testServer.responses.size(); i++) {
            String json = testServer.responses.get((int) i);
            Map<String, String> map = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            try {
                //convert JSON string to Map
                map = mapper.readValue(json,
                    new TypeReference<HashMap<String, String>>() {});

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
            pingCountMap = mapper.readValue(map.get("ping_count"), new TypeReference<Map<String, Long>>() {});

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
            assertThat(map.get("license_ident"), is(""));
        }
    }

    @Test
    public void testUnsuccessfulPingTaskRun() throws Exception {
        testServer = new HttpTestServer(18081, true);
        testServer.run();
        PingTask task = createPingTask("http://localhost:18081/", Settings.EMPTY);
        task.run();
        assertThat(testServer.responses.size(), is(1));
        task.run();
        assertThat(testServer.responses.size(), is(2));

        for (long i = 0; i < testServer.responses.size(); i++) {
            String json = testServer.responses.get((int) i);
            Map<String, String> map = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            try {
                //convert JSON string to Map
                map = mapper.readValue(json,
                    new TypeReference<HashMap<String, String>>() {});

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
            pingCountMap = mapper.readValue(map.get("ping_count"), new TypeReference<Map<String, Long>>() {});

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
