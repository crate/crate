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

import io.crate.http.HttpTestServer;
import io.crate.license.LicenseData;
import io.crate.license.LicenseService;
import io.crate.monitor.ExtendedNetworkInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.monitor.ExtendedOsInfo;
import io.crate.monitor.SysInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PingTaskTest extends CrateDummyClusterServiceUnitTest {

    static private LicenseData LICENSE = new LicenseData(
        System.currentTimeMillis() + TimeUnit.DAYS.toMillis(30),
        "crate" ,
        3
        );

    private ExtendedNodeInfo extendedNodeInfo;
    private LicenseService licenseService;

    private HttpTestServer testServer;

    private PingTask createPingTask(String pingUrl, Settings settings) {
        return new PingTask(
            clusterService,
            extendedNodeInfo,
            pingUrl,
            settings,
            licenseService
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
        extendedNodeInfo = mock(ExtendedNodeInfo.class);
        when(extendedNodeInfo.networkInfo()).thenReturn(new ExtendedNetworkInfo(ExtendedNetworkInfo.NA_INTERFACE));
        when(extendedNodeInfo.osInfo()).thenReturn(new ExtendedOsInfo(SysInfo.gather()));
        licenseService = mock(LicenseService.class);
    }

    @Test
    public void testGetHardwareAddressMacAddrNull() throws Exception {
        PingTask pingTask = createPingTask();
        assertThat(pingTask.getHardwareAddress(), Matchers.nullValue());
    }

    @Test
    public void testSuccessfulPingTaskRunWhenLicenseIsNotNull() throws Exception {
        when(licenseService.currentLicense()).thenReturn(LICENSE);

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
            assertThat(map, hasKey("license_expiry_date"));
            assertThat(map.get("license_expiry_date"), is(String.valueOf(LICENSE.expiryDateInMs())));
            assertThat(map, hasKey("license_issued_to"));
            assertThat(map.get("license_issued_to"), is(LICENSE.issuedTo()));
            assertThat(Integer.parseInt(map.get("num_processors")), greaterThan(0));
            assertThat(map, hasKey("license_max_nodes"));
            assertThat(map.get("license_max_nodes"), is(String.valueOf(LICENSE.maxNumberOfNodes())));
            assertThat(map, hasKey("enterprise_edition"));
            assertThat(map.get("enterprise_edition"), is(String.valueOf(true)));
        }
    }

    @Test
    public void testSuccessfulPingTaskRunWhenLicenseIsNull() throws Exception {
        when(licenseService.currentLicense()).thenReturn(null);

        testServer = new HttpTestServer(18080, false);
        testServer.run();

        PingTask task = createPingTask("http://localhost:18080/", Settings.EMPTY);
        task.run();
        assertThat(testServer.responses.size(), is(1));
        String json = testServer.responses.get(0);
        Map<String, String> map = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            //convert JSON string to Map
            map = mapper.readValue(json,
                new TypeReference<HashMap<String, String>>() {});

        } catch (Exception e) {
            e.printStackTrace();
        }

        assertThat(map, not(hasKey("license_expiry_date")));
        assertThat(map, not(hasKey("license_issued_to")));
        assertThat(map, not(hasKey("license_max_nodes")));
        assertThat(map, hasKey("enterprise_edition"));
        assertThat(map.get("enterprise_edition"), is(String.valueOf(false)));
    }

    @Test
    public void testUnsuccessfulPingTaskRun() throws Exception {
        when(licenseService.currentLicense()).thenReturn(LICENSE);

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
            assertThat(map, hasKey("license_expiry_date"));
            assertThat(map.get("license_expiry_date"), is(notNullValue()));
            assertThat(map, hasKey("license_issued_to"));
            assertThat(map.get("license_issued_to"), is(notNullValue()));
            assertThat(map, hasKey("license_max_nodes"));
            assertThat(map.get("license_max_nodes"), is(notNullValue()));
        }
    }
}
