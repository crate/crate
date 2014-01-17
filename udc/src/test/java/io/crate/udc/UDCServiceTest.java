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

package io.crate.udc;

import io.crate.http.HttpTestServer;
import io.crate.udc.plugin.UDCPlugin;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.TEST, numNodes = 0)
public class UDCServiceTest extends CrateIntegrationTest {

    @Test
    public void testUDCService() throws Exception {
        HttpTestServer httpTestServer = new HttpTestServer(18080);
        httpTestServer.run();

        Settings settings = settingsBuilder()
                .put(UDCPlugin.ENABLED_SETTING_NAME, true)
                .put(UDCPlugin.URL_SETTING_NAME, "http://localhost:18080/")
                .put(UDCPlugin.INITIAL_DELAY_SETTING_NAME, new TimeValue(4, TimeUnit.SECONDS))
                .put(UDCPlugin.INTERVAL_SETTING_NAME, new TimeValue(1, TimeUnit.SECONDS))
                .build();
        cluster().startNode(settings);
        ensureGreen();

        sleep(1000);
        assertThat(httpTestServer.responses.size(), greaterThanOrEqualTo(1));

        sleep(1000);
        assertThat(httpTestServer.responses.size(), greaterThanOrEqualTo(2));

        // validate content of 1st response
        String json = httpTestServer.responses.get(0);
        Map<String,String> map = new HashMap<String,String>();
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
        assertTrue(map.containsKey("hardware_address"));
        assertNotNull(map.get("hardware_address"));
        assertTrue(map.containsKey("crate_version"));
        assertNotNull(map.get("crate_version"));
        assertTrue(map.containsKey("java_version"));
        assertNotNull(map.get("java_version"));


        // clean system properties to make test suite happy
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
    }
}
