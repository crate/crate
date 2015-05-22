/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.discovery.srv;

import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;
import org.xbill.DNS.*;

import java.io.IOException;
import java.net.URL;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.TEST, numNodes = 0)
public class SrvDiscoveryIntegrationTest extends CrateIntegrationTest {

    @Before
    public void prepare() throws Exception {
        Lookup.setDefaultCache(new MockedZoneCache("crate.internal."), DClass.IN);
    }

    @Test
    public void testClusterSrvDiscovery() throws Exception {
        Settings localSettings = settingsBuilder()
                .put("node.mode", "network")
                .put("discovery.zen.ping.multicast.enabled", "false")
                .put("discovery.type", "srv")
                .put(SrvUnicastHostsProvider.DISCOVERY_SRV_QUERY, "_test._srv.crate.internal.")
                .build();
        cluster().startNode(localSettings);
        cluster().startNode(localSettings);
        cluster().startNode(localSettings);
        cluster().ensureAtLeastNumNodes(3);
        assertEquals(3, cluster().size());

        cluster().stopCurrentMasterNode();
        cluster().ensureAtLeastNumNodes(2);
        assertEquals(2, cluster().size());

        cluster().stopRandomNode();
        cluster().ensureAtLeastNumNodes(1);
        assertEquals(1, cluster().size());
    }

    private Zone loadZone(String zoneName) throws IOException {
        String zoneFilename = zoneName + "zone";
        URL zoneResource = getClass().getResource(zoneFilename);
        assertNotNull("test resource for zone could not be loaded: " + zoneFilename, zoneResource);
        String zoneFile = zoneResource.getFile();
        return new Zone(Name.fromString(zoneName),zoneFile);
    }

    private final class MockedZoneCache extends Cache {

        Zone zone = null;

        public MockedZoneCache(String string) throws IOException {
            zone = loadZone(string);
        }

        public SetResponse lookupRecords(Name arg0, int arg1, int arg2) {
            return zone.findRecords(arg0, arg1);
        }
    }

}
