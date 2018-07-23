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

package io.crate.discovery;

import io.crate.plugin.SrvPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xbill.DNS.Cache;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Name;
import org.xbill.DNS.SetResponse;
import org.xbill.DNS.Zone;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.settings.Settings.builder;

@ESIntegTestCase.ClusterScope(numClientNodes = 0, numDataNodes = 0, transportClientRatio = 0)
public class SrvDiscoveryIntegrationTest extends ESIntegTestCase {

    @Before
    public void prepare() throws Exception {
        Lookup.setDefaultCache(new MockedZoneCache("crate.internal."), DClass.IN);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(SrvPlugin.class);
    }

    @After
    public void clearDNSCache() throws Exception {
        Lookup.setDefaultCache(new Cache(), DClass.IN);
    }

    @Test
    public void testClusterSrvDiscovery() throws Exception {
        Settings localSettings = builder()
            .put("discovery.zen.hosts_provider", "srv")
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_QUERY.getKey(), "_test._srv.crate.internal.")
            .build();
        internalCluster().startNode(localSettings);
        internalCluster().startNode(localSettings);
        internalCluster().startNode(localSettings);
        internalCluster().ensureAtLeastNumDataNodes(3);
        internalCluster().ensureAtMostNumDataNodes(3);
        assertEquals(3, internalCluster().size());

        internalCluster().stopCurrentMasterNode();
        internalCluster().ensureAtLeastNumDataNodes(2);
        internalCluster().ensureAtMostNumDataNodes(2);
        assertEquals(2, internalCluster().size());

        internalCluster().stopRandomNonMasterNode();
        internalCluster().ensureAtLeastNumDataNodes(1);
        internalCluster().ensureAtMostNumDataNodes(1);
        assertEquals(1, internalCluster().size());
    }

    private Zone loadZone(String zoneName) throws IOException {
        String zoneFilename = zoneName + "zone";
        URL zoneResource = getClass().getResource(zoneFilename);
        assertNotNull("test resource for zone could not be loaded: " + zoneFilename, zoneResource);
        String zoneFile = zoneResource.getFile();
        return new Zone(Name.fromString(zoneName), zoneFile);
    }

    private final class MockedZoneCache extends Cache {

        Zone zone = null;

        private MockedZoneCache(String string) throws IOException {
            zone = loadZone(string);
        }

        @Override
        public SetResponse lookupRecords(Name arg0, int arg1, int arg2) {
            return zone.findRecords(arg0, arg1);
        }
    }
}
