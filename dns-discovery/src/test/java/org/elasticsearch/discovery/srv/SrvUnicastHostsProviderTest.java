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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;
import org.xbill.DNS.*;

import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.*;

public class SrvUnicastHostsProviderTest {

    private TransportService transportService;

    abstract class DummySrvUnicastHostsProvider extends SrvUnicastHostsProvider {

        public DummySrvUnicastHostsProvider(Settings settings, TransportService transportService, Version version) {
            super(settings, transportService, version);
        }

        @Override
        protected Record[] lookupRecords() throws TextParseException {
            return null;
        }
    }

    @Before
    public void mockTransportService() throws Exception {
        transportService = mock(TransportService.class);
        when(transportService.addressesFromString(eq("crate1.internal:44300"))).thenReturn(new TransportAddress[]{
                new LocalTransportAddress("crate1.internal")
        });
        when(transportService.addressesFromString(eq("crate2.internal:44301"))).thenReturn(new TransportAddress[]{
                new LocalTransportAddress("crate2.internal")
        });
    }

    @Test
    public void testInvalidResolver() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER, "foobar.txt")
                .put(SrvUnicastHostsProvider.DISCOVERY_SRV_QUERY, "_crate._srv.foo.txt");
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(builder.build(),
                transportService, Version.CURRENT);
        assertNull(unicastHostsProvider.resolver);
        assertNull(unicastHostsProvider.lookupRecords());
    }

    @Test
    public void testValidResolver() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER, "8.8.4.4");
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(builder.build(),
                transportService, Version.CURRENT);
        assertEquals("/8.8.4.4:53", ((SimpleResolver)unicastHostsProvider.resolver).getAddress().toString());
    }

    @Test
    public void testBuildDynamicNodesNoQuery() throws Exception {
        // no query -> empty list of discovery nodes
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(ImmutableSettings.EMPTY,
                transportService, Version.CURRENT);
        List<DiscoveryNode> discoNodes = unicastHostsProvider.buildDynamicNodes();
        assertTrue(discoNodes.isEmpty());
    }

    @Test
    public void testBuildDynamicNoRecords() throws Exception {
        // no records -> empty list of discovery nodes
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(SrvUnicastHostsProvider.DISCOVERY_SRV_QUERY, "_crate._srv.crate.internal");
        SrvUnicastHostsProvider unicastHostsProvider = new DummySrvUnicastHostsProvider(builder.build(),
                transportService, Version.CURRENT) {
            @Override
            protected Record[] lookupRecords() throws TextParseException {
                return null;
            }
        };
        List<DiscoveryNode> discoNodes = unicastHostsProvider.buildDynamicNodes();
        assertTrue(discoNodes.isEmpty());
    }

    @Test
    public void testBuildDynamicNodes() throws Exception {
        // records
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(SrvUnicastHostsProvider.DISCOVERY_SRV_QUERY, "_crate._srv.crate.internal");
        SrvUnicastHostsProvider unicastHostsProvider = new DummySrvUnicastHostsProvider(builder.build(),
                transportService, Version.CURRENT) {
            @Override
            protected Record[] lookupRecords() throws TextParseException {
                Name srvName = Name.fromConstantString("_crate._srv.crate.internal.");
                return new Record[]{
                        new SRVRecord(srvName, DClass.IN, 3600, 10, 60, 44300, Name.fromConstantString("crate1.internal.")),
                        new SRVRecord(srvName, DClass.IN, 3600, 10, 60, 44301, Name.fromConstantString("crate2.internal."))
                };
            }
        };
        List<DiscoveryNode> discoNodes = unicastHostsProvider.buildDynamicNodes();
        assertEquals(2, discoNodes.size());
    }

    @Test
    public void testParseRecords() throws Exception {
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(ImmutableSettings.EMPTY,
                transportService, Version.CURRENT);

        Name srvName = Name.fromConstantString("_crate._srv.crate.internal.");
        Record[] records = new Record[]{
                new SRVRecord(srvName, DClass.IN, 3600, 10, 60, 44300, Name.fromConstantString("crate1.internal.")),
                new SRVRecord(srvName, DClass.IN, 3600, 10, 60, 44301, Name.fromConstantString("crate2.internal."))
        };
        List<DiscoveryNode> discoNodes = unicastHostsProvider.parseRecords(records);
        assertEquals("#srv-crate1.internal:44300", discoNodes.get(0).getId());
        assertEquals("#srv-crate2.internal:44301", discoNodes.get(1).getId());
    }
}