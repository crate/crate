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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Name;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.TextParseException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SrvUnicastHostsProviderTest {

    private TransportService transportService;
    private ThreadPool threadPool;

    private abstract class DummySrvUnicastHostsProvider extends SrvUnicastHostsProvider {

        private DummySrvUnicastHostsProvider(Settings settings, TransportService transportService) {
            super(settings, transportService);
        }

        @Override
        protected Record[] lookupRecords() throws TextParseException {
            return null;
        }
    }

    @Before
    public void mockTransportService() throws Exception {
        threadPool = new TestThreadPool("dummy", Settings.EMPTY);
        MockTcpTransport transport = new MockTcpTransport(
            Settings.EMPTY,
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(),
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
            new NetworkService(Collections.emptyList())
        );
        transportService = new TransportService(
            Settings.EMPTY,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> new DiscoveryNode(
                "dummy",
                UUIDs.randomBase64UUID(),
                boundAddress.publishAddress(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Version.CURRENT
            ),
            null
        ) {
            @Override
            public TransportAddress[] addressesFromString(String address, int perAddressLimit) {
                int num = Integer.parseInt(address.substring("crate".length(), address.indexOf(".")));
                byte[] dummyIp = new byte[] { 0, 0, 0, 0 };
                try {
                    return new TransportAddress[] {
                        new TransportAddress(InetAddress.getByAddress("crate" + num + ".internal", dummyIp), 44300)
                    };
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @After
    public void releaseThreadPool() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    public void testInvalidResolver() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER.getKey(), "foobar.txt")
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_QUERY.getKey(), "_crate._srv.foo.txt");
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(builder.build(), transportService);
        assertNull(unicastHostsProvider.resolver);
        assertNull(unicastHostsProvider.lookupRecords());
    }

    @Test
    public void testValidResolver() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER.getKey(), "8.8.4.4");
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(builder.build(), transportService);
        assertEquals("/8.8.4.4:53", ((SimpleResolver) unicastHostsProvider.resolver).getAddress().toString());
    }

    @Test
    public void testValidResolverWithPort() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER.getKey(), "127.0.0.1:5353");
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(builder.build(), transportService);
        assertEquals("/127.0.0.1:5353", ((SimpleResolver) unicastHostsProvider.resolver).getAddress().toString());
    }

    @Test
    public void testValidResolverWithInvalidPort() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_RESOLVER.getKey(), "127.0.0.1:42a");
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(builder.build(), transportService);
        assertEquals("/127.0.0.1:53", ((SimpleResolver) unicastHostsProvider.resolver).getAddress().toString());
    }

    @Test
    public void testBuildDynamicNodesNoQuery() throws Exception {
        // no query -> empty list of discovery nodes
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(Settings.EMPTY, transportService);
        List<DiscoveryNode> discoNodes = unicastHostsProvider.buildDynamicNodes();
        assertTrue(discoNodes.isEmpty());
    }

    @Test
    public void testBuildDynamicNoRecords() throws Exception {
        // no records -> empty list of discovery nodes
        Settings.Builder builder = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_QUERY.getKey(), "_crate._srv.crate.internal");
        SrvUnicastHostsProvider unicastHostsProvider = new DummySrvUnicastHostsProvider(builder.build(), transportService) {
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
        Settings.Builder builder = Settings.builder()
            .put(SrvUnicastHostsProvider.DISCOVERY_SRV_QUERY.getKey(), "_crate._srv.crate.internal");
        SrvUnicastHostsProvider unicastHostsProvider = new DummySrvUnicastHostsProvider(builder.build(), transportService) {
            @Override
            protected Record[] lookupRecords() throws TextParseException {
                Name srvName = Name.fromConstantString("_crate._srv.crate.internal.");
                return new Record[]{
                    new SRVRecord(srvName, DClass.IN, 3600, 1, 10, 44300, Name.fromConstantString("crate1.internal.")),
                    new SRVRecord(srvName, DClass.IN, 3600, 1, 20, 44300, Name.fromConstantString("crate2.internal."))
                };
            }
        };
        List<DiscoveryNode> discoNodes = unicastHostsProvider.buildDynamicNodes();
        assertEquals(2, discoNodes.size());
    }

    @Test
    public void testParseRecords() throws Exception {
        SrvUnicastHostsProvider unicastHostsProvider = new SrvUnicastHostsProvider(Settings.EMPTY, transportService);

        Name srvName = Name.fromConstantString("_crate._srv.crate.internal.");
        Record[] records = new Record[]{
            new SRVRecord(srvName, DClass.IN, 3600, 1, 10, 44300, Name.fromConstantString("crate1.internal.")),
            new SRVRecord(srvName, DClass.IN, 3600, 1, 20, 44300, Name.fromConstantString("crate2.internal.")),
            new SRVRecord(srvName, DClass.IN, 3600, 2, 10, 44300, Name.fromConstantString("crate3.internal.")),
            new SRVRecord(srvName, DClass.IN, 3600, 2, 20, 44300, Name.fromConstantString("crate4.internal."))
        };
        List<DiscoveryNode> discoNodes = unicastHostsProvider.parseRecords(records);
        // nodes need to be sorted by priority (asc), weight (desc) and name (asc) of SRV record
        assertEquals("#srv-crate1.internal:44300", discoNodes.get(0).getId());
        assertEquals("#srv-crate2.internal:44300", discoNodes.get(1).getId());
        assertEquals("#srv-crate3.internal:44300", discoNodes.get(2).getId());
        assertEquals("#srv-crate4.internal:44300", discoNodes.get(3).getId());
    }
}
