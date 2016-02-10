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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */
package io.crate.operation.reference.sys;

import com.google.common.collect.ImmutableMap;
import io.crate.Build;
import io.crate.Version;
import io.crate.metadata.NestedReferenceResolver;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleObjectExpression;
import io.crate.operation.Input;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.node.*;
import io.crate.stats.DummyExtendedNodeStats;
import io.crate.stats.ExtendedNodeStats;
import io.crate.stats.StatsModule;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysNodesExpressionsTest extends CrateUnitTest {

    private static final Settings NODE_SETTINGS = Settings.builder()
            .put(StatsModule.EXTENDED_STATS_TYPE, "dummy")
            .build();

    private Injector injector;
    private NestedReferenceResolver resolver;

    static class TestModule extends AbstractModule {

        private final boolean isDataNode;

        public TestModule(boolean isDataNode) {
            this.isDataNode = isDataNode;
        }

        @Override
        protected void configure() {
            bind(Settings.class).toInstance(Settings.EMPTY);

            ClusterService clusterService = mock(ClusterService.class);
            bind(ClusterService.class).toInstance(clusterService);

            OsService osService = mock(OsService.class);
            OsStats osStats = mock(OsStats.class);
            when(osService.stats()).thenReturn(osStats);

            ByteSizeValue byteSizeValue = mock(ByteSizeValue.class);
            when(byteSizeValue.bytes()).thenReturn(12345342234L);
            when(byteSizeValue.toString()).thenReturn("11.4gb");

            OsStats.Mem mem = mock(OsStats.Mem.class);
            when(osStats.getMem()).thenReturn(mem);
            when(mem.getFree()).thenReturn(byteSizeValue);
            when(mem.getUsed()).thenReturn(byteSizeValue);
            when(mem.getUsedPercent()).thenReturn((short) 22);
            when(mem.getFreePercent()).thenReturn((short) 78);

            OsInfo osInfo = mock(OsInfo.class);
            when(osService.info()).thenReturn(osInfo);
            when(osInfo.getAvailableProcessors()).thenReturn(4);

            bind(OsService.class).toInstance(osService);

            NodeService nodeService = mock(NodeService.class);
            NodeStats nodeStats = mock(NodeStats.class);
            try {
                when(nodeService.stats()).thenReturn(nodeStats);
            } catch (IOException e) {
                // wrong signature, IOException will never be thrown
            }

            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getHostName()).thenReturn("localhost");
            when(nodeStats.getNode()).thenReturn(node);
            when(clusterService.localNode()).thenReturn(node);

            when(nodeStats.getOs()).thenReturn(osStats);

            ProcessStats processStats = mock(ProcessStats.class);
            when(nodeStats.getProcess()).thenReturn(processStats);
            when(processStats.getOpenFileDescriptors()).thenReturn(42L);
            when(processStats.getMaxFileDescriptors()).thenReturn(1000L);

            NodeInfo nodeInfo = mock(NodeInfo.class);
            when(nodeService.info()).thenReturn(nodeInfo);

            Discovery discovery = mock(Discovery.class);
            bind(Discovery.class).toInstance(discovery);
            when(discovery.localNode()).thenReturn(node);
            when(node.getId()).thenReturn("node-id-1");
            when(node.getName()).thenReturn("node 1");

            InetAddress localhost = null;
            try {
                localhost = InetAddress.getByName("localhost");
            } catch (UnknownHostException e) {
                fail(e.getMessage());
            }

            TransportAddress transportAddress = new InetSocketTransportAddress(localhost, 44300);
            when(node.address()).thenReturn(transportAddress);
            when(node.attributes()).thenReturn(
                    ImmutableMap.<String, String>builder().put("http_address", "http://localhost:44200").build()
            );


            bind(NodeService.class).toInstance(nodeService);

            NodeEnvironment nodeEnv = mock(NodeEnvironment.class);
            Path[] dataLocations = new Path[]{new File("/foo").toPath(), new File("/bar").toPath()};
            when(nodeEnv.hasNodeFile()).then(new Answer<Boolean>() {
                @Override
                public Boolean answer(InvocationOnMock invocation) throws Throwable {
                    return isDataNode;
                }
            });
            when(nodeEnv.nodeDataPaths()).thenReturn(dataLocations);
            bind(NodeEnvironment.class).toInstance(nodeEnv);

            HttpInfo httpInfo = mock(HttpInfo.class);
            when(nodeInfo.getHttp()).thenReturn(httpInfo);
            TransportAddress[] boundAddresses = new TransportAddress[]{new InetSocketTransportAddress(localhost, 44200)};
            BoundTransportAddress boundTransportAddress = new BoundTransportAddress(boundAddresses, boundAddresses[0]);
            when(httpInfo.address()).thenReturn(boundTransportAddress);

            JvmService jvmService = mock(JvmService.class);
            JvmStats jvmStats = mock(JvmStats.class);
            JvmStats.Mem jvmStatsMem = mock(JvmStats.Mem.class);
            ByteSizeValue heapByteSizeValueMax = mock(ByteSizeValue.class);
            when(heapByteSizeValueMax.bytes()).thenReturn(123456L);
            when(jvmStatsMem.getHeapMax()).thenReturn(heapByteSizeValueMax);
            when(jvmStatsMem.getHeapUsed()).thenReturn(heapByteSizeValueMax);

            when(jvmStats.getMem()).thenReturn(jvmStatsMem);
            when(jvmService.stats()).thenReturn(jvmStats);
            bind(JvmService.class).toInstance(jvmService);

            ThreadPool threadPool = new ThreadPool(getClass().getName());
            bind(ThreadPool.class).toInstance(threadPool);
        }
    }

    /**
     * Resolve canonical path (platform independent)
     *
     * @param path the path to be resolved (e.g. /dev/sda1)
     * @return full canonical path (e.g. linux will resolve to /dev/sda1, windows to C:\dev\sda1)
     */
    String resolveCanonicalPath(String path) {
        try {
            return new File(path).getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Before
    public void prepare() throws Exception {
        StatsModule statsModule = new StatsModule(NODE_SETTINGS);
        statsModule.addExtendedStatsType("dummy", DummyExtendedNodeStats.class);

        injector = new ModulesBuilder().add(
                new TestModule(true),
                statsModule,
                new SysNodeExpressionModule()
        ).createInjector();
        resolver = new NodeSysReferenceResolver(injector.getInstance(NodeSysExpression.class));
    }

    @After
    public void cleanUp() throws Exception {
        injector.getInstance(ThreadPool.class).shutdownNow();
    }

    @Test
    public void testLoad() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.load", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression load = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = load.value();
        assertNull(v.get("something"));
        assertEquals(1D, v.get("1"));
        assertEquals(5D, v.get("5"));
        assertEquals(15D, v.get("15"));

        Input ci = load.getChildImplementation("1");
        assertEquals(1D, ci.value());
        ci = load.getChildImplementation("5");
        assertEquals(5D, ci.value());
        ci = load.getChildImplementation("15");
        assertEquals(15D, ci.value());
    }

    @Test
    public void testName() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.name", DataTypes.STRING, RowGranularity.NODE);
        SimpleObjectExpression<String> name = (SimpleObjectExpression<String>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("node 1"), name.value());
    }

    @Test
    public void testId() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.id", DataTypes.STRING, RowGranularity.NODE);
        SimpleObjectExpression<BytesRef> id = (SimpleObjectExpression<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("node-id-1"), id.value());
    }

    @Test
    public void testHostname() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.hostname", DataTypes.STRING, RowGranularity.NODE);
        SimpleObjectExpression<BytesRef> hostname = (SimpleObjectExpression<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("localhost"), hostname.value());
    }

    @Test
    public void testRestUrl() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.rest_url", DataTypes.STRING, RowGranularity.NODE);
        SimpleObjectExpression<BytesRef> http_addr = (SimpleObjectExpression<BytesRef>) resolver.getImplementation(refInfo);
        assertEquals(new BytesRef("http://localhost:44200"), http_addr.value());
    }

    @Test
    public void testPorts() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.port", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression port = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = port.value();
        assertEquals(44200, v.get("http"));
        assertEquals(44300, v.get("transport"));
    }

    @Test
    public void testMemory() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.mem", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression mem = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = mem.value();

        assertEquals(12345342234L, v.get("free"));
        assertEquals(new Short("78"), v.get("free_percent"));

        assertEquals(12345342234L, v.get("used"));
        assertEquals(new Short("22"), v.get("used_percent"));
    }

    @Test
    public void testHeap() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.heap", DataTypes.STRING, RowGranularity.NODE);
        NestedObjectExpression heap = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = heap.value();

        assertEquals(123456L, v.get("max"));
        assertEquals(123456L, v.get("used"));
        assertEquals(0L, v.get("free"));
    }

    @Test
    public void testFs() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE);
        NestedObjectExpression fs = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = fs.value();
        String total = mapToSortedString((Map<String, Object>) v.get("total"));
        assertThat(total, is("available=86016, bytes_read=84, bytes_written=84, reads=84, size=86016, used=86016, writes=84"));
        Object[] disks = (Object[]) v.get("disks");
        assertThat(disks.length, is(2));
        Map<String, Object> disk0 = (Map<String, Object>) disks[0];
        assertThat((String) disk0.get("dev"), is(resolveCanonicalPath("/dev/sda1")));
        assertThat((Long) disk0.get("size"), is(42L * 1024));

        Map<String, Object> disk1 = (Map<String, Object>) disks[1];
        assertThat((String) disk1.get("dev"), is(resolveCanonicalPath("/dev/sda2")));
        assertThat((Long) disk0.get("used"), is(42L * 1024));

        Object[] data = (Object[]) v.get("data");
        assertThat(data.length, is(2));
        assertThat((String) ((Map<String, Object>) data[0]).get("dev"), is(resolveCanonicalPath("/dev/sda1")));
        assertThat((String) ((Map<String, Object>) data[0]).get("path"), is(resolveCanonicalPath("/foo")));

        assertThat((String) ((Map<String, Object>) data[1]).get("dev"), is(resolveCanonicalPath("/dev/sda2")));
        assertThat((String) ((Map<String, Object>) data[1]).get("path"), is(resolveCanonicalPath("/bar")));

        refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE, "data", "dev");
        SimpleObjectExpression<Object[]> fsData = (SimpleObjectExpression<Object[]>) resolver.getImplementation(refInfo);
        for (Object arrayElement : fsData.value()) {
            assertThat(arrayElement, instanceOf(BytesRef.class));
        }

    }

    @Test
    public void testVersion() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression version = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = version.value();
        assertEquals(Version.CURRENT.number(), v.get("number"));
        assertEquals(Build.CURRENT.hash(), v.get("build_hash"));
        assertEquals(Version.CURRENT.snapshot, v.get("build_snapshot"));

    }

    @Test
    public void testNetwork() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression networkRef = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> networkStats = networkRef.value();
        assertThat(mapToSortedString(networkStats),
                is("probe_timestamp=0, tcp={" +
                   "connections={accepted=42, curr_established=42, dropped=42, embryonic_dropped=42, initiated=42}, " +
                   "packets={errors_received=42, received=42, retransmitted=42, rst_sent=42, sent=42}" +
                   "}"));
    }

    @Test
    public void testNetworkTCP() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE, "tcp");
        NestedObjectExpression tcpRef = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> tcpStats = tcpRef.value();

        assertThat(tcpStats, instanceOf(Map.class));
        assertThat(mapToSortedString(tcpStats),
                is("connections={accepted=42, curr_established=42, dropped=42, embryonic_dropped=42, initiated=42}, " +
                   "packets={errors_received=42, received=42, retransmitted=42, rst_sent=42, sent=42}"));
    }

    @Test
    public void testCpu() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.os", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression os = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = os.value();
        assertEquals(3600000L, v.get("uptime"));

        Map<String, Short> cpuObj = new HashMap<>(5);
        cpuObj.put("system", (short) 2);
        cpuObj.put("user", (short) 4);
        cpuObj.put("idle", (short) 94);
        cpuObj.put("used", (short) 6);
        cpuObj.put("stolen", (short) 10);
        assertEquals(cpuObj, v.get("cpu"));
    }

    @Test
    public void testProcess() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.process", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression processRef = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = processRef.value();
        assertEquals(42L, (long) v.get("open_file_descriptors"));
        assertEquals(1000L, (long) v.get("max_open_file_descriptors"));

        Map<String, Object> cpuObj = new HashMap<>(4);
        cpuObj.put("percent", (short) 50);
        cpuObj.put("system", 1000L);
        cpuObj.put("user", 500L);
        assertEquals(cpuObj, v.get("cpu"));
    }

    @Test
    public void testOsInfo() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.os_info", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression ref = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = ref.value();
        int cores = (int) v.get("available_processors");
        assertEquals(4, cores);
    }

    @Test
    public void testNestedBytesRefExpressionsString() throws Exception {
        ReferenceInfo refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression version = (NestedObjectExpression) resolver.getImplementation(refInfo);

        refInfo = refInfo("sys.nodes.version", DataTypes.STRING, RowGranularity.NODE, "number");
        SysNodeExpression<BytesRef> versionNumber = (SysNodeExpression<BytesRef>) resolver.getImplementation(refInfo);

        assertThat(version.value().get(NodeVersionExpression.NUMBER), instanceOf(String.class));
        assertThat(versionNumber.value(), is(new BytesRef(version.value().get(NodeVersionExpression.NUMBER).toString())));

    }

}
