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

import com.google.common.net.InetAddresses;
import io.crate.Build;
import io.crate.Version;
import io.crate.data.Input;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.RowGranularity;
import io.crate.monitor.DummyExtendedNodeInfo;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.node.local.NodeSysExpression;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.discovery.local.LocalDiscovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.monitor.MonitorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.Inet4Address;
import java.util.HashMap;
import java.util.Map;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysNodesExpressionsTest extends CrateDummyClusterServiceUnitTest {

    private NodeSysExpression nodeSysExpression;
    private NodeEnvironment nodeEnvironment;

    @Before
    public void prepare() throws Exception {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir()).build();
        LocalDiscovery localDiscovery = new LocalDiscovery(settings, clusterService, clusterService.getClusterSettings(),
            mock(NamedWriteableRegistry.class));
        Environment environment = new Environment(settings);
        nodeEnvironment = new NodeEnvironment(settings, environment);
        MonitorService monitorService = new MonitorService(settings, nodeEnvironment, THREAD_POOL);

        HttpServerTransport httpServer = mock(HttpServerTransport.class);
        TransportAddress httpAddress = new InetSocketTransportAddress(Inet4Address.getLocalHost(), 44200);
        BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[]{httpAddress}, httpAddress);
        when(httpServer.info()).thenReturn(new HttpInfo(boundAddress, 10));

        nodeSysExpression = new NodeSysExpression(
            clusterService,
            monitorService,
            httpServer,
            localDiscovery,
            THREAD_POOL,
            new DummyExtendedNodeInfo(nodeEnvironment)
        );
    }

    @After
    public void releaseResources() throws Exception {
        nodeEnvironment.close();
    }

    @Test
    public void testLoad() throws Exception {
        Reference refInfo = refInfo("sys.nodes.load", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression load =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

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
        Reference refInfo = refInfo("sys.nodes.name", DataTypes.STRING, RowGranularity.NODE);
        @SuppressWarnings("unchecked") ReferenceImplementation<BytesRef> name =
            (ReferenceImplementation<BytesRef>) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());
        assertEquals(new BytesRef("node-name"), name.value());
    }

    @Test
    public void testId() throws Exception {
        Reference refInfo = refInfo("sys.nodes.id", DataTypes.STRING, RowGranularity.NODE);
        @SuppressWarnings("unchecked") ReferenceImplementation<BytesRef> id =
            (ReferenceImplementation<BytesRef>) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());
        assertEquals(new BytesRef("node"), id.value());
    }

    @Test
    public void testHostname() throws Exception {
        Reference refInfo = refInfo("sys.nodes.hostname", DataTypes.STRING, RowGranularity.NODE);
        @SuppressWarnings("unchecked") ReferenceImplementation<BytesRef> expression =
            (ReferenceImplementation) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());
        BytesRef hostname = expression.value();
        assertThat(hostname, notNullValue());
        assertThat(InetAddresses.isInetAddress(BytesRefs.toString(hostname)), is(false));
    }

    @Test
    public void testPorts() throws Exception {
        Reference refInfo = refInfo("sys.nodes.port", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression port =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = port.value();
        assertEquals(44200, v.get("http"));
        assertEquals(-1, v.get("transport"));
    }

    @Test
    public void testMemory() throws Exception {
        Reference refInfo = refInfo("sys.nodes.mem", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression mem =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = mem.value();

        assertThat((long) v.get("free"), greaterThan(10L));
        assertThat((short) v.get("free_percent"), greaterThan((short) -1)); // assert that it's not the default value -1

        assertThat((long) v.get("used"), greaterThan(10L));
        assertThat((short) v.get("used_percent"), greaterThan((short) -1)); // assert that it's not the default value -1
    }

    @Test
    public void testHeap() throws Exception {
        Reference refInfo = refInfo("sys.nodes.heap", DataTypes.STRING, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression heap =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = heap.value();

        assertThat((long) v.get("max"), greaterThan(1L));
        assertThat((long) v.get("used"), greaterThan(1L));
        assertThat((long) v.get("free"), greaterThan(1L));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFs() throws Exception {
        Reference refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression fs =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = fs.value();
        String total = mapToSortedString((Map<String, Object>) v.get("total"));
        assertThat(total, is("available=86016, bytes_read=84, bytes_written=84, reads=84, size=86016, used=86016, writes=84"));
        Object[] disks = (Object[]) v.get("disks");
        assertThat(disks.length, is(2));
        Map<String, Object> disk0 = (Map<String, Object>) disks[0];
        assertThat((String) disk0.get("dev"), is("/dev/sda1"));
        assertThat((Long) disk0.get("size"), is(42L));

        Map<String, Object> disk1 = (Map<String, Object>) disks[1];
        assertThat((String) disk1.get("dev"), is("/dev/sda2"));
        assertThat((Long) disk0.get("used"), is(42L));

        Object[] data = (Object[]) v.get("data");
        assertThat(data.length, is(2));
        assertThat((String) ((Map<String, Object>) data[0]).get("dev"), is("/dev/sda1"));
        assertThat((String) ((Map<String, Object>) data[0]).get("path"), is("/foo"));

        assertThat((String) ((Map<String, Object>) data[1]).get("dev"), is("/dev/sda2"));
        assertThat((String) ((Map<String, Object>) data[1]).get("path"), is("/bar"));

        refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE, "data", "dev");
        NestedObjectExpression fsRef =
            (NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        SysStaticObjectArrayReference dataRef =
            (SysStaticObjectArrayReference) fsRef.getChildImplementation(refInfo.ident().columnIdent().path().get(0));

        for (io.crate.operation.reference.NestedObjectExpression exp : dataRef.getChildImplementations()) {
            assertThat(
                exp.getChildImplementation(refInfo.ident().columnIdent().path().get(1)).value(),
                instanceOf(BytesRef.class)
            );
        }
    }

    @Test
    public void testVersion() throws Exception {
        Reference refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression version =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = version.value();
        assertEquals(Version.CURRENT.number(), v.get("number"));
        assertEquals(Build.CURRENT.hash(), v.get("build_hash"));
        assertEquals(Version.CURRENT.snapshot, v.get("build_snapshot"));

    }

    @Test
    public void testNetwork() throws Exception {
        Reference refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression networkRef =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> networkStats = networkRef.value();
        assertThat(mapToSortedString(networkStats),
            is("probe_timestamp=0, tcp={" +
               "connections={accepted=42, curr_established=42, dropped=42, embryonic_dropped=42, initiated=42}, " +
               "packets={errors_received=42, received=42, retransmitted=42, rst_sent=42, sent=42}" +
               "}"));
    }

    @Test
    public void testNetworkTCP() throws Exception {
        Reference refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE, "tcp");
        ColumnIdent columnIdent = refInfo.ident().columnIdent();
        io.crate.operation.reference.NestedObjectExpression network = (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(columnIdent.name());
        NestedObjectExpression tcpRef = (NestedObjectExpression) network.getChildImplementation(columnIdent.path().get(0));
        Map<String, Object> tcpStats = tcpRef.value();

        assertThat(tcpStats, instanceOf(Map.class));
        assertThat(mapToSortedString(tcpStats),
            is("connections={accepted=42, curr_established=42, dropped=42, embryonic_dropped=42, initiated=42}, " +
               "packets={errors_received=42, received=42, retransmitted=42, rst_sent=42, sent=42}"));
    }

    @Test
    public void testCpu() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression os =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = os.value();
        assertEquals(3600000L, v.get("uptime"));

        Map<String, Short> cpuObj = new HashMap<>(5);
        cpuObj.put("system", (short) 0);
        cpuObj.put("user", (short) 4);
        cpuObj.put("idle", (short) 94);
        cpuObj.put("used", (short) 4);
        cpuObj.put("stolen", (short) 10);
        assertEquals(cpuObj, v.get("cpu"));
    }

    @Test
    public void testProcess() throws Exception {
        Reference refInfo = refInfo("sys.nodes.process", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression processRef = (io.crate.operation.reference.NestedObjectExpression)
            nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = processRef.value();

        if (isRunningOnWindows() == false) {
            assertThat((long) v.get("open_file_descriptors"), greaterThan(10L));
            assertThat((long) v.get("max_open_file_descriptors"), greaterThan(10L));
        }

        Map<String, Object> cpuObj = new HashMap<>(4);
        cpuObj.put("percent", (short) 50);
        cpuObj.put("system", 1000L);
        cpuObj.put("user", 500L);
        assertThat(v.get("cpu"), is(cpuObj));
    }

    @Test
    public void testOsInfo() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os_info", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression ref =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = ref.value();
        int cores = (int) v.get("available_processors");
        assertThat(cores, is(EsExecutors.boundedNumberOfProcessors(Settings.EMPTY)));
    }

    @Test
    public void testNestedBytesRefExpressionsString() throws Exception {
        Reference refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        ColumnIdent versionColIdent = refInfo.ident().columnIdent();
        io.crate.operation.reference.NestedObjectExpression version =
            (io.crate.operation.reference.NestedObjectExpression) nodeSysExpression.getChildImplementation(versionColIdent.name());

        assertThat(version.value().get("number"), instanceOf(String.class));
    }
}
