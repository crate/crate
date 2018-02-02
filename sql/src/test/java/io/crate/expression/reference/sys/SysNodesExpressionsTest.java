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
package io.crate.expression.reference.sys;

import com.google.common.net.InetAddresses;
import io.crate.Build;
import io.crate.Version;
import io.crate.data.Input;
import io.crate.expression.reference.NestedObjectExpression;
import io.crate.expression.reference.sys.node.local.NodeSysExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.expression.NestableInput;
import io.crate.metadata.RowGranularity;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.node.NodeService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.Inet4Address;
import java.util.Map;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for NodeSysExpression which is used for _node.
 */
@SuppressWarnings("unchecked")
public class SysNodesExpressionsTest extends CrateDummyClusterServiceUnitTest {

    private NodeSysExpression nodeSysExpression;
    private NodeEnvironment nodeEnvironment;
    private FsService fsService;

    @Before
    public void prepare() throws Exception {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir()).build();
        Environment environment = new Environment(settings);
        nodeEnvironment = new NodeEnvironment(settings, environment);
        MonitorService monitorService = new MonitorService(settings, nodeEnvironment, THREAD_POOL, () -> null);
        fsService = monitorService.fsService();

        HttpServerTransport httpServer = mock(HttpServerTransport.class);
        TransportAddress httpAddress = new TransportAddress(Inet4Address.getLocalHost(), 44200);
        BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[]{httpAddress}, httpAddress);
        when(httpServer.info()).thenReturn(new HttpInfo(boundAddress, 10));

        NodeService nodeService = mock(NodeService.class);
        when(nodeService.getMonitorService()).thenReturn(monitorService);

        nodeSysExpression = new NodeSysExpression(
            clusterService,
            nodeService,
            httpServer,
            THREAD_POOL,
            new ExtendedNodeInfo()
        );
    }

    @After
    public void releaseResources() throws Exception {
        nodeEnvironment.close();
    }

    @Test
    public void testLoad() throws Exception {
        Reference refInfo = refInfo("sys.nodes.load", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression load =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> v = load.value();
        assertNull(v.get("something"));
        if (isRunningOnWindows()) {
            assertThat(v.get("1"), is(-1.0d));
            assertThat(v.get("5"), is(-1.0d));
            assertThat(v.get("15"), is(-1.0d));
        } else if (isRunningOnMacOSX()) {
            assertThat((double) v.get("1"), greaterThanOrEqualTo(0.0d));
            assertThat(v.get("5"), is(-1.0d));
            assertThat(v.get("15"), is(-1.0d));
        } else {
            assertThat((double) v.get("1"), greaterThanOrEqualTo(0.0d));
            assertThat((double) v.get("5"), greaterThanOrEqualTo(0.0d));
            assertThat((double) v.get("15"), greaterThanOrEqualTo(0.0d));
        }

        Input ci = load.getChild("1");
        assertEquals(ci.value(), v.get("1"));
        ci = load.getChild("5");
        assertEquals(ci.value(), v.get("5"));
        ci = load.getChild("15");
        assertEquals(ci.value(), v.get("15"));
    }

    @Test
    public void testName() throws Exception {
        Reference refInfo = refInfo("sys.nodes.name", DataTypes.STRING, RowGranularity.NODE);
        NestableInput<BytesRef> name =
            (NestableInput<BytesRef>) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());
        assertEquals(new BytesRef("node-name"), name.value());
    }

    @Test
    public void testId() throws Exception {
        Reference refInfo = refInfo("sys.nodes.id", DataTypes.STRING, RowGranularity.NODE);
        NestableInput<BytesRef> id =
            (NestableInput<BytesRef>) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());
        assertEquals(new BytesRef("n1"), id.value());
    }

    @Test
    public void testHostname() throws Exception {
        Reference refInfo = refInfo("sys.nodes.hostname", DataTypes.STRING, RowGranularity.NODE);
        NestableInput<BytesRef> expression =
            (NestableInput) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());
        BytesRef hostname = expression.value();
        assertThat(hostname, notNullValue());
        assertThat(InetAddresses.isInetAddress(BytesRefs.toString(hostname)), is(false));
    }

    @Test
    public void testPorts() throws Exception {
        Reference refInfo = refInfo("sys.nodes.port", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression port =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> v = port.value();
        assertEquals(44200, v.get("http"));
        assertEquals(clusterService.localNode().getAddress().getPort(), v.get("transport"));
    }

    @Test
    public void testMemory() throws Exception {
        Reference refInfo = refInfo("sys.nodes.mem", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression mem =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> v = mem.value();

        assertThat((long) v.get("free"), greaterThan(10L));
        assertThat((short) v.get("free_percent"), greaterThan((short) -1)); // assert that it's not the default value -1

        assertThat((long) v.get("used"), greaterThan(10L));
        assertThat((short) v.get("used_percent"), greaterThan((short) -1)); // assert that it's not the default value -1
    }

    @Test
    public void testHeap() throws Exception {
        Reference refInfo = refInfo("sys.nodes.heap", DataTypes.STRING, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression heap =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> v = heap.value();

        assertThat((long) v.get("max"), greaterThan(1L));
        assertThat((long) v.get("used"), greaterThan(1L));
        assertThat((long) v.get("free"), greaterThan(1L));
    }

    @Test
    public void testFs() throws Exception {
        boolean ioStatsAvailable = fsService.stats().getIoStats() != null;
        Reference refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression fs =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> v = fs.value();
        Map<String, Object> total = (Map<String, Object>) v.get("total");
        assertThat((long) total.get("size"), greaterThan(1024L));
        assertThat((long) total.get("used"), greaterThan(1024L));
        assertThat((long) total.get("available"), greaterThan(1024L));
        if (ioStatsAvailable) {
            /*
              reads/bytes_read/writes/bytes_written are -1 if the FsInfo.ioStats() probe is null
              This is the case if the probe cache has not been refreshed (default refresh interval is 1s).
              Unfortunately the probe cannot be forced to refresh.
             */
            assertThat((long) total.get("reads"), greaterThanOrEqualTo(0L));
            assertThat((long) total.get("bytes_written"), greaterThanOrEqualTo(0L));
            assertThat((long) total.get("writes"), greaterThanOrEqualTo(0L));
            assertThat((long) total.get("bytes_written"), greaterThanOrEqualTo(0L));
        } else {
            assertThat(total.get("reads"), is(-1L));
            assertThat(total.get("bytes_written"), is(-1L));
            assertThat(total.get("writes"), is(-1L));
            assertThat(total.get("bytes_written"), is(-1L));
        }

        Object[] disks = (Object[]) v.get("disks");
        assertThat(disks.length, greaterThanOrEqualTo(1));
        for (int i = 0; i < disks.length; i++) {
            assertNotNull(((Map<String, Object>) disks[i]).get("dev"));
            assertThat((long) ((Map<String, Object>) disks[i]).get("size"), greaterThan(1024L));
        }

        Object[] data = (Object[]) v.get("data");
        assertEquals(data.length, disks.length);
        for (int i = 0; i < data.length; i++) {
            assertNotNull(((Map<String, Object>) data[i]).get("dev"));
            assertNotNull(((Map<String, Object>) data[i]).get("path"));
        }

        refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE, "data", "dev");
        NestedObjectExpression fsRef =
            (NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        SysStaticObjectArrayReference dataRef =
            (SysStaticObjectArrayReference) fsRef.getChild(refInfo.ident().columnIdent().path().get(0));

        for (io.crate.expression.reference.NestedObjectExpression exp : dataRef.getChildImplementations()) {
            assertThat(
                exp.getChild(refInfo.ident().columnIdent().path().get(1)).value(),
                instanceOf(BytesRef.class)
            );
        }
    }

    @Test
    public void testVersion() throws Exception {
        Reference refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression version =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> v = version.value();
        assertEquals(Version.CURRENT.number(), v.get("number"));
        assertEquals(Build.CURRENT.hash(), v.get("build_hash"));
        assertEquals(Version.CURRENT.snapshot, v.get("build_snapshot"));

    }

    @Test
    public void testNetwork() throws Exception {
        Reference refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression networkRef =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> networkStats = networkRef.value();
        assertThat(mapToSortedString(networkStats),
            is("probe_timestamp=0, tcp={" +
               "connections={accepted=0, curr_established=0, dropped=0, embryonic_dropped=0, initiated=0}, " +
               "packets={errors_received=0, received=0, retransmitted=0, rst_sent=0, sent=0}" +
               "}"));
    }

    @Test
    public void testNetworkTCP() throws Exception {
        Reference refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE, "tcp");
        ColumnIdent columnIdent = refInfo.ident().columnIdent();
        io.crate.expression.reference.NestedObjectExpression network = (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(columnIdent.name());
        NestedObjectExpression tcpRef = (NestedObjectExpression) network.getChild(columnIdent.path().get(0));
        Map<String, Object> tcpStats = tcpRef.value();

        assertThat(tcpStats, instanceOf(Map.class));
        assertThat(mapToSortedString(tcpStats),
            is("connections={accepted=0, curr_established=0, dropped=0, embryonic_dropped=0, initiated=0}, " +
               "packets={errors_received=0, received=0, retransmitted=0, rst_sent=0, sent=0}"));
    }

    @Test
    public void testCpu() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression os =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> v = os.value();
        if (Constants.LINUX) {
            assertThat((long) v.get("uptime"), greaterThan(1000L));
        }
        // Windows and macOS require a sys call for "uptime",
        // Sometimes syscalls work, sometimes not, e.g. starting tests with Powershell works
        // TODO: Figure out why. For now, just ignore other OSs than Linux
        // assertThat(v.get("uptime"), is(-1L));

        String cpu = mapToSortedString((Map<String, Object>) v.get("cpu"));
        assertThat(cpu, containsString("system=-1"));
        assertThat(cpu, containsString("user=-1"));
        assertThat(cpu, containsString("idle=-1"));
        assertThat(cpu, containsString("stolen=-1"));
        assertThat(cpu, containsString("used=")); // "used" is greater than -1
    }

    @Test
    public void testOsCgroup() {
        Reference refInfo = refInfo("sys.nodes.os", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression os =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());
        Map<String, Object> v = os.value();
        Map<String, Object> cgroup = (Map<String, Object>) v.get("cgroup");
        if (Constants.LINUX) {
            // cgroup is only available on Linux
            String cgroupCpu = mapToSortedString((Map<String, Object>) cgroup.get("cpu"));
            assertThat(cgroupCpu, containsString("cfs_period_micros="));
            assertThat(cgroupCpu, containsString("cfs_quota_micros="));
            assertThat(cgroupCpu, containsString("control_group=/"));
            assertThat(cgroupCpu, containsString("num_elapsed_periods="));
            assertThat(cgroupCpu, containsString("num_times_throttled="));
            assertThat(cgroupCpu, containsString("time_throttled_nanos="));
            String cgroupCpuAcct = mapToSortedString((Map<String, Object>) cgroup.get("cpuacct"));
            assertThat(cgroupCpuAcct, containsString("control_group=/"));
            assertThat(cgroupCpuAcct, containsString("usage_nanos="));
            String cgroupMem = mapToSortedString((Map<String, Object>) cgroup.get("mem"));
            assertThat(cgroupMem, containsString("control_group=/"));
            assertThat(cgroupMem, containsString("limit_bytes="));
            assertThat(cgroupMem, containsString("usage_bytes="));
        }
    }

    @Test
    public void testProcess() throws Exception {
        Reference refInfo = refInfo("sys.nodes.process", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression processRef = (io.crate.expression.reference.NestedObjectExpression)
            nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> v = processRef.value();

        if (isRunningOnWindows() == false) {
            assertThat((long) v.get("open_file_descriptors"), greaterThan(10L));
            assertThat((long) v.get("max_open_file_descriptors"), greaterThan(10L));
        }

        String cpu = mapToSortedString((Map<String, Object>) v.get("cpu"));
        assertThat(cpu, containsString("system=-1"));
        assertThat(cpu, containsString("user=-1"));
        assertThat(cpu, containsString("percent=")); // "percent" is greater than -1
    }

    @Test
    public void testOsInfo() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os_info", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.expression.reference.NestedObjectExpression ref =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(refInfo.ident().columnIdent().name());

        Map<String, Object> v = ref.value();
        int cores = (int) v.get("available_processors");
        assertThat(cores, is(EsExecutors.numberOfProcessors(Settings.EMPTY)));
    }

    @Test
    public void testNestedBytesRefExpressionsString() throws Exception {
        Reference refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        ColumnIdent versionColIdent = refInfo.ident().columnIdent();
        io.crate.expression.reference.NestedObjectExpression version =
            (io.crate.expression.reference.NestedObjectExpression) nodeSysExpression.getChild(versionColIdent.name());

        assertThat(version.value().get("number"), instanceOf(String.class));
    }
}
