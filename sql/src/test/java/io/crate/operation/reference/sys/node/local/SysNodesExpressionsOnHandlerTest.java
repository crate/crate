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

package io.crate.operation.reference.sys.node.local;

import io.crate.Build;
import io.crate.Version;
import io.crate.metadata.Reference;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.RowGranularity;
import io.crate.monitor.DummyExtendedNodeInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.reference.sys.RowContextReferenceResolver;
import io.crate.operation.reference.sys.node.NodeStatsContext;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class SysNodesExpressionsOnHandlerTest extends CrateUnitTest {

    private final RowContextReferenceResolver resolver = RowContextReferenceResolver.INSTANCE;
    private static final NodeStatsContext CONTEXT = new NodeStatsContext(false);

    private CollectExpression collectExpression;

    @BeforeClass
    public static void setup() throws IOException {

        // jvm
        JvmStats jvmStats = mock(JvmStats.class);
        JvmStats.Mem jvmStatsMem = mock(JvmStats.Mem.class);
        ByteSizeValue heapByteSizeValueMax = new ByteSizeValue(123456L);
        when(jvmStatsMem.getHeapMax()).thenReturn(heapByteSizeValueMax);
        when(jvmStatsMem.getHeapUsed()).thenReturn(heapByteSizeValueMax);
        when(jvmStats.getMem()).thenReturn(jvmStatsMem);

        // mem
        ByteSizeValue byteSizeValue = new ByteSizeValue(12345342234L);

        // os service
        OsService osService = mock(OsService.class);
        OsStats osStats = mock(OsStats.class);
        when(osService.stats()).thenReturn(osStats);
        OsStats.Mem mem = mock(OsStats.Mem.class);
        when(osStats.getMem()).thenReturn(mem);
        when(mem.getFree()).thenReturn(byteSizeValue);
        when(mem.getUsed()).thenReturn(byteSizeValue);
        when(mem.getUsedPercent()).thenReturn((short) 22);
        when(mem.getFreePercent()).thenReturn((short) 78);

        // os info
        OsInfo osInfo = mock(OsInfo.class);
        when(osService.info()).thenReturn(osInfo);
        when(osInfo.getAvailableProcessors()).thenReturn(4);

        // node info
        NodeEnvironment nodeEnv = mock(NodeEnvironment.class);
        Path[] dataLocations = new Path[]{new File("/foo").toPath(), new File("/bar").toPath()};
        when(nodeEnv.hasNodeFile()).then(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return true;
            }
        });
        when(nodeEnv.nodeDataPaths()).thenReturn(dataLocations);
        ExtendedNodeInfo extendedNodeInfo = new DummyExtendedNodeInfo(nodeEnv);

        // process stats
        ProcessStats processStats = mock(ProcessStats.class);
        when(processStats.getOpenFileDescriptors()).thenReturn(42L);
        when(processStats.getMaxFileDescriptors()).thenReturn(1000L);

        CONTEXT.id(BytesRefs.toBytesRef("93c7ff92-52fa-11e6-aad8-3c15c2d3ad18"));
        CONTEXT.name(BytesRefs.toBytesRef("crate1"));
        CONTEXT.hostname(BytesRefs.toBytesRef("crate1.example.com"));
        CONTEXT.version(Version.CURRENT);
        CONTEXT.build(Build.CURRENT);
        CONTEXT.restUrl(BytesRefs.toBytesRef("10.0.0.1:4200"));
        CONTEXT.port(new HashMap<String, Integer>(2) {{
            put("http", 4200);
            put("transport", 4300);
        }});
        CONTEXT.jvmStats(jvmStats);
        CONTEXT.osInfo(osInfo);
        CONTEXT.processStats(processStats);
        CONTEXT.osStats(osStats);
        CONTEXT.extendedOsStats(extendedNodeInfo.osStats());
        CONTEXT.networkStats(extendedNodeInfo.networkStats());
        CONTEXT.extendedProcessCpuStats(extendedNodeInfo.processCpuStats());
        CONTEXT.extendedFsStats(extendedNodeInfo.fsStats());
    }

    @Test
    public void testLoad() throws Exception {
        Reference refInfo = refInfo("sys.nodes.load", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat((double) v.get("1"), is(1D));
        assertThat((double) v.get("5"), is(5D));
        assertThat((double) v.get("15"), is(15D));
    }

    @Test
    public void testName() throws Exception {
        Reference refInfo = refInfo("sys.nodes.name", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = (RowCollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);
        assertThat(BytesRefs.toBytesRef("crate1"), is(collectExpression.value()));
    }

    @Test
    public void testId() throws Exception {
        Reference refInfo = refInfo("sys.nodes.id", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);
        assertThat(BytesRefs.toBytesRef("93c7ff92-52fa-11e6-aad8-3c15c2d3ad18"), is(collectExpression.value()));
    }

    @Test
    public void testHostname() throws Exception {
        Reference refInfo = refInfo("sys.nodes.hostname", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);
        assertThat(BytesRefs.toBytesRef("crate1.example.com"), is(collectExpression.value()));
    }

    @Test
    public void testRestUrl() throws Exception {
        Reference refInfo = refInfo("sys.nodes.rest_url", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);
        assertThat(BytesRefs.toBytesRef("10.0.0.1:4200"), is(collectExpression.value()));
    }

    @Test
    public void testPorts() throws Exception {
        Reference refInfo = refInfo("sys.nodes.port", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = (RowCollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat((int) v.get("http"), is(4200));
        assertThat((int) v.get("transport"), is(4300));
    }

    @Test
    public void testMemory() throws Exception {
        Reference refInfo = refInfo("sys.nodes.mem", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat((long) v.get("free"), is(12345342234L));
        assertThat((short) v.get("free_percent"), is(Short.valueOf("78")));
        assertThat((long) v.get("used"), is(12345342234L));
        assertThat((short) v.get("used_percent"), is(Short.valueOf("22")));
    }

    @Test
    public void testHeap() throws Exception {
        Reference refInfo = refInfo("sys.nodes.heap", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat((long) v.get("max"), is(123456L));
        assertThat((long) v.get("used"), is(123456L));
        assertThat((long) v.get("free"), is(0L));
    }

    @Test
    public void testFs() throws Exception {
        Reference refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        String total = mapToSortedString((Map<String, Object>) v.get("total"));
        assertThat(total, is("available=86016, bytes_read=84, bytes_written=84, reads=84, size=86016, used=86016, writes=84"));
        Object[] disks = (Object[]) v.get("disks");
        assertThat(disks.length, is(2));
        Map<String, Object> disk0 = (Map<String, Object>) disks[0];
        assertThat((BytesRef) disk0.get("dev"), is(BytesRefs.toBytesRef("/dev/sda1")));
        assertThat((Long) disk0.get("size"), is(42L));

        Map<String, Object> disk1 = (Map<String, Object>) disks[1];
        assertThat((BytesRef) disk1.get("dev"), is(BytesRefs.toBytesRef("/dev/sda2")));
        assertThat((Long) disk0.get("used"), is(42L));

        Object[] data = (Object[]) v.get("data");
        assertThat(data.length, is(2));
        assertThat(
            (BytesRef) ((Map<String, Object>) data[0]).get("dev"),
            is(BytesRefs.toBytesRef("/dev/sda1"))
        );
        assertThat(
            (BytesRef) ((Map<String, Object>) data[0]).get("path"),
            is(BytesRefs.toBytesRef("/foo"))
        );

        assertThat(
            (BytesRef) ((Map<String, Object>) data[1]).get("dev"),
            is(BytesRefs.toBytesRef("/dev/sda2"))
        );
        assertThat(
            (BytesRef) ((Map<String, Object>) data[1]).get("path"),
            is(BytesRefs.toBytesRef("/bar"))
        );

        refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE, "data", "dev");
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);
        for (Object arrayElement : (Object[]) collectExpression.value()) {
            assertThat(arrayElement, instanceOf(BytesRef.class));
        }
    }

    @Test
    public void testVersion() throws Exception {
        Reference refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat((String) v.get("number"), is(Version.CURRENT.number()));
        assertThat((String) v.get("build_hash"), is(Build.CURRENT.hash()));
        assertThat((boolean) v.get("build_snapshot"), is(Version.CURRENT.snapshot()));
    }

    @Test
    public void testNetwork() throws Exception {
        Reference refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> networkStats = (Map<String, Object>) collectExpression.value();
        assertThat(mapToSortedString(networkStats),
            is("probe_timestamp=0, tcp={" +
               "connections={accepted=42, curr_established=42, dropped=42, embryonic_dropped=42, initiated=42}, " +
               "packets={errors_received=42, received=42, retransmitted=42, rst_sent=42, sent=42}" +
               "}"));
    }

    @Test
    public void testNetworkTCP() throws Exception {
        Reference refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE, "tcp");
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);
        Map<String, Object> tcpStats = (Map<String, Object>) collectExpression.value();

        assertThat(tcpStats, instanceOf(Map.class));
        assertThat(mapToSortedString(tcpStats),
            is("connections={accepted=42, curr_established=42, dropped=42, embryonic_dropped=42, initiated=42}, " +
               "packets={errors_received=42, received=42, retransmitted=42, rst_sent=42, sent=42}"));
    }

    @Test
    public void testCpu() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Short> expectedCpu = new HashMap<>(5);
        expectedCpu.put("system", (short) 0);
        expectedCpu.put("user", (short) 4);
        expectedCpu.put("idle", (short) 94);
        expectedCpu.put("used", (short) 4);
        expectedCpu.put("stolen", (short) 10);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat((long) v.get("uptime"), is(3600000L));
        assertThat(v.get("cpu"), Matchers.<Object>is(expectedCpu));
    }

    @Test
    public void testProcess() throws Exception {
        Reference refInfo = refInfo("sys.nodes.process", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> expectedCpu = new HashMap<>(4);
        expectedCpu.put("percent", (short) 50);
        expectedCpu.put("system", 1000L);
        expectedCpu.put("user", 500L);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat((long) v.get("open_file_descriptors"), is(42L));
        assertThat((long) v.get("max_open_file_descriptors"), is(1000L));
        assertThat(v.get("cpu"), Matchers.<Object>is(expectedCpu));
    }

    @Test
    public void testOsInfo() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os_info", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat((int) v.get("available_processors"), is(4));
    }

    @Test
    public void testNestedBytesRefExpressionsString() throws Exception {
        Reference refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);

        Map<String, Object> version = (Map<String, Object>) collectExpression.value();
        String versionNumber = (String) version.get("number");
        refInfo = refInfo("sys.nodes.version", DataTypes.STRING, RowGranularity.NODE, "number");

        collectExpression = (CollectExpression) resolver.getImplementation(refInfo);
        collectExpression.setNextRow(CONTEXT);
        assertThat(collectExpression.value(), Matchers.<Object>is(new BytesRef(versionNumber)));
    }
}
