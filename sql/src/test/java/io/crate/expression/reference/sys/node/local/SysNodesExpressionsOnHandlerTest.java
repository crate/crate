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

package io.crate.expression.reference.sys.node.local;

import io.crate.Build;
import io.crate.Version;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.expression.reference.sys.node.DummyStatsProvider;
import io.crate.expression.reference.sys.node.NodeStatsContext;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsService;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.StringContains.containsString;


/**
 * Tests for Node{Os,Fs,Mem,...}Expression which are used for sys.nodes table.
 */
@SuppressWarnings("unchecked")
public class SysNodesExpressionsOnHandlerTest extends CrateUnitTest {

    private final StaticTableReferenceResolver<NodeStatsContext> resolver =
        new StaticTableReferenceResolver<>(SysNodesTableInfo.expressions());
    private final NodeStatsContext context = new NodeStatsContext(true);

    private CollectExpression collectExpression;
    private DummyStatsProvider statsProvider;

    @Before
    public void setup() throws IOException {
        statsProvider = new DummyStatsProvider();
        context.id(BytesRefs.toBytesRef("93c7ff92-52fa-11e6-aad8-3c15c2d3ad18"));
        context.name(BytesRefs.toBytesRef("crate1"));
        context.hostname(BytesRefs.toBytesRef("crate1.example.com"));
        context.version(Version.CURRENT);
        context.build(Build.CURRENT);
        context.restUrl(BytesRefs.toBytesRef("10.0.0.1:4200"));
        context.port(new HashMap<String, Integer>(2) {{
            put("http", 4200);
            put("transport", 4300);
        }});
        context.timestamp(statsProvider.probeTimestamp());
        JvmService jvmService = new JvmService(Settings.EMPTY);
        context.jvmStats(jvmService.stats());
        OsService osService = new OsService(Settings.EMPTY);
        context.osInfo(osService.info());
        context.osStats(osService.stats());
        context.processStats(statsProvider.processStats());
        context.fsInfo(statsProvider.fsInfo());
        ExtendedNodeInfo extendedNodeInfo = new ExtendedNodeInfo();
        context.extendedOsStats(extendedNodeInfo.osStats());
        context.networkStats(extendedNodeInfo.networkStats());
    }

    @Test
    public void testLoad() throws Exception {
        Reference refInfo = refInfo("sys.nodes.load", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
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
    }

    @Test
    public void testName() throws Exception {
        Reference refInfo = refInfo("sys.nodes.name", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);
        assertThat(BytesRefs.toBytesRef("crate1"), is(collectExpression.value()));
    }

    @Test
    public void testId() throws Exception {
        Reference refInfo = refInfo("sys.nodes.id", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);
        assertThat(BytesRefs.toBytesRef("93c7ff92-52fa-11e6-aad8-3c15c2d3ad18"), is(collectExpression.value()));
    }

    @Test
    public void testHostname() throws Exception {
        Reference refInfo = refInfo("sys.nodes.hostname", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);
        assertThat(BytesRefs.toBytesRef("crate1.example.com"), is(collectExpression.value()));
    }

    @Test
    public void testRestUrl() throws Exception {
        Reference refInfo = refInfo("sys.nodes.rest_url", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);
        assertThat(BytesRefs.toBytesRef("10.0.0.1:4200"), is(collectExpression.value()));
    }

    @Test
    public void testPorts() throws Exception {
        Reference refInfo = refInfo("sys.nodes.port", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat(v.get("http"), is(4200));
        assertThat(v.get("transport"), is(4300));
    }

    @Test
    public void testMemory() throws Exception {
        Reference refInfo = refInfo("sys.nodes.mem", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat((long) v.get("free"), greaterThan(2L));
        assertThat(v.get("free_percent"), notNullValue());
        assertThat((long) v.get("used"), greaterThan(2L));
        assertThat(v.get("used_percent"), notNullValue());
    }

    @Test
    public void testHeap() throws Exception {
        Reference refInfo = refInfo("sys.nodes.heap", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        long max = (long) v.get("max");
        assertThat(max, greaterThan(2L));
        long used = (long) v.get("used");
        assertThat(used, greaterThan(2L));
        long free = (long) v.get("free");
        assertThat(free, greaterThan(2L));

        assertThat(max - used, is(free));
    }

    @Test
    public void testFs() throws Exception {
        Reference refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        Map<String, Object> total = (Map<String, Object>) v.get("total");
        assertThat(total.get("size"), is(2048L));
        assertThat(total.get("used"), is(1024L));
        assertThat(total.get("available"), is(1024L));
        if (statsProvider.hasIoStats()) {
            assertThat(total.get("reads"), is(2L));
            assertThat(total.get("bytes_written"), is(0L));
            assertThat(total.get("writes"), is(2L));
            assertThat(total.get("bytes_written"), is(0L));
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
            assertThat(((Map<String, Object>) disks[i]).get("size"), is(1024L));
        }

        Object[] data = (Object[]) v.get("data");
        assertEquals(data.length, disks.length);
        for (int i = 0; i < data.length; i++) {
            assertNotNull(((Map<String, Object>) data[i]).get("dev"));
            assertNotNull(((Map<String, Object>) data[i]).get("path"));
        }

        refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE, "data", "dev");
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);
        for (Object arrayElement : (Object[]) collectExpression.value()) {
            assertThat(arrayElement, instanceOf(BytesRef.class));
        }
    }

    @Test
    public void testVersion() throws Exception {
        Reference refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat(v.get("number"), is(Version.CURRENT.number()));
        assertThat(v.get("build_hash"), is(Build.CURRENT.hash()));
        assertThat(v.get("build_snapshot"), is(Version.CURRENT.snapshot()));
    }

    @Test
    public void testNetwork() throws Exception {
        Reference refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> networkStats = (Map<String, Object>) collectExpression.value();
        assertThat(mapToSortedString(networkStats),
            is("probe_timestamp=0, tcp={" +
               "connections={accepted=0, curr_established=0, dropped=0, embryonic_dropped=0, initiated=0}, " +
               "packets={errors_received=0, received=0, retransmitted=0, rst_sent=0, sent=0}" +
               "}"));
    }

    @Test
    public void testNetworkTCP() throws Exception {
        Reference refInfo = refInfo("sys.nodes.network", DataTypes.OBJECT, RowGranularity.NODE, "tcp");
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);
        Map<String, Object> tcpStats = (Map<String, Object>) collectExpression.value();

        assertThat(tcpStats, instanceOf(Map.class));
        assertThat(mapToSortedString(tcpStats),
            is("connections={accepted=0, curr_established=0, dropped=0, embryonic_dropped=0, initiated=0}, " +
               "packets={errors_received=0, received=0, retransmitted=0, rst_sent=0, sent=0}"));
    }

    @Test
    public void testCpu() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
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
    public void testProcess() throws Exception {
        Reference refInfo = refInfo("sys.nodes.process", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat(v.get("open_file_descriptors"), is(1L));
        assertThat(v.get("max_open_file_descriptors"), is(2L));
        Map<String, Object> cpu = (Map<String, Object>) v.get("cpu");
        assertThat(cpu.get("percent"), is((short) 50));
        assertThat(cpu.get("system"), is(-1L));
        assertThat(cpu.get("user"), is(-1L));
    }

    @Test
    public void testOsInfo() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os_info", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> v = (Map<String, Object>) collectExpression.value();
        assertThat(v.get("available_processors"), is(EsExecutors.boundedNumberOfProcessors(Settings.EMPTY)));
    }

    @Test
    public void testNestedBytesRefExpressionsString() throws Exception {
        Reference refInfo = refInfo("sys.nodes.version", DataTypes.OBJECT, RowGranularity.NODE);
        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);

        Map<String, Object> version = (Map<String, Object>) collectExpression.value();
        String versionNumber = (String) version.get("number");
        refInfo = refInfo("sys.nodes.version", DataTypes.STRING, RowGranularity.NODE, "number");

        collectExpression = resolver.getImplementation(refInfo);
        collectExpression.setNextRow(context);
        assertThat(collectExpression.value(), Matchers.is(new BytesRef(versionNumber)));
    }
}
