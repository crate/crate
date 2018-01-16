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
package io.crate.execution.expression.reference.sys;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.execution.expression.reference.NestedObjectExpression;
import io.crate.execution.expression.reference.sys.node.local.NodeSysExpression;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.local.LocalDiscovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.fs.FsService;
import org.hamcrest.core.StringContains;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("unchecked")
public class SysNodesExpressionsWithDefaultExtendedStatsTest extends CrateDummyClusterServiceUnitTest {

    private NodeSysExpression nodeExpression;
    private NodeEnvironment nodeEnvironment;
    private FsService fsService;

    @Before
    public void prepare() throws Exception {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            /*
              storage cannot be enabled on master and data nodes
              see {@link (org.elasticsearch.cluster.node.DiscoveryNode).nodeRequiresLocalStorage()}
              used for {@link #testFsDataOnClientNode()} }
             */
            .put("node.local_storage", false)
            .put("node.data", false)
            .put("node.master", false)
            .build();
        LocalDiscovery localDiscovery = new LocalDiscovery(
            settings,
            clusterService,
            clusterService.getClusterSettings(),
            new NamedWriteableRegistry(Collections.emptyList()));
        Environment environment = new Environment(settings);
        nodeEnvironment = new NodeEnvironment(settings, environment);
        MonitorService monitorService = new MonitorService(settings, nodeEnvironment, THREAD_POOL);
        fsService = monitorService.fsService();
        nodeExpression = new NodeSysExpression(
            clusterService,
            monitorService,
            null,
            localDiscovery,
            THREAD_POOL,
            new ExtendedNodeInfo()
        );
    }

    @After
    public void cleanUp() throws Exception {
        nodeEnvironment.close();
    }

    @Test
    public void testLoad() throws Exception {
        Reference refInfo = refInfo("sys.nodes.load", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.execution.expression.reference.NestedObjectExpression load =
            (io.crate.execution.expression.reference.NestedObjectExpression) nodeExpression.getChildImplementation(refInfo.ident().columnIdent().name());
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
    }

    @Test
    public void testFs() throws Exception {
        boolean ioStatsAvailable = fsService.stats().getIoStats() != null;
        Reference refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE);
        io.crate.execution.expression.reference.NestedObjectExpression fs = (io.crate.execution.expression.reference.NestedObjectExpression)
            nodeExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = fs.value();
        Map<String, Object> total = (Map<String, Object>) v.get("total");
        // node is not a data node, that's why there are no disk stats
        assertThat(total.get("size"), is(-1L));
        assertThat(total.get("used"), is(-1L));
        assertThat(total.get("available"), is(-1L));
        if (ioStatsAvailable) {
            assertThat((long) total.get("reads"), greaterThanOrEqualTo(0L));
            assertThat((long) total.get("bytes_written"), greaterThanOrEqualTo(0L));
            assertThat((long) total.get("writes"), greaterThanOrEqualTo(0L));
            assertThat((long) total.get("bytes_written"), greaterThanOrEqualTo(0L));
        } else {
            /*
              reads/bytes_read/writes/bytes_written are -1 if the FsInfo.ioStats() probe is null
              This is the case if the probe cache has not been refreshed (default refresh interval is 1s).
              Unfortunately the probe cannot be forced to refresh.
             */
            assertThat(total.get("reads"), is(-1L));
            assertThat(total.get("bytes_written"), is(-1L));
            assertThat(total.get("writes"), is(-1L));
            assertThat(total.get("bytes_written"), is(-1L));
        }

        Object[] disks = (Object[]) v.get("disks");
        assertThat(disks.length, is(0));

        Object[] data = (Object[]) v.get("data");
        assertThat(data.length, is(0));
    }

    @Test
    public void testCpu() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.execution.expression.reference.NestedObjectExpression os = (io.crate.execution.expression.reference.NestedObjectExpression)
            nodeExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = os.value();
        if (Constants.LINUX) {
            assertThat((long) v.get("uptime"), greaterThan(1000L));
        }
        // Windows and macOS require a sys call for "uptime",
        // Sometimes syscalls work, sometimes not, e.g. starting tests with Powershell works
        // TODO: Figure out why. For now, just ignore other OSs than Linux
        // assertThat(v.get("uptime"), is(-1L));

        String cpu = mapToSortedString((Map<String, Object>) v.get("cpu"));
        assertThat(cpu, StringContains.containsString("idle=-1"));
        assertThat(cpu, StringContains.containsString("stolen=-1"));
        assertThat(cpu, StringContains.containsString("system=-1"));
        assertThat(cpu, StringContains.containsString("used="));
        assertThat(cpu, StringContains.containsString("user=-1"));

    }

    @Test
    public void testFsDataOnClientNode() throws Exception {
        Reference refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE, "data");
        ColumnIdent columnIdent = refInfo.ident().columnIdent();
        NestedObjectExpression fs = (NestedObjectExpression)
            nodeExpression.getChildImplementation(columnIdent.name());
        assertThat(((Object[]) fs.getChildImplementation(columnIdent.path().get(0)).value()).length, is(0));
    }
}
