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
package io.crate.operation.reference.sys;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.monitor.ZeroExtendedNodeInfo;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.node.local.NodeSysExpression;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.local.LocalDiscovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.MonitorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SysNodesExpressionsWithDefaultExtendedStatsTest extends CrateDummyClusterServiceUnitTest {

    private NodeSysExpression nodeExpression;
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
        nodeExpression = new NodeSysExpression(
            clusterService,
            monitorService,
            null,
            localDiscovery,
            THREAD_POOL,
            new ZeroExtendedNodeInfo()
        );
    }

    @After
    public void cleanUp() throws Exception {
        nodeEnvironment.close();
    }

    @Test
    public void testLoad() throws Exception {
        Reference refInfo = refInfo("sys.nodes.load", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression load =
            (io.crate.operation.reference.NestedObjectExpression) nodeExpression.getChildImplementation(refInfo.ident().columnIdent().name());
        Map<String, Object> loadValue = load.value();
        assertThat((Double) loadValue.get("1"), is(-1.0d));
        assertThat((Double) loadValue.get("5"), is(-1.0d));
        assertThat((Double) loadValue.get("15"), is(-1.0d));
    }

    @Test
    public void testFs() throws Exception {
        Reference refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression fs = (io.crate.operation.reference.NestedObjectExpression)
            nodeExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = fs.value();
        //noinspection unchecked
        assertThat(mapToSortedString((Map<String, Object>) v.get("total")),
            is("available=-1, bytes_read=-1, bytes_written=-1, reads=-1, size=-1, used=-1, writes=-1"));
        Object[] disks = (Object[]) v.get("disks");
        assertThat(disks.length, is(0));

        Object[] data = (Object[]) v.get("data");
        assertThat(data.length, is(0));
    }

    @Test
    public void testCpu() throws Exception {
        Reference refInfo = refInfo("sys.nodes.os", DataTypes.OBJECT, RowGranularity.NODE);
        io.crate.operation.reference.NestedObjectExpression os = (io.crate.operation.reference.NestedObjectExpression)
            nodeExpression.getChildImplementation(refInfo.ident().columnIdent().name());

        Map<String, Object> v = os.value();

        Map<String, Short> cpuObj = new HashMap<>(5);
        cpuObj.put("system", (short) -1);
        cpuObj.put("user", (short) -1);
        cpuObj.put("idle", (short) -1);
        cpuObj.put("used", (short) -1);
        cpuObj.put("stolen", (short) -1);
        assertEquals(cpuObj, v.get("cpu"));
    }

    @Test
    public void testFsDataOnNonDataNode() throws Exception {
        Reference refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE, "data");
        ColumnIdent columnIdent = refInfo.ident().columnIdent();
        NestedObjectExpression fs = (NestedObjectExpression)
            nodeExpression.getChildImplementation(columnIdent.name());
        assertThat(((Object[]) fs.getChildImplementation(columnIdent.path().get(0)).value()).length, is(0));
    }
}
