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

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.NestedReferenceResolver;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.node.*;
import io.crate.stats.DummyExtendedNodeStats;
import io.crate.stats.ExtendedNodeStats;
import io.crate.stats.StatsModule;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
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
import java.util.Map;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysNodesExpressionsWithDefaultExtendedStatsTest extends CrateUnitTest {

    private Injector injector;
    private NestedReferenceResolver resolver;

    private void prepare(boolean isDataNode) throws Exception {
        injector = new ModulesBuilder().add(
                new SysNodesExpressionsTest.TestModule(isDataNode),
                new StatsModule(Settings.EMPTY),
                new SysNodeExpressionModule()
        ).createInjector();
        resolver = new NodeSysReferenceResolver(injector.getInstance(NodeSysExpression.class));
    }

    @After
    public void cleanUp() throws Exception {
        if (injector != null) {
            injector.getInstance(ThreadPool.class).shutdownNow();
        }
    }

    @Test
    public void testLoad() throws Exception {
        prepare(true);
        ReferenceInfo refInfo = refInfo("sys.nodes.load", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression load = (NestedObjectExpression) resolver.getImplementation(refInfo);
        Map<String, Object> loadValue = load.value();
        assertThat((Double) loadValue.get("1"), is(-1.0d));
        assertThat((Double) loadValue.get("5"), is(-1.0d));
        assertThat((Double) loadValue.get("15"), is(-1.0d));
    }

    @Test
    public void testFs() throws Exception {
        prepare(true);
        ReferenceInfo refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE);
        NestedObjectExpression fs = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map<String, Object> v = fs.value();
        assertThat(mapToSortedString((Map<String, Object>) v.get("total")),
                is("available=-1, bytes_read=-1, bytes_written=-1, reads=-1, size=-1, used=-1, writes=-1"));
        Object[] disks = (Object[]) v.get("disks");
        assertThat(disks.length, is(0));

        Object[] data = (Object[]) v.get("data");
        assertThat(data.length, is(0));
    }

    @Test
    public void testFsDataOnNonDataNode() throws Exception {
        prepare(false);
        ReferenceInfo refInfo = refInfo("sys.nodes.fs", DataTypes.STRING, RowGranularity.NODE, "data");
        SysObjectArrayReference fs = (SysObjectArrayReference) resolver.getImplementation(refInfo);
        assertThat(fs.value().length, is(0));
    }
}
