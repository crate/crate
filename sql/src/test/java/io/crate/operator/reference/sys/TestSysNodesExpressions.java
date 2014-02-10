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
package io.crate.operator.reference.sys;

import io.crate.metadata.GlobalReferenceResolver;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operator.Input;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.node.service.NodeService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Map;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSysNodesExpressions {

    private Injector injector;
    private ReferenceResolver resolver;

    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            OsService osService = mock(OsService.class);
            OsStats osStats = mock(OsStats.class);
            when(osService.stats()).thenReturn(osStats);
            when(osStats.loadAverage()).thenReturn(new double[]{1, 5, 15});

            ByteSizeValue byteSizeValue = mock(ByteSizeValue.class);
            when(byteSizeValue.bytes()).thenReturn(12345342234L);
            when(byteSizeValue.toString()).thenReturn("11.4gb");

            OsStats.Mem mem = mock(OsStats.Mem.class);
            when(osStats.mem()).thenReturn(mem);
            when(mem.free()).thenReturn(byteSizeValue);
            when(mem.used()).thenReturn(byteSizeValue);
            when(mem.usedPercent()).thenReturn((short) 22);
            when(mem.freePercent()).thenReturn((short) 78);

            bind(OsService.class).toInstance(osService);

            NodeService nodeService = mock(NodeService.class);
            NodeStats nodeStats = mock(NodeStats.class);
            when(nodeService.stats()).thenReturn(nodeStats);
            when(nodeStats.getHostname()).thenReturn("localhost");

            DiscoveryNode node = mock(DiscoveryNode.class);
            TransportAddress transportAddress = new InetSocketTransportAddress("localhost", 44300);

            when(nodeStats.getNode()).thenReturn(node);
            when(node.getId()).thenReturn("node-id-1");
            when(node.getName()).thenReturn("node 1");
            when(node.address()).thenReturn(transportAddress);

            FsStats fsStats = mock(FsStats.class);
            when(nodeStats.getFs()).thenReturn(fsStats);

            FsStats.Info fsInfo = mock(FsStats.Info.class);

            ByteSizeValue byteSizeValueFree = mock(ByteSizeValue.class);
            when(byteSizeValueFree.bytes()).thenReturn(12345342234L);

            ByteSizeValue byteSizeValueTotal = mock(ByteSizeValue.class);
            when(byteSizeValueTotal.bytes()).thenReturn(42345342234L);

            when(fsInfo.getFree()).thenReturn(byteSizeValueFree);
            when(fsInfo.getTotal()).thenReturn(byteSizeValueTotal);

            final FsStats.Info[] infos = new FsStats.Info[]{fsInfo, fsInfo};
            when(fsStats.iterator()).then(new Answer<Object>() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    return Arrays.asList(infos).iterator();
                }
            });

            bind(NodeService.class).toInstance(nodeService);
            HttpServer httpServer = mock(HttpServer.class);
            HttpInfo httpInfo = mock(HttpInfo.class);
            BoundTransportAddress boundTransportAddress = new BoundTransportAddress(
                    new InetSocketTransportAddress("localhost", 44200),
                    new InetSocketTransportAddress("localhost", 44200)
            );
            when(httpInfo.address()).thenReturn(boundTransportAddress);
            when(httpServer.info()).thenReturn(httpInfo);
            bind(HttpServer.class).toInstance(httpServer);

            bind(ReferenceResolver.class).to(GlobalReferenceResolver.class).asEagerSingleton();
        }
    }

    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder().add(
                new TestModule(),
                new SysNodeExpressionModule()
        ).createInjector();
        resolver = injector.getInstance(ReferenceResolver.class);

    }

    @Test
    public void testLoad() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "load");
        SysObjectReference<Double> load = (SysObjectReference<Double>) resolver.getImplementation(ident);

        Map<String, Double> v = load.value();
        assertNull(v.get("something"));
        assertEquals(new Double(1), v.get("1"));
        assertEquals(new Double(5), v.get("5"));
        assertEquals(new Double(15), v.get("15"));

        Input<Double> ci = load.getChildImplementation("1");
        assertEquals(new Double(1), ci.value());
        ci = load.getChildImplementation("5");
        assertEquals(new Double(5), ci.value());
        ci = load.getChildImplementation("15");
        assertEquals(new Double(15), ci.value());

    }

    @Test
    public void testName() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "name");
        SysExpression<String> name = (SysExpression<String>) resolver.getImplementation(ident);

        assertEquals("node 1", name.value());
    }

    @Test
    public void testId() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "id");
        SysExpression<String> id = (SysExpression<String>) resolver.getImplementation(ident);

        assertEquals("node-id-1", id.value());
    }

    @Test
    public void testHostname() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "hostname");
        SysExpression<String> hostname = (SysExpression<String>) resolver.getImplementation(ident);

        assertEquals("localhost", hostname.value());
    }

    @Test
    public void testPorts() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "port");
        SysObjectReference<Integer> port = (SysObjectReference<Integer>) resolver.getImplementation(ident);

        Map<String, Integer> v = port.value();
        assertEquals(new Integer(44200), v.get("http"));
        assertEquals(new Integer(44300), v.get("transport"));
    }

    @Test
    public void testMemory() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "mem");
        SysObjectReference<Object> mem = (SysObjectReference<Object>) resolver.getImplementation(ident);

        Map<String, Object> v = mem.value();

        assertEquals(12345342234L, v.get("free"));
        assertEquals(new Short("78"), v.get("free_percent"));

        assertEquals(12345342234L, v.get("used"));
        assertEquals(new Short("22"), v.get("used_percent"));
    }

    @Test
    public void testFs() throws Exception {

        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "fs");
        SysObjectReference<Object> fs = (SysObjectReference<Object>) resolver.getImplementation(ident);

        Map<String, Object> v = fs.value();

        assertEquals(84690684468L, v.get("total"));
        assertEquals(24690684468L, v.get("free"));
        assertEquals(60000000000L, v.get("used"));

        assertEquals(29.15395550655783, v.get("free_percent"));
        assertEquals(70.84604449344218, v.get("used_percent"));
    }

}
