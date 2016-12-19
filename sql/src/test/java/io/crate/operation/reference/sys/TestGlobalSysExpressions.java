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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys;


import io.crate.metadata.*;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.monitor.DummyExtendedNodeInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.operation.Input;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.operation.reference.sys.node.local.NodeLoadExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGlobalSysExpressions extends CrateUnitTest {

    private Injector injector;
    private NestedReferenceResolver resolver;
    private Schemas schemas;

    private Reference loadInfo;
    private Reference load1Info;
    private ThreadPool threadPool;


    @Before
    public void prepare() throws Exception {
        threadPool = new TestThreadPool("testing");
        injector = new ModulesBuilder().add(
            new TestModule(),
            new MetaDataModule(),
            new MetaDataSysModule()
        ).createInjector();
        resolver = injector.getInstance(NestedReferenceResolver.class);
        schemas = injector.getInstance(Schemas.class);
        loadInfo = schemas.getTableInfo(SysNodesTableInfo.IDENT).getReference(new ColumnIdent("load"));
        load1Info = schemas.getTableInfo(SysNodesTableInfo.IDENT).getReference(new ColumnIdent("load", "1"));
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }


    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Settings.class).toInstance(Settings.EMPTY);
            bind(ThreadPool.class).toInstance(threadPool);

            OsService osService = mock(OsService.class);
            OsStats osStats = mock(OsStats.class);
            when(osService.stats()).thenReturn(osStats);
            bind(OsService.class).toInstance(osService);

            NodeService nodeService = mock(NodeService.class);
            bind(NodeService.class).toInstance(nodeService);

            ClusterService clusterService = mock(ClusterService.class);
            ClusterState state = mock(ClusterState.class);
            MetaData metaData = mock(MetaData.class);
            when(metaData.getConcreteAllOpenIndices()).thenReturn(new String[0]);
            when(metaData.templates()).thenReturn(ImmutableOpenMap.<String, IndexTemplateMetaData>of());
            when(metaData.settings()).thenReturn(Settings.EMPTY);
            when(state.metaData()).thenReturn(metaData);
            when(clusterService.state()).thenReturn(state);
            bind(ClusterService.class).toInstance(clusterService);
            bind(TransportPutIndexTemplateAction.class).toInstance(mock(TransportPutIndexTemplateAction.class));

            NodeEnvironment nodeEnvironment = mock(NodeEnvironment.class);
            when(nodeEnvironment.hasNodeFile()).thenReturn(true);
            ExtendedNodeInfo extendedNodeInfo = new DummyExtendedNodeInfo(nodeEnvironment);
            NodeLoadExpression loadExpr = new NodeLoadExpression(extendedNodeInfo.osStats());

            MapBinder<ReferenceIdent, ReferenceImplementation> b = MapBinder
                .newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
            b.addBinding(new ReferenceIdent(SysNodesTableInfo.IDENT, "load")).toInstance(loadExpr);

            b.addBinding(new ReferenceIdent(SysClusterTableInfo.IDENT, new ColumnIdent(ClusterSettingsExpression.NAME))).to(
                ClusterSettingsExpression.class).asEagerSingleton();
        }
    }

    @Test
    public void testInfoLookup() throws Exception {
        ReferenceIdent ident = loadInfo.ident();
        TableInfo sysNodesTableInfo = schemas.getTableInfo(SysNodesTableInfo.IDENT);
        assertEquals(loadInfo, sysNodesTableInfo.getReference(ident.columnIdent()));

        ident = load1Info.ident();
        assertEquals(sysNodesTableInfo.getReference(ident.columnIdent()), load1Info);
    }


    @Test
    public void testChildImplementationLookup() throws Exception {
        Reference refInfo = refInfo("sys.nodes.load", DataTypes.OBJECT, RowGranularity.NODE);
        NestedObjectExpression load = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Input ci = load.getChildImplementation("1");
        assertEquals(1D, ci.value());

        SimpleObjectExpression<Double> l1 = (SimpleObjectExpression<Double>) resolver.getImplementation(load1Info);
        assertTrue(ci == l1);
    }

    @Test
    public void testClusterSettings() throws Exception {
        Reference refInfo = refInfo("sys.cluster.settings", DataTypes.OBJECT, RowGranularity.CLUSTER);
        NestedObjectExpression settingsExpression = (NestedObjectExpression) resolver.getImplementation(refInfo);

        Map settings = settingsExpression.value();

        Map stats = (Map) settings.get(CrateSettings.STATS.name());
        assertEquals(CrateSettings.STATS_ENABLED.defaultValue(),
            stats.get(CrateSettings.STATS_ENABLED.name()));
        assertEquals(CrateSettings.STATS_JOBS_LOG_SIZE.defaultValue(),
            stats.get(CrateSettings.STATS_JOBS_LOG_SIZE.name()));
        assertEquals(CrateSettings.STATS_OPERATIONS_LOG_SIZE.defaultValue(),
            stats.get(CrateSettings.STATS_OPERATIONS_LOG_SIZE.name()));

        Map cluster = (Map) settings.get(CrateSettings.CLUSTER.name());
        Map gracefulStop = (Map) cluster.get(CrateSettings.GRACEFUL_STOP.name());
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.name()),
            is((Object) CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.defaultValue()));
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_REALLOCATE.name()),
            is((Object) CrateSettings.GRACEFUL_STOP_REALLOCATE.defaultValue()));
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_TIMEOUT.name()),
            is((Object) CrateSettings.GRACEFUL_STOP_TIMEOUT.defaultValue().toString())
        );
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_FORCE.name()),
            is((Object) CrateSettings.GRACEFUL_STOP_FORCE.defaultValue())
        );
        assertThat(
            gracefulStop.get(CrateSettings.GRACEFUL_STOP_TIMEOUT.name()),
            is((Object) CrateSettings.GRACEFUL_STOP_TIMEOUT.defaultValue().toString())
        );
        Map routing = (Map) cluster.get(CrateSettings.ROUTING.name());
        Map routingAllocation = (Map) routing.get(CrateSettings.ROUTING_ALLOCATION.name());
        assertThat(
            routingAllocation.get(CrateSettings.ROUTING_ALLOCATION_ENABLE.name()),
            is((Object) CrateSettings.ROUTING_ALLOCATION_ENABLE.defaultValue())
        );

        Map gateway = (Map) settings.get(CrateSettings.GATEWAY.name());
        assertThat(gateway.get(CrateSettings.GATEWAY_RECOVER_AFTER_TIME.name()),
            is((Object) CrateSettings.GATEWAY_RECOVER_AFTER_TIME.defaultValue().toString()));
        assertEquals(gateway.get(CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.name()),
            CrateSettings.GATEWAY_RECOVERY_AFTER_NODES.defaultValue());
        assertEquals(gateway.get(CrateSettings.GATEWAY_EXPECTED_NODES.name()),
            CrateSettings.GATEWAY_EXPECTED_NODES.defaultValue());
    }
}
