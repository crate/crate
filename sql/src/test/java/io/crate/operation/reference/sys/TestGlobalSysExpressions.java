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
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.Input;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.operation.reference.sys.node.NodeLoadExpression;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.node.service.NodeService;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGlobalSysExpressions {

    private Injector injector;
    private ReferenceResolver resolver;
    private ReferenceInfos referenceInfos;

    private static final ReferenceInfo LOAD_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load"));
    private static final ReferenceInfo LOAD1_INFO = SysNodesTableInfo.INFOS.get(new ColumnIdent("load", "1"));

    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            OsService osService = mock(OsService.class);
            OsStats osStats = mock(OsStats.class);
            when(osService.stats()).thenReturn(osStats);
            when(osStats.loadAverage()).thenReturn(new double[]{1, 5, 15});
            bind(OsService.class).toInstance(osService);

            NodeService nodeService = mock(NodeService.class);
            bind(NodeService.class).toInstance(nodeService);

            ClusterService clusterService = mock(ClusterService.class);
            bind(ClusterService.class).toInstance(clusterService);

            MapBinder<ReferenceIdent, ReferenceImplementation> b = MapBinder
                    .newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
            b.addBinding(SysNodesTableInfo.INFOS.get(new ColumnIdent("load")).ident()).to(
                    NodeLoadExpression.class).asEagerSingleton();

            b.addBinding(SysClusterTableInfo.INFOS.get(new ColumnIdent("settings")).ident()).to(
                    ClusterSettingsExpression.class).asEagerSingleton();
        }
    }


    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder().add(
                new TestModule(),
                new MetaDataModule(),
                new MetaDataSysModule()
        ).createInjector();
        resolver = injector.getInstance(ReferenceResolver.class);
        referenceInfos = injector.getInstance(ReferenceInfos.class);
    }

    @Test
    public void testWrongSchema() throws Exception {
        // unsupported schema
        ReferenceIdent ident = new ReferenceIdent(new TableIdent("something", "sometable"), "somecolumn");
        assertNull(referenceInfos.getReferenceInfo(ident));

    }


    @Test
    public void testInfoLookup() throws Exception {

        ReferenceIdent ident = LOAD_INFO.ident();
        assertEquals(LOAD_INFO, referenceInfos.getReferenceInfo(ident));

        ident = LOAD1_INFO.ident();
        assertEquals(referenceInfos.getReferenceInfo(ident), LOAD1_INFO);

    }

    @Test
    public void testChildImplementationLookup() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysNodesTableInfo.IDENT, "load");
        SysObjectReference<Double> load = (SysObjectReference<Double>) resolver.getImplementation(ident);
        assertEquals(LOAD_INFO, load.info());

        Input<Double> ci = load.getChildImplementation("1");
        assertEquals(new Double(1), ci.value());

        ident = LOAD1_INFO.ident();
        SysExpression<Double> l1 = (SysExpression<Double>) resolver.getImplementation(ident);
        assertEquals(LOAD1_INFO, l1.info());
    }

    @Test
    public void testClusterSettings() throws Exception {
        ReferenceIdent ident = new ReferenceIdent(SysClusterTableInfo.IDENT, ClusterSettingsExpression.NAME);
        SysObjectReference<Object> settingsExpression = (SysObjectReference<Object>) resolver.getImplementation(ident);

        Map<String, Object> settings = settingsExpression.value();
        assertEquals(String.valueOf(0), settings.get(ClusterSettingsExpression.JOBS_LOG_SIZE));
        assertEquals(String.valueOf(0), settings.get(ClusterSettingsExpression.OPERATIONS_LOG_SIZE));
    }

}
