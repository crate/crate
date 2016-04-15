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

package io.crate.monitor;

import io.crate.module.SigarModule;
import io.crate.plugin.SigarPlugin;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SigarExtendedNodeInfoTest extends CrateUnitTest {

    private static final Settings NODE_SETTINGS = Settings.builder()
            .put(MonitorModule.NODE_INFO_EXTENDED_TYPE, SigarPlugin.NODE_INFO_EXTENDED_TYPE)
            .build();

    private ExtendedNodeInfo extendedNodeInfo;

    @Before
    public void prepare() throws Exception {
        MonitorModule monitorModule = new MonitorModule(NODE_SETTINGS);
        monitorModule.addExtendedNodeInfoType(SigarPlugin.NODE_INFO_EXTENDED_TYPE, SigarExtendedNodeInfo.class);
        Injector injector = new ModulesBuilder().add(
                new SettingsModule(NODE_SETTINGS),
                monitorModule,
                new SigarModule(new SigarService(NODE_SETTINGS))
        ).createInjector();
        extendedNodeInfo = injector.getInstance(ExtendedNodeInfo.class);
    }

    @Test
    public void testNetworkStats() throws Exception {
        ExtendedNetworkStats stats = extendedNodeInfo.networkStats();
        assertThat(stats.timestamp(), greaterThan(0L));
        assertThat(stats.tcp().activeOpens(), greaterThan(0L));
    }

    @Test
    public void testNetworkInfo() throws Exception {
        ExtendedNetworkInfo info = extendedNodeInfo.networkInfo();
        assertThat(info.primaryInterface().name(), notNullValue());
        assertThat(info.primaryInterface().name().length(), greaterThan(0));
    }

    @Test
    public void testFsStats() throws Exception {
        NodeEnvironment nodeEnvironment = mock(NodeEnvironment.class);
        when(nodeEnvironment.hasNodeFile()).thenReturn(true);
        Path tempDir = createTempDir();
        NodeEnvironment.NodePath[] dataLocations = new NodeEnvironment.NodePath[]{new NodeEnvironment.NodePath(tempDir, mock(Environment.class))};
        when(nodeEnvironment.nodePaths()).thenReturn(dataLocations);

        ExtendedFsStats stats = extendedNodeInfo.fsStats(nodeEnvironment);
        assertThat(stats.size(), is(1));
        ExtendedFsStats.Info info = stats.iterator().next();
        assertThat(info.path(), notNullValue());
        assertThat(info.dev(), notNullValue());
        assertThat(info.total(), greaterThan(-1L));
        assertThat(info.free(), greaterThan(-1L));
        assertThat(info.available(), greaterThan(-1L));
        assertThat(info.used(), greaterThan(-1L));
    }

    @Test
    public void testOsStats() throws Exception {
        ExtendedOsStats stats = extendedNodeInfo.osStats();
        assertThat(stats.timestamp(), greaterThan(0L));
        assertThat(stats.uptime().millis(), greaterThan(0L));
        ExtendedOsStats.Cpu cpu = stats.cpu();
        assertThat(cpu.sys(), greaterThan((short) 0));
    }

    @Test
    public void testOsInfo() throws Exception {
        ExtendedOsInfo info = extendedNodeInfo.osInfo();
        assertThat(info.kernelData().size(), greaterThan(0));
    }

    @Test
    public void testProcessCpuStats() throws Exception {
        ExtendedProcessCpuStats stats = extendedNodeInfo.processCpuStats();
        assertThat(stats.percent(), greaterThan((short) -1));
        assertThat(stats.sys().millis(), greaterThan(-1L));
    }
}
