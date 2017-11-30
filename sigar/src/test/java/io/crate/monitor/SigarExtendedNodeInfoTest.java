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

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

public class SigarExtendedNodeInfoTest extends CrateUnitTest {

    private ExtendedNodeInfo extendedNodeInfo;
    private NodeEnvironment nodeEnvironment;

    @Before
    public void prepare() throws Exception {
        Path tmpHome = createTempDir();
        Settings settings = Settings.builder()
            .put("path.home", tmpHome.toString())
            .build();
        nodeEnvironment = new NodeEnvironment(settings, new Environment(settings));
        extendedNodeInfo = new SigarExtendedNodeInfo(
            new SigarService(settings),
            nodeEnvironment
        );
    }

    @After
    public void cleanup() throws Exception {
        nodeEnvironment.close();
    }

    @Test
    public void testNetworkStats() throws Exception {
        ExtendedNetworkStats stats = extendedNodeInfo.networkStats();
        assertThat(stats.timestamp(), greaterThan(0L));
        if (isNewMacOSX()) {
            assertThat(stats.tcp().currEstab(), greaterThan(0L));
        } else {
            assertThat(stats.tcp().activeOpens(), greaterThan(0L));
        }
    }

    @Test
    public void testNetworkInfo() throws Exception {
        ExtendedNetworkInfo info = extendedNodeInfo.networkInfo();
        assertThat(info.primaryInterface().name(), notNullValue());
        assertThat(info.primaryInterface().name().length(), greaterThan(0));
    }

    @Test
    public void testFsStats() throws Exception {
        ExtendedFsStats stats = extendedNodeInfo.fsStats();
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
        assertThat(cpu.sys(), greaterThan((short) -1));
    }

    @Test
    public void testOsStatsCache() throws Exception {
        /*
         * get 2 osStats until we have probes with identical timestamps
         * then wait for the cache to time out and fetch a new osStats again
         * the new stats object must have a probe timestamp that is greater/equal than old probe timestamp + cache time
         */
        ExtendedOsStats statsOld = extendedNodeInfo.osStats();
        ExtendedOsStats statsNew = extendedNodeInfo.osStats();
        // the loop is only for the edge case where we get 2 different probes
        while (statsNew.timestamp() != statsOld.timestamp()) {
            statsOld = extendedNodeInfo.osStats();
            statsNew = extendedNodeInfo.osStats();
        }
        assertEquals(statsOld, statsNew);
        long cacheTime = SigarExtendedNodeInfo.PROBE_CACHE_TIME.millis();
        Thread.sleep(cacheTime + 100L);
        statsNew = extendedNodeInfo.osStats();
        assertTrue(statsNew.timestamp() - statsOld.timestamp() >= cacheTime);
    }

    @Test
    public void testOsInfo() throws Exception {
        ExtendedOsInfo info = extendedNodeInfo.osInfo();
        assertThat(info.kernelData().size(), greaterThan(0));
    }

    @Test
    public void testProcessCpuStats() throws Exception {
        assertBusy(() -> {
            ExtendedProcessCpuStats stats = extendedNodeInfo.processCpuStats();
            // anything else than the default values
            assertThat(stats.percent(), not((short) -1));
            assertThat(stats.sys().millis(), not(-1L));
        });
    }
}
