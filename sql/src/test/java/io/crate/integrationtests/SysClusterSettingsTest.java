/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.integrationtests;

import io.crate.metadata.settings.CrateSettings;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Map;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.TEST)
public class SysClusterSettingsTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testSetResetGlobalSetting() throws Exception {
        execute("set global persistent stats.enabled = true");
        execute("select settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount(), Matchers.is(1L));
        assertThat((Boolean)response.rows()[0][0], Matchers.is(true));

        execute("reset global stats.enabled");
        execute("select settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount(), Matchers.is(1L));
        assertThat((Boolean)response.rows()[0][0], Matchers.is(false));

        execute("set global transient stats = { enabled = true, jobs_log_size = 3, operations_log_size = 4 }");
        execute("select settings['stats']['enabled'], settings['stats']['jobs_log_size']," +
                "settings['stats']['operations_log_size'] from sys.cluster");
        assertThat(response.rowCount(), Matchers.is(1L));
        assertThat((Boolean)response.rows()[0][0], Matchers.is(true));
        assertThat((Integer)response.rows()[0][1], Matchers.is(3));
        assertThat((Integer)response.rows()[0][2], Matchers.is(4));

        execute("reset global stats");
        execute("select settings['stats']['enabled'], settings['stats']['jobs_log_size']," +
                "settings['stats']['operations_log_size'] from sys.cluster");
        assertThat(response.rowCount(), Matchers.is(1L));
        assertThat((Boolean)response.rows()[0][0], Matchers.is(false));
        assertThat((Integer)response.rows()[0][1], Matchers.is(10_000));
        assertThat((Integer)response.rows()[0][2], Matchers.is(10_000));
    }

    @Test
    public void testDynamicTransientSettings() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 1)
                .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 2)
                .put(CrateSettings.STATS_ENABLED.settingName(), false);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(builder.build()).execute().actionGet();

        execute("select settings from sys.cluster");
        assertEquals(1L, response.rowCount());
        Map<String, Map> settings = (Map<String, Map>)response.rows()[0][0];
        Map stats = settings.get(CrateSettings.STATS.name());
        assertEquals(1, stats.get(CrateSettings.STATS_JOBS_LOG_SIZE.name()));
        assertEquals(2, stats.get(CrateSettings.STATS_OPERATIONS_LOG_SIZE.name()));
        assertEquals(false, stats.get(CrateSettings.STATS_ENABLED.name()));

        cluster().fullRestart();
        ensureGreen();

        execute("select settings from sys.cluster");
        assertEquals(1L, response.rowCount());
        settings = (Map<String, Map>)response.rows()[0][0];
        stats = settings.get(CrateSettings.STATS.name());
        assertEquals(CrateSettings.STATS_JOBS_LOG_SIZE.defaultValue(),
                stats.get(CrateSettings.STATS_JOBS_LOG_SIZE.name()));
        assertEquals(CrateSettings.STATS_OPERATIONS_LOG_SIZE.defaultValue(),
                stats.get(CrateSettings.STATS_OPERATIONS_LOG_SIZE.name()));
        assertEquals(CrateSettings.STATS_ENABLED.defaultValue(),
                stats.get(CrateSettings.STATS_ENABLED.name()));
    }

    @Test
    public void testDynamicPersistentSettings() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 1)
                .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 2)
                .put(CrateSettings.STATS_ENABLED.settingName(), false);
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(builder.build()).execute().actionGet();

        execute("select settings from sys.cluster");
        assertEquals(1L, response.rowCount());
        Map<String, Map> settings = (Map<String, Map>)response.rows()[0][0];
        Map stats = settings.get(CrateSettings.STATS.name());
        assertEquals(1, stats.get(CrateSettings.STATS_JOBS_LOG_SIZE.name()));
        assertEquals(2, stats.get(CrateSettings.STATS_OPERATIONS_LOG_SIZE.name()));
        assertEquals(false, stats.get(CrateSettings.STATS_ENABLED.name()));

        cluster().fullRestart();
        ensureGreen();

        execute("select settings from sys.cluster");
        assertEquals(1L, response.rowCount());
        settings = (Map<String, Map>)response.rows()[0][0];
        stats = settings.get(CrateSettings.STATS.name());
        assertEquals(1, stats.get(CrateSettings.STATS_JOBS_LOG_SIZE.name()));
        assertEquals(2, stats.get(CrateSettings.STATS_OPERATIONS_LOG_SIZE.name()));
        assertEquals(false, stats.get(CrateSettings.STATS_ENABLED.name()));
    }
}
