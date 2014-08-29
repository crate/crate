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

import java.util.Map;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class ClusterSettingsTest extends SQLTransportIntegrationTest {

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
