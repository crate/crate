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
                .put(CrateSettings.JOBS_LOG_SIZE.settingName(), 1)
                .put(CrateSettings.OPERATIONS_LOG_SIZE.settingName(), 2)
                .put(CrateSettings.COLLECT_STATS.settingName(), false);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(builder.build()).execute().actionGet();

        execute("select settings from sys.cluster");
        assertEquals(1L, response.rowCount());
        Map settings = (Map)response.rows()[0][0];
        assertEquals(1, settings.get(CrateSettings.JOBS_LOG_SIZE.name()));
        assertEquals(2, settings.get(CrateSettings.OPERATIONS_LOG_SIZE.name()));
        assertEquals(false, settings.get(CrateSettings.COLLECT_STATS.name()));

        cluster().fullRestart();
        ensureGreen();

        execute("select settings from sys.cluster");
        assertEquals(1L, response.rowCount());
        settings = (Map)response.rows()[0][0];
        assertEquals(CrateSettings.JOBS_LOG_SIZE.defaultValue(),
                settings.get(CrateSettings.JOBS_LOG_SIZE.name()));
        assertEquals(CrateSettings.OPERATIONS_LOG_SIZE.defaultValue(),
                settings.get(CrateSettings.OPERATIONS_LOG_SIZE.name()));
        assertEquals(CrateSettings.COLLECT_STATS.defaultValue(),
                settings.get(CrateSettings.COLLECT_STATS.name()));
    }

    public void testDynamicPersistentSettings() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put(CrateSettings.JOBS_LOG_SIZE.settingName(), 1)
                .put(CrateSettings.OPERATIONS_LOG_SIZE.settingName(), 2)
                .put(CrateSettings.COLLECT_STATS.settingName(), false);
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(builder.build()).execute().actionGet();

        execute("select settings from sys.cluster");
        assertEquals(1L, response.rowCount());
        Map settings = (Map)response.rows()[0][0];
        assertEquals(1, settings.get(CrateSettings.JOBS_LOG_SIZE.name()));
        assertEquals(2, settings.get(CrateSettings.OPERATIONS_LOG_SIZE.name()));
        assertEquals(false, settings.get(CrateSettings.COLLECT_STATS.name()));

        cluster().fullRestart();
        ensureGreen();

        execute("select settings from sys.cluster");
        assertEquals(1L, response.rowCount());
        settings = (Map)response.rows()[0][0];
        assertEquals(1, settings.get(CrateSettings.JOBS_LOG_SIZE.name()));
        assertEquals(2, settings.get(CrateSettings.OPERATIONS_LOG_SIZE.name()));
        assertEquals(false, settings.get(CrateSettings.COLLECT_STATS.name()));
    }

}
