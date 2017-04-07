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

package io.crate.operation.reference.sys.cluster;

import io.crate.metadata.settings.CrateSettings;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ApplySettingsTest extends CrateUnitTest {

    @Test
    public void testOnRefreshSettings() throws Exception {

        ConcurrentHashMap<String, Object> values = new ConcurrentHashMap<>();
        ClusterSettingsExpression.ApplySettings applySettings = new ClusterSettingsExpression.ApplySettings(Settings.EMPTY, values);

        Settings.Builder builder = Settings.builder()
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 1)
            .put(CrateSettings.STATS_ENABLED.settingName(), false)
            .put(CrateSettings.STATS_SERVICE_INTERVAL.settingName(), "2h")
            .put(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.settingName(), "full")
            .put(CrateSettings.GRACEFUL_STOP_TIMEOUT.settingName(), "1m")
            .put(CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.settingName(), 2);
        Settings settings = builder.build();
        applySettings.onRefreshSettings(settings);

        String name = CrateSettings.STATS_JOBS_LOG_SIZE.settingName();
        assertEquals(values.get(name), settings.getAsInt(name, 0));

        name = CrateSettings.STATS_ENABLED.settingName();
        assertEquals(values.get(name), settings.getAsBoolean(name, true));

        name = CrateSettings.STATS_SERVICE_INTERVAL.settingName();
        assertEquals(values.get(name), settings.get(name, "2h"));

        name = CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.settingName();
        assertEquals(values.get(name), settings.get(name, "none"));

        name = CrateSettings.GRACEFUL_STOP_TIMEOUT.settingName();
        assertEquals(values.get(name), settings.get(name, "1h"));

        name = CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.settingName();
        assertEquals(values.get(name), settings.getAsInt(name, 2));

    }

    @Test
    public void testOnRefreshSettingsWithInitial() throws Exception {

        ConcurrentHashMap<String, Object> values = new ConcurrentHashMap<>();
        values.put(CrateSettings.BULK_REQUEST_TIMEOUT.settingName(), CrateSettings.BULK_REQUEST_TIMEOUT.defaultValue());
        Settings initialSettings = Settings.builder()
            .put(CrateSettings.BULK_REQUEST_TIMEOUT.settingName(), 10L, TimeUnit.SECONDS)
            .build();
        ClusterSettingsExpression.ApplySettings applySettings = new ClusterSettingsExpression.ApplySettings(initialSettings, values);

        Settings.Builder builder = Settings.builder()
            .put(CrateSettings.BULK_REQUEST_TIMEOUT.settingName(), 1L, TimeUnit.SECONDS)
            .put(CrateSettings.STATS_ENABLED.settingName(), false)
            .put(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.settingName(), "full")
            .put(CrateSettings.GRACEFUL_STOP_TIMEOUT.settingName(), "1m")
            .put(CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.settingName(), 2);
        Settings settings = builder.build();
        applySettings.onRefreshSettings(settings);

        String name = CrateSettings.BULK_REQUEST_TIMEOUT.settingName();
        assertEquals(values.get(name), settings.getAsTime(name, new TimeValue(100, TimeUnit.SECONDS)).toString());

        name = CrateSettings.STATS_ENABLED.settingName();
        assertEquals(values.get(name), settings.getAsBoolean(name, true));

        name = CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.settingName();
        assertEquals(values.get(name), settings.get(name, "none"));

        name = CrateSettings.GRACEFUL_STOP_TIMEOUT.settingName();
        assertEquals(values.get(name), settings.get(name, "1h"));

        name = CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.settingName();
        assertEquals(values.get(name), settings.getAsInt(name, 2));
    }
}
