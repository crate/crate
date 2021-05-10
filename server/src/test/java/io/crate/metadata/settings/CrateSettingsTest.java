/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.settings;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.Map;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import io.crate.common.collections.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.junit.Test;

import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class CrateSettingsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testValidSetting() {
        assertThat(CrateSettings.isValidSetting(JobsLogService.STATS_ENABLED_SETTING.getKey()), is(true));
    }

    @Test
    public void testValidLoggingSetting() {
        assertThat(CrateSettings.isValidSetting("logger.info"), is(true));
    }

    @Test
    public void testValidPrefixSetting() {
        assertThat(CrateSettings.isValidSetting("stats"), is(true));
    }

    @Test
    public void testSettingsByNamePrefix() {
        assertThat(CrateSettings.settingNamesByPrefix("stats.jobs_log"), containsInAnyOrder(
            JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(),
            JobsLogService.STATS_JOBS_LOG_FILTER.getKey(),
            JobsLogService.STATS_JOBS_LOG_PERSIST_FILTER.getKey(),
            JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING.getKey()
            ));
    }

    @Test
    public void testLoggingSettingsByNamePrefix() throws Exception {
        assertThat(CrateSettings.settingNamesByPrefix("logger."), contains("logger."));
    }

    @Test
    public void testIsRuntimeSetting() {
        // valid, no exception thrown here
        CrateSettings.checkIfRuntimeSetting(JobsLogService.STATS_ENABLED_SETTING.getKey());
    }

    @Test
    public void testIsNotRuntimeSetting() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Setting 'gateway.expected_nodes' cannot be set/reset at runtime");

        CrateSettings.checkIfRuntimeSetting(GatewayService.EXPECTED_NODES_SETTING.getKey());
    }

    @Test
    public void testFlattenObjectSettings() {
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("enabled", true)
            .put("breaker",
                MapBuilder.newMapBuilder()
                    .put("log", MapBuilder.newMapBuilder()
                        .put("jobs", MapBuilder.newMapBuilder()
                            .put("overhead", 1.05d).map()
                        ).map()
                    ).map()
            ).map();

        Settings.Builder builder  = Settings.builder();
        Settings expected = Settings.builder()
            .put("stats.enabled", true)
            .put("stats.breaker.log.jobs.overhead", 1.05d)
            .build();
        CrateSettings.flattenSettings(builder, "stats", value);
        assertThat(builder.build(), is(expected));
    }

    @Test
    public void testDefaultValuesAreSet() {
        CrateSettings crateSettings = new CrateSettings(clusterService, clusterService.getSettings());
        assertThat(
            crateSettings.settings().get(JobsLogService.STATS_ENABLED_SETTING.getKey()),
            is(JobsLogService.STATS_ENABLED_SETTING.getDefaultRaw(Settings.EMPTY)));
    }


    @Test
    public void testSettingsChanged() {
        CrateSettings crateSettings = new CrateSettings(clusterService, clusterService.getSettings());

        ClusterState newState = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder().transientSettings(
                Settings.builder().put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true).build()))
            .build();
        crateSettings.clusterChanged(new ClusterChangedEvent("settings updated", newState, clusterService.state()));
        assertThat(
            crateSettings.settings().getAsBoolean(JobsLogService.STATS_ENABLED_SETTING.getKey(), false),
            is(true));
    }
}
