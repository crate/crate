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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.common.collections.Lists;
import io.crate.common.unit.TimeValue;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.execution.engine.indexing.ShardingUpsertExecutor;
import io.crate.udc.service.UDCService;

@IntegTestCase.ClusterScope
public class SysClusterSettingsTest extends IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        builder.put(ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.getKey(), "42s");
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Lists.concat(super.nodePlugins(), TestPluginWithArchivedSetting.class);
    }

    @After
    public void resetSettings() throws Exception {
        execute("reset global stats, bulk, indices, cluster");
    }

    @Test
    public void testSetResetGlobalSetting() throws Exception {
        execute("set global persistent stats.enabled = false");
        execute("select settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(false);

        execute("reset global stats.enabled");
        execute("select settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(JobsLogService.STATS_ENABLED_SETTING.getDefault(Settings.EMPTY));

        execute("set global transient stats = { enabled = true, jobs_log_size = 3, operations_log_size = 4 }");
        execute("select settings['stats']['enabled'], settings['stats']['jobs_log_size']," +
                "settings['stats']['operations_log_size'] from sys.cluster");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(true);
        assertThat(response.rows()[0][1]).isEqualTo(3);
        assertThat(response.rows()[0][2]).isEqualTo(4);

        execute("reset global stats");
        execute("select settings['stats']['enabled'], settings['stats']['jobs_log_size']," +
                "settings['stats']['operations_log_size'] from sys.cluster");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(JobsLogService.STATS_ENABLED_SETTING.getDefault(Settings.EMPTY));
        assertThat(response.rows()[0][1]).isEqualTo(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getDefault(Settings.EMPTY));
        assertThat(response.rows()[0][2]).isEqualTo(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault(Settings.EMPTY));

        execute("set global transient \"indices.breaker.query.limit\" = '20mb'");
        execute("select settings from sys.cluster");
        assertSettingsValue(HierarchyCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "20mb");
    }

    @Test
    public void testResetPersistent() throws Exception {

        execute("select settings['bulk']['request_timeout'] from sys.cluster");
        assertThat(response.rows()[0][0]).isEqualTo("42s"); // configured via nodeSettings

        execute("set global persistent bulk.request_timeout = '59s'");
        execute("select settings['bulk']['request_timeout'] from sys.cluster");
        assertThat(response.rows()[0][0]).isEqualTo("59s");

        execute("reset global bulk.request_timeout");
        execute("select settings['bulk']['request_timeout'] from sys.cluster");
        assertThat(response.rows()[0][0]).isEqualTo("42s");
    }

    @Test
    public void testDynamicTransientSettings() throws Exception {
        execute("set global transient stats.jobs_log_size = 1, stats.operations_log_size = 2, stats.enabled = false");

        execute("select settings from sys.cluster");
        assertSettingsValue(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 1);
        assertSettingsValue(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 2);
        assertSettingsValue(JobsLogService.STATS_ENABLED_SETTING.getKey(), false);

        cluster().fullRestart();

        execute("select settings from sys.cluster");
        assertSettingsDefault(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING);
        assertSettingsDefault(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING);
        assertSettingsDefault(JobsLogService.STATS_ENABLED_SETTING);
    }

    @Test
    public void testDynamicPersistentSettings() throws Exception {
        execute("set global persistent stats.operations_log_size = 100");

        execute("select settings from sys.cluster");
        assertSettingsValue(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 100);

        cluster().fullRestart();
        // the gateway recovery is async and
        // it might take a bit until it reads the persisted cluster state and updates the settings expression
        assertBusy(() -> {
            execute("select settings from sys.cluster");
            assertSettingsValue(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 100);
        });
    }

    @Test
    public void testStaticGatewayDefaultSettings() {
        execute("select settings from sys.cluster");
        assertSettingsDefault(GatewayService.EXPECTED_NODES_SETTING);
        assertSettingsDefault(GatewayService.EXPECTED_DATA_NODES_SETTING);
        assertSettingsDefault(GatewayService.RECOVER_AFTER_NODES_SETTING);
        assertSettingsDefault(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING);
        assertSettingsValue(
            GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(),
            GatewayService.RECOVER_AFTER_TIME_SETTING.getDefaultRaw(Settings.EMPTY));
    }

    @Test
    public void testStaticUDCDefaultSettings() {
        execute("select settings from sys.cluster");
        assertSettingsDefault(UDCService.UDC_ENABLED_SETTING);
        assertSettingsValue(
            UDCService.UDC_INITIAL_DELAY_SETTING.getKey(),
            UDCService.UDC_INITIAL_DELAY_SETTING.getDefaultRaw(Settings.EMPTY));
        assertSettingsValue(
            UDCService.UDC_INTERVAL_SETTING.getKey(),
            UDCService.UDC_INTERVAL_SETTING.getDefaultRaw(Settings.EMPTY));
        assertSettingsDefault(UDCService.UDC_URL_SETTING);
    }

    @Test
    public void testStatsCircuitBreakerLogsDefaultSettings() {
        execute("select settings from sys.cluster");
        assertSettingsValue(
            HierarchyCircuitBreakerService.JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(),
            MemorySizeValue.parseBytesSizeValueOrHeapRatio(
                HierarchyCircuitBreakerService.JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.getDefaultRaw(Settings.EMPTY),
                HierarchyCircuitBreakerService.JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()).toString());
    }

    @Test
    public void testReadChangedElasticsearchSetting() {
        execute("set global transient indices.recovery.max_bytes_per_sec = ?",
                new Object[]{"100kb"});
        execute("select settings from sys.cluster");
        assertSettingsValue(
            INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(),
            "100kb");
    }

    /**
     * There was a regression introduced with v4.0.0 which prevents to set this setting as it was not included
     * at {@link org.elasticsearch.common.settings.ClusterSettings#BUILT_IN_CLUSTER_SETTINGS}.
     * Additionally the new setting was not applied internally to the cluster service due to missing update consumer.
     */
    @Test
    public void test_set_cluster_info_update_interval() throws Exception {
        execute("set global transient \"cluster.info.update.interval\" = '45s'");
        var clusterService = cluster().getInstance(ClusterInfoService.class);
        Field f1 = clusterService.getClass().getDeclaredField("updateFrequency");
        f1.setAccessible(true);
        assertThat(f1.get(clusterService)).isEqualTo(TimeValue.timeValueSeconds(45));
    }

    @Test
    public void test_archived_setting_can_be_reset() throws Exception {
        execute("set global transient \"archived.cluster.routing.allocation.disk.watermark.low\"=\"70%\"");
        var clusterService = cluster().getMasterNodeInstance(ClusterService.class);
        assertThat(clusterService.getClusterSettings().get("archived.cluster.routing.allocation.disk.watermark.low"))
            .isNotNull();
        assertThat(clusterService.state().metadata().settings().get("archived.cluster.routing.allocation.disk.watermark.low"))
            .isEqualTo("70%");

        execute("reset global \"archived.cluster.routing.allocation.disk.watermark.low\"");
        clusterService = cluster().getMasterNodeInstance(ClusterService.class);
        assertThat(clusterService.state().metadata().settings().get("archived.cluster.routing.allocation.disk.watermark.low"))
            .isNull();
    }

    private void assertSettingsDefault(Setting<?> esSetting) {
        assertSettingsValue(esSetting.getKey(), esSetting.getDefault(Settings.EMPTY));
    }

    @SuppressWarnings("unchecked")
    private void assertSettingsValue(String key, Object expectedValue) {
        Map<String, Object> settingMap = (Map<String, Object>) response.rows()[0][0];
        List<String> settingPath = List.of(key.split("\\."));
        for (int i = 0; i < settingPath.size() - 1; i++) {
            settingMap = (Map<String, Object>) settingMap.get(settingPath.get(i));
        }
        assertThat(settingMap.get(settingPath.get(settingPath.size() - 1))).isEqualTo(expectedValue);
    }

    public static class TestPluginWithArchivedSetting extends Plugin {

        public TestPluginWithArchivedSetting() {}

        @Override
        public Settings additionalSettings() {
            return Settings.builder()
                .put("archived.cluster.routing.allocation.disk.watermark.low", "80%")
                .build();
        }

        @Override
        public List<Setting<?>> getSettings() {
            Setting<String> diskWatermarkLow = Setting.simpleString(
                "archived.cluster.routing.allocation.disk.watermark.low",
                "85%",
                Property.Dynamic,
                Property.NodeScope,
                Property.Exposed
            );
            return List.of(diskWatermarkLow);
        }
    }
}
