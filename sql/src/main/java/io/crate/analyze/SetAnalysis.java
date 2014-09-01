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

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.TableIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Map;

public class SetAnalysis extends Analysis {

    public static final Map<String, SettingsApplier> SUPPORTED_SETTINGS = ImmutableMap.<String, SettingsApplier>builder()
        .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.STATS_JOBS_LOG_SIZE))
        .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.STATS_OPERATIONS_LOG_SIZE))
        .put(CrateSettings.STATS_ENABLED.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.STATS_ENABLED))
        .put(CrateSettings.GRACEFUL_STOP.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.GRACEFUL_STOP))
        .put(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY))
        .put(CrateSettings.GRACEFUL_STOP_REALLOCATE.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.GRACEFUL_STOP_REALLOCATE))
        .put(CrateSettings.GRACEFUL_STOP_FORCE.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.GRACEFUL_STOP_FORCE))
        .put(CrateSettings.GRACEFUL_STOP_TIMEOUT.settingName(),
                new SettingsAppliers.TimeSettingsApplier(CrateSettings.GRACEFUL_STOP_TIMEOUT))
        .put(CrateSettings.GRACEFUL_STOP_IS_DEFAULT.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.GRACEFUL_STOP_IS_DEFAULT))
        .put(CrateSettings.DISCOVERY.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.DISCOVERY))
        .put(CrateSettings.DISCOVERY_ZEN.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.DISCOVERY_ZEN))
        .put(CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES))
        .put(CrateSettings.DISCOVERY_ZEN_PING_TIMEOUT.settingName(),
                new SettingsAppliers.TimeSettingsApplier(CrateSettings.DISCOVERY_ZEN_PING_TIMEOUT))
        .put(CrateSettings.DISCOVERY_ZEN_PUBLISH_TIMEOUT.settingName(),
                new SettingsAppliers.TimeSettingsApplier(CrateSettings.DISCOVERY_ZEN_PUBLISH_TIMEOUT))
        .put(CrateSettings.ROUTING.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING))
        .put(CrateSettings.ROUTING_ALLOCATION.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION))
        .put(CrateSettings.ROUTING_ALLOCATION_ENABLE.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_ENABLE))
        .put(CrateSettings.ROUTING_ALLOCATION_ALLOW_REBALANCE.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_ALLOW_REBALANCE))
        .put(CrateSettings.ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE))
        .put(CrateSettings.ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES))
        .put(CrateSettings.ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES))
        .put(CrateSettings.ROUTING_ALLOCATION_AWARENESS.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_AWARENESS))
        .put(CrateSettings.ROUTING_ALLOCATION_AWARENESS_ATTRIBUTES.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_AWARENESS_ATTRIBUTES))
        .put(CrateSettings.ROUTING_ALLOCATION_BALANCE.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE))
        .put(CrateSettings.ROUTING_ALLOCATION_BALANCE_SHARD.settingName(),
                new SettingsAppliers.FloatSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE_SHARD))
        .put(CrateSettings.ROUTING_ALLOCATION_BALANCE_INDEX.settingName(),
                new SettingsAppliers.FloatSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE_INDEX))
        .put(CrateSettings.ROUTING_ALLOCATION_BALANCE_PRIMARY.settingName(),
                new SettingsAppliers.FloatSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE_PRIMARY))
        .put(CrateSettings.ROUTING_ALLOCATION_BALANCE_THRESHOLD.settingName(),
                new SettingsAppliers.FloatSettingsApplier(CrateSettings.ROUTING_ALLOCATION_BALANCE_THRESHOLD))
        .put(CrateSettings.ROUTING_ALLOCATION_DISK.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK))
        .put(CrateSettings.ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED))
        .put(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK))
        .put(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_LOW.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_LOW))
        .put(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_HIGH.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_HIGH))
        .put(CrateSettings.INDICES.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES))
        .put(CrateSettings.INDICES_RECOVERY.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_RECOVERY))
        .put(CrateSettings.INDICES_RECOVERY_CONCURRENT_STREAMS.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.INDICES_RECOVERY_CONCURRENT_STREAMS))
        .put(CrateSettings.INDICES_RECOVERY_FILE_CHUNK_SIZE.settingName(),
                new SettingsAppliers.ByteSizeSettingsApplier(CrateSettings.INDICES_RECOVERY_FILE_CHUNK_SIZE))
        .put(CrateSettings.INDICES_RECOVERY_TRANSLOG_OPS.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.INDICES_RECOVERY_TRANSLOG_OPS))
        .put(CrateSettings.INDICES_RECOVERY_TRANSLOG_SIZE.settingName(),
                new SettingsAppliers.ByteSizeSettingsApplier(CrateSettings.INDICES_RECOVERY_TRANSLOG_SIZE))
        .put(CrateSettings.INDICES_RECOVERY_COMPRESS.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.INDICES_RECOVERY_COMPRESS))
        .put(CrateSettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC.settingName(),
                new SettingsAppliers.ByteSizeSettingsApplier(CrateSettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC))
        .put(CrateSettings.INDICES_STORE.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_STORE))
        .put(CrateSettings.INDICES_STORE_THROTTLE.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_STORE_THROTTLE))
        .put(CrateSettings.INDICES_STORE_THROTTLE_TYPE.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.INDICES_STORE_THROTTLE_TYPE))
        .put(CrateSettings.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC.settingName(),
                new SettingsAppliers.ByteSizeSettingsApplier(CrateSettings.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC))
        .put(CrateSettings.INDICES_FIELDDATA.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_FIELDDATA))
        .put(CrateSettings.INDICES_FIELDDATA_BREAKER.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.INDICES_FIELDDATA_BREAKER))
        .put(CrateSettings.INDICES_FIELDDATA_BREAKER_LIMIT.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.INDICES_FIELDDATA_BREAKER_LIMIT))
        .put(CrateSettings.INDICES_FIELDDATA_BREAKER_OVERHEAD.settingName(),
                new SettingsAppliers.DoubleSettingsApplier(CrateSettings.INDICES_FIELDDATA_BREAKER_OVERHEAD))
        .put(CrateSettings.CLUSTER_INFO.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.CLUSTER_INFO))
        .put(CrateSettings.CLUSTER_INFO_UPDATE.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.CLUSTER_INFO_UPDATE))
        .put(CrateSettings.CLUSTER_INFO_UPDATE_INTERVAL.settingName(),
                new SettingsAppliers.TimeSettingsApplier(CrateSettings.CLUSTER_INFO_UPDATE_INTERVAL))
            .build();

    private Settings settings;
    private boolean persistent = false;

    protected SetAnalysis(Analyzer.ParameterContext parameterContext) {
        super(parameterContext);
    }

    public Settings settings() {
        return settings;
    }

    public void settings(Settings settings) {
        this.settings = settings;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public boolean isTransient() {
        return !persistent;
    }

    public void persistent(boolean persistent) {
        this.persistent = persistent;
    }

    public @Nullable SettingsApplier getSetting(String name) {
        return SUPPORTED_SETTINGS.get(name);
    }

    @Override
    public void table(TableIdent tableIdent) {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "table() not supported on %s", getClass().getSimpleName())
        );
    }

    @Override
    public TableInfo table() {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "table() not supported on %s", getClass().getSimpleName()));
    }

    @Override
    public SchemaInfo schema() {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "schema() not supported on %s", getClass().getSimpleName())
        );
    }

    @Override
    public boolean hasNoResult() {
        return settings.getAsMap().isEmpty();
    }

    @Override
    public void normalize() {

    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitSetAnalysis(this, context);
    }
}
