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

import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.execution.engine.indexing.ShardingUpsertExecutor;
import io.crate.execution.jobs.NodeLimits;
import io.crate.memory.MemoryManagerFactory;
import io.crate.replication.logical.LogicalReplicationSettings;
import io.crate.statistics.TableStatsService;
import io.crate.types.DataTypes;
import io.crate.udc.service.UDCService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CrateSettings implements ClusterStateListener {

    public static final List<Setting<?>> CRATE_CLUSTER_SETTINGS = List.of(
        // STATS
        JobsLogService.STATS_ENABLED_SETTING,
        JobsLogService.STATS_JOBS_LOG_SIZE_SETTING,
        JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING,
        JobsLogService.STATS_JOBS_LOG_FILTER,
        JobsLogService.STATS_JOBS_LOG_PERSIST_FILTER,
        JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING,
        JobsLogService.STATS_OPERATIONS_LOG_EXPIRATION_SETTING,
        TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING,

        // BULK
        ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING,

        // GRACEFUL STOP
        DecommissioningService.DECOMMISSION_INTERNAL_SETTING_GROUP,
        DecommissioningService.GRACEFUL_STOP_MIN_AVAILABILITY_SETTING,
        DecommissioningService.GRACEFUL_STOP_TIMEOUT_SETTING,
        DecommissioningService.GRACEFUL_STOP_FORCE_SETTING,

        // UDC
        UDCService.UDC_ENABLED_SETTING,
        UDCService.UDC_URL_SETTING,
        UDCService.UDC_INITIAL_DELAY_SETTING,
        UDCService.UDC_INTERVAL_SETTING,

        MemoryManagerFactory.MEMORY_ALLOCATION_TYPE,

        // Logical Replication
        LogicalReplicationSettings.REPLICATION_CHANGE_BATCH_SIZE,
        LogicalReplicationSettings.REPLICATION_READ_POLL_DURATION
    );

    private static final List<Setting<?>> EXPOSED_ES_SETTINGS = List.of(
        // CLUSTER
        InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING,
        // CLUSTER ROUTING
        EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
        EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING,
        ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING,
        ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
        ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
        HierarchyCircuitBreakerService.JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        HierarchyCircuitBreakerService.OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        HierarchyCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_ip",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_id",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_host",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_ip",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_id",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_host",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_ip",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_host",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        Setting.simpleString(
            FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name",
            Setting.Property.NodeScope, Setting.Property.Dynamic),
        BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING,
        BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING,
        BalancedShardsAllocator.THRESHOLD_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING,
        // DISCOVERY
        DiscoverySettings.PUBLISH_TIMEOUT_SETTING,
        // GATEWAY
        GatewayService.RECOVER_AFTER_NODES_SETTING,
        GatewayService.RECOVER_AFTER_DATA_NODES_SETTING,
        GatewayService.RECOVER_AFTER_TIME_SETTING,
        GatewayService.EXPECTED_NODES_SETTING,
        GatewayService.EXPECTED_DATA_NODES_SETTING,
        // INDICES
        RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING,
        RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING,
        RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING,
        RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING,
        HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
        ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE,

        NodeLimits.INITIAL_CONCURRENCY,
        NodeLimits.QUEUE_SIZE,
        NodeLimits.MIN_CONCURRENCY,
        NodeLimits.MAX_CONCURRENCY
    );


    public static final List<Setting<?>> BUILT_IN_SETTINGS = Stream.concat(CRATE_CLUSTER_SETTINGS.stream(), EXPOSED_ES_SETTINGS.stream())
        .filter(cs -> cs.getKey().startsWith("crate.internal.") == false)  // don't expose internal settings
        .collect(Collectors.toList());
    private static final List<String> BUILT_IN_SETTING_NAMES = BUILT_IN_SETTINGS.stream()
        .map(Setting::getKey)
        .collect(Collectors.toList());

    public static boolean isValidSetting(String name) {
        return isLoggingSetting(name) ||
               BUILT_IN_SETTING_NAMES.contains(name) ||
               BUILT_IN_SETTING_NAMES.stream().noneMatch(s -> s.startsWith(name + ".")) == false;
    }

    public static List<String> settingNamesByPrefix(String prefix) {
        if (isLoggingSetting(prefix)) {
            return Collections.singletonList(prefix);
        }
        List<String> filteredList = new ArrayList<>();
        for (String key : BUILT_IN_SETTING_NAMES) {
            if (key.startsWith(prefix)) {
                filteredList.add(key);
            }
        }
        return filteredList;
    }

    public static void checkIfRuntimeSetting(String name) {
        for (Setting<?> setting : BUILT_IN_SETTINGS) {
            if (setting.getKey().equals(name) && setting.isDynamic() == false) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "Setting '%s' cannot be set/reset at runtime", name));
            }
        }
    }

    public static void flattenSettings(Settings.Builder settingsBuilder,
                                       String key,
                                       Object value) {
        if (value instanceof Map) {
            for (Map.Entry<String, Object> setting : ((Map<String, Object>) value).entrySet()) {
                flattenSettings(
                    settingsBuilder,
                    String.join(".", key, setting.getKey()),
                    setting.getValue()
                );
            }
        } else if (value == null) {
            throw new IllegalArgumentException(
                "Cannot set \"" + key + "\" to `null`. Use `RESET [GLOBAL] \"" + key +
                "\"` to reset a setting to its default value");
        } else if (value instanceof List) {
            List<String> values = DataTypes.STRING_ARRAY.sanitizeValue(value);
            settingsBuilder.put(key, String.join(",", values));
        } else {
            settingsBuilder.put(key, value.toString());
        }
    }

    private static boolean isLoggingSetting(String name) {
        return name.startsWith("logger.");
    }

    private final Logger logger;
    private final Settings initialSettings;

    private volatile Settings settings;

    @Inject
    public CrateSettings(ClusterService clusterService, Settings settings) {
        logger = LogManager.getLogger(this.getClass());
        Settings.Builder defaultsBuilder = Settings.builder();
        for (Setting<?> builtInSetting : BUILT_IN_SETTINGS) {
            defaultsBuilder.put(builtInSetting.getKey(), builtInSetting.getDefaultRaw(settings));
        }
        initialSettings = defaultsBuilder.put(settings).build();
        this.settings = initialSettings;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            // nothing to do until we actually recover from the gateway or any other block indicates we need to disable persistency
            if (event.state().blocks().disableStatePersistence() == false && event.metadataChanged()) {
                Settings incomingSetting = event.state().metadata().settings();
                settings = Settings.builder().put(initialSettings).put(incomingSetting).build();
            }
        } catch (Exception ex) {
            logger.warn("failed to apply cluster settings", ex);
        }

    }

    public Settings settings() {
        return settings;
    }
}
