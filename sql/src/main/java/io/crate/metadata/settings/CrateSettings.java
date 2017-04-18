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

package io.crate.metadata.settings;

import com.google.common.base.Joiner;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.metadata.ReferenceImplementation;
import io.crate.operation.collect.stats.JobsLogService;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.planner.TableStatsService;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.settings.CrateSetting;
import io.crate.settings.SharedSettings;
import io.crate.types.DataTypes;
import io.crate.udc.service.UDCService;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.*;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CrateSettings implements ClusterStateListener {

    public static final List<CrateSetting> CRATE_CLUSTER_SETTINGS = Collections.unmodifiableList(
        Arrays.asList(
            // STATS
            JobsLogService.STATS_ENABLED_SETTING,
            JobsLogService.STATS_JOBS_LOG_SIZE_SETTING,
            JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING,
            JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING,
            JobsLogService.STATS_OPERATIONS_LOG_EXPIRATION_SETTING,
            TableStatsService.STATS_SERVICE_REFRESH_INTERVAL_SETTING,
            CrateCircuitBreakerService.JOBS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
            CrateCircuitBreakerService.JOBS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            CrateCircuitBreakerService.OPERATIONS_LOG_CIRCUIT_BREAKER_LIMIT_SETTING,
            CrateCircuitBreakerService.OPERATIONS_LOG_CIRCUIT_BREAKER_OVERHEAD_SETTING,

            // INDICES
            CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING,
            CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_OVERHEAD_SETTING,

            // BULK
            BulkShardProcessor.BULK_REQUEST_TIMEOUT_SETTING,

            // GRACEFUL STOP
            DecommissioningService.DECOMMISSION_INTERNAL_SETTING_GROUP,
            DecommissioningService.GRACEFUL_STOP_MIN_AVAILABILITY_SETTING,
            DecommissioningService.GRACEFUL_STOP_REALLOCATE_SETTING,
            DecommissioningService.GRACEFUL_STOP_TIMEOUT_SETTING,
            DecommissioningService.GRACEFUL_STOP_FORCE_SETTING,

            // UDC
            UDCService.UDC_ENABLED_SETTING,
            UDCService.UDC_URL_SETTING,
            UDCService.UDC_INITIAL_DELAY_SETTING,
            UDCService.UDC_INTERVAL_SETTING,

            // PSQL
            PostgresNetty.PSQL_ENABLED_SETTING,
            PostgresNetty.PSQL_PORT_SETTING,

            // ENTERPRISE
            SharedSettings.ENTERPRISE_LICENSE_SETTING,
            SharedSettings.LICENSE_IDENT_SETTING
        ));

    private static final List<CrateSetting> EXPOSED_ES_SETTINGS = Collections.unmodifiableList(
        Arrays.asList(
            // CLUSTER
            CrateSetting.of(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING, DataTypes.STRING),
            // CLUSTER ROUTING
            CrateSetting.of(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING, DataTypes.STRING),
            CrateSetting.of(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING, DataTypes.STRING),
            CrateSetting.of(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING, DataTypes.INTEGER),
            CrateSetting.of(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING, DataTypes.INTEGER),
            CrateSetting.of(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING, DataTypes.INTEGER),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_ip",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_id",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_host",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_ip",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_id",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_host",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_ip",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_host",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name",
                Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.STRING),
            CrateSetting.of(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING, DataTypes.FLOAT),
            CrateSetting.of(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING, DataTypes.FLOAT),
            CrateSetting.of(BalancedShardsAllocator.THRESHOLD_SETTING, DataTypes.FLOAT),
            CrateSetting.of(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING, DataTypes.BOOLEAN),
            CrateSetting.of(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING, DataTypes.STRING),
            CrateSetting.of(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING, DataTypes.STRING),
            // DISCOVERY
            CrateSetting.of(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING, DataTypes.INTEGER),
            CrateSetting.of(ZenDiscovery.PING_TIMEOUT_SETTING, DataTypes.STRING),
            CrateSetting.of(DiscoverySettings.PUBLISH_TIMEOUT_SETTING, DataTypes.STRING),
            // GATEWAY
            CrateSetting.of(GatewayService.RECOVER_AFTER_NODES_SETTING, DataTypes.INTEGER),
            CrateSetting.of(GatewayService.RECOVER_AFTER_TIME_SETTING, DataTypes.STRING),
            CrateSetting.of(GatewayService.EXPECTED_NODES_SETTING, DataTypes.INTEGER),
            // INDICES
            CrateSetting.of(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING, DataTypes.STRING),
            CrateSetting.of(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING, DataTypes.STRING),
            CrateSetting.of(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING, DataTypes.STRING),
            CrateSetting.of(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING, DataTypes.STRING),
            CrateSetting.of(RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING, DataTypes.STRING),
            CrateSetting.of(RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, DataTypes.STRING),
            CrateSetting.of(IndexStoreConfig.INDICES_STORE_THROTTLE_TYPE_SETTING, DataTypes.STRING),
            CrateSetting.of(IndexStoreConfig.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING, DataTypes.STRING),
            CrateSetting.of(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, DataTypes.STRING),
            CrateSetting.of(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING, DataTypes.DOUBLE),
            CrateSetting.of(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, DataTypes.STRING),
            CrateSetting.of(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING, DataTypes.DOUBLE)
        ));


    public static final List<CrateSetting> BUILT_IN_SETTINGS = Stream.concat(CRATE_CLUSTER_SETTINGS.stream(), EXPOSED_ES_SETTINGS.stream())
        .filter(cs -> cs.getKey().startsWith("crate.internal.") == false)  // don't expose internal settings
        .collect(Collectors.toList());
    private static final List<String> BUILT_IN_SETTING_NAMES = BUILT_IN_SETTINGS.stream()
        .map(CrateSetting::getKey)
        .collect(Collectors.toList());
    private static final Map<String, String> BUILT_IN_SETTINGS_DEFAULTS_MAP = BUILT_IN_SETTINGS.stream()
        .collect(Collectors.toMap(CrateSetting::getKey, s -> s.setting().getDefaultRaw(Settings.EMPTY)));
    private static final Joiner DOT_JOINER = Joiner.on(".");

    public static boolean isValidSetting(String name) {
        return isLoggingSetting(name) ||
               BUILT_IN_SETTING_NAMES.contains(name) ||
               BUILT_IN_SETTING_NAMES.stream().filter(s -> s.startsWith(name + "."))
                   .collect(Collectors.toList()).isEmpty() == false;
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
        for (CrateSetting<?> crateSetting : BUILT_IN_SETTINGS) {
            Setting<?> setting = crateSetting.setting();
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
                flattenSettings(settingsBuilder, DOT_JOINER.join(key, setting.getKey()), setting.getValue());
            }
        } else {
            if (value instanceof BytesRef) {
                value = BytesRefs.toString(value);
            }
            settingsBuilder.put(key, value);
        }
    }

    private static boolean isLoggingSetting(String name) {
        return name.startsWith("logger.");
    }


    private final Logger logger;
    private final Settings initialSettings;
    private final Map<String, ReferenceImplementation> referenceImplementationTree;

    private volatile Settings settings;

    @Inject
    public CrateSettings(ClusterService clusterService) {
        logger = Loggers.getLogger(this.getClass(), clusterService.getSettings());
        initialSettings = Settings.builder()
            .put(BUILT_IN_SETTINGS_DEFAULTS_MAP)
            .put(clusterService.getSettings())
            .build();
        this.settings = initialSettings;
        referenceImplementationTree = buildReferenceTree();
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            // nothing to do until we actually recover from the gateway or any other block indicates we need to disable persistency
            if (event.state().blocks().disableStatePersistence() == false && event.metaDataChanged()) {
                Settings incomingSetting = event.state().metaData().settings();
                settings = Settings.builder().put(initialSettings).put(incomingSetting).build();
            }
        } catch (Exception ex) {
            logger.warn("failed to apply cluster settings", ex);
        }

    }

    Settings settings() {
        return settings;
    }

    public Map<String, ReferenceImplementation> referenceImplementationTree() {
        return referenceImplementationTree;
    }

    private Map<String, ReferenceImplementation> buildReferenceTree() {
        Map<String, ReferenceImplementation> referenceMap = new HashMap<>(BUILT_IN_SETTINGS.size());
        for (CrateSetting crateSetting : BUILT_IN_SETTINGS) {
            buildReferenceTree(referenceMap, crateSetting);
        }
        return referenceMap;
    }

    private void buildReferenceTree(Map<String, ReferenceImplementation> referenceMap,
                                    CrateSetting<?> crateSetting) {
        String fullName = crateSetting.setting().getKey();
        List<String> parts = crateSetting.path();
        int numParts = parts.size();
        String name = parts.get(numParts - 1);
        if (numParts == 1) {
            // top level setting
            referenceMap.put(fullName, new SettingExpression(this, crateSetting, fullName));
        } else {
            ReferenceImplementation referenceImplementation = new SettingExpression(this, crateSetting, name);

            String topLevelName = parts.get(0);
            NestedSettingExpression topLevelImpl = (NestedSettingExpression) referenceMap.get(topLevelName);
            if (topLevelImpl == null) {
                topLevelImpl = new NestedSettingExpression();
                referenceMap.put(topLevelName, topLevelImpl);
            }

            // group settings have empty name, parent is created above
            if (numParts == 2 && name.isEmpty() == false) {
                topLevelImpl.putChildImplementation(name, referenceImplementation);
            } else {
                // find parent impl
                NestedSettingExpression parentImpl = topLevelImpl;
                for (int i = 1; i < numParts - 1; i++) {
                    String currentName = parts.get(i);
                    NestedSettingExpression current = (NestedSettingExpression) parentImpl.childImplementations().get(currentName);
                    if (current == null) {
                        current = new NestedSettingExpression();
                        parentImpl.putChildImplementation(currentName, current);
                    }
                    parentImpl = current;
                }
                // group settings have empty name, parents are created above
                if (name.isEmpty() == false) {
                    parentImpl.putChildImplementation(name, referenceImplementation);
                }
            }
        }
    }

    static class SettingExpression implements ReferenceImplementation<Object> {
        private final CrateSettings crateSettings;
        private final CrateSetting<?> crateSetting;
        private final String name;

        SettingExpression(CrateSettings crateSettings, CrateSetting<?> crateSetting, String name) {
            this.crateSettings = crateSettings;
            this.crateSetting = crateSetting;
            this.name = name;
        }

        public String name() {
            return name;
        }

        @Override
        public Object value() {
            return crateSetting.dataType().value(crateSetting.setting().get(crateSettings.settings()));
        }
    }

    private static class NestedSettingExpression extends NestedObjectExpression {

        void putChildImplementation(String name, ReferenceImplementation settingExpression) {
            childImplementations.put(name, settingExpression);
        }

        public Map<String, ReferenceImplementation> childImplementations() {
            return childImplementations;
        }
    }
}
