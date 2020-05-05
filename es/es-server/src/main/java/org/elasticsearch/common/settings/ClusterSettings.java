/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.ElectionSchedulerFactory;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.coordination.LagDetector;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.discovery.SettingsBasedSeedHostsProvider;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.HunspellService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.monitor.jvm.JvmGcMonitorService;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.process.ProcessService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportSettings;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Encapsulates all valid cluster level settings.
 */
public final class ClusterSettings extends AbstractScopedSettings {

    private final List<Setting<?>> maskedSettings;

    public ClusterSettings(final Settings nodeSettings, final Set<Setting<?>> settingsSet) {
        this(nodeSettings, List.of(), settingsSet, Collections.emptySet());
    }

    public ClusterSettings(final Settings nodeSettings,
                           final List<Setting<?>> maskedSettings,
                           final Set<Setting<?>> settingsSet,
                           final Set<SettingUpgrader<?>> settingUpgraders) {
        super(nodeSettings, settingsSet, settingUpgraders, Property.NodeScope);
        this.maskedSettings = maskedSettings;
        addSettingsUpdater(new LoggingSettingUpdater(nodeSettings));
    }

    public List<Setting<?>> maskedSettings() {
        return maskedSettings;
    }

    private static final class LoggingSettingUpdater implements SettingUpdater<Settings> {
        final Predicate<String> loggerPredicate = Loggers.LOG_LEVEL_SETTING::match;
        private final Settings settings;

        LoggingSettingUpdater(Settings settings) {
            this.settings = settings;
        }

        @Override
        public boolean hasChanged(Settings current, Settings previous) {
            return current.filter(loggerPredicate).equals(previous.filter(loggerPredicate)) == false;
        }

        @Override
        public Settings getValue(Settings current, Settings previous) {
            Settings.Builder builder = Settings.builder();
            builder.put(current.filter(loggerPredicate));
            for (String key : previous.keySet()) {
                if (loggerPredicate.test(key) && builder.keys().contains(key) == false) {
                    if (Loggers.LOG_LEVEL_SETTING.getConcreteSetting(key).exists(settings) == false) {
                        builder.putNull(key);
                    } else {
                        builder.put(key, Loggers.LOG_LEVEL_SETTING.getConcreteSetting(key).get(settings).toString());
                    }
                }
            }
            return builder.build();
        }

        @Override
        public void apply(Settings value, Settings current, Settings previous) {
            for (String key : value.keySet()) {
                assert loggerPredicate.test(key);
                String component = key.substring("logger.".length());
                if ("level".equals(component)) {
                    continue;
                }
                if ("_root".equals(component)) {
                    final String rootLevel = value.get(key);
                    if (rootLevel == null) {
                        Loggers.setLevel(LogManager.getRootLogger(), Loggers.LOG_DEFAULT_LEVEL_SETTING.get(settings));
                    } else {
                        Loggers.setLevel(LogManager.getRootLogger(), rootLevel);
                    }
                } else {
                    Loggers.setLevel(LogManager.getLogger(component), value.get(key));
                }
            }
        }
    }

    public static final Set<Setting<?>> BUILT_IN_CLUSTER_SETTINGS = Set.of(
        AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
        AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING,
        BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING,
        BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING,
        BalancedShardsAllocator.THRESHOLD_SETTING,
        ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING,
        ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
        EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
        EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING,
        FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING,
        FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING,
        FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING,
        FsRepository.REPOSITORIES_CHUNK_SIZE_SETTING,
        FsRepository.REPOSITORIES_LOCATION_SETTING,
        IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING,
        IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING,
        IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING,
        MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING,
        MetaData.SETTING_READ_ONLY_SETTING,
        MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING,
        RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING,
        RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING,
        RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING,
        RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
        RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING,
        DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING,
        SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING,
        DestructiveOperations.REQUIRES_NAME_SETTING,
        NoMasterBlockService.NO_MASTER_BLOCK_SETTING,
        DiscoverySettings.PUBLISH_TIMEOUT_SETTING,
        DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING,
        DiscoverySettings.COMMIT_TIMEOUT_SETTING,
        DiscoverySettings.NO_MASTER_BLOCK_SETTING,
        GatewayService.EXPECTED_DATA_NODES_SETTING,
        GatewayService.EXPECTED_MASTER_NODES_SETTING,
        GatewayService.EXPECTED_NODES_SETTING,
        GatewayService.RECOVER_AFTER_DATA_NODES_SETTING,
        GatewayService.RECOVER_AFTER_MASTER_NODES_SETTING,
        GatewayService.RECOVER_AFTER_NODES_SETTING,
        GatewayService.RECOVER_AFTER_TIME_SETTING,
        NetworkModule.HTTP_DEFAULT_TYPE_SETTING,
        NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING,
        NetworkModule.HTTP_TYPE_SETTING,
        NetworkModule.TRANSPORT_TYPE_SETTING,
        HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS,
        HttpTransportSettings.SETTING_CORS_ENABLED,
        HttpTransportSettings.SETTING_CORS_MAX_AGE,
        HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED,
        HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN,
        HttpTransportSettings.SETTING_HTTP_HOST,
        HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST,
        HttpTransportSettings.SETTING_HTTP_BIND_HOST,
        HttpTransportSettings.SETTING_HTTP_PORT,
        HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT,
        HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS,
        HttpTransportSettings.SETTING_HTTP_COMPRESSION,
        HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL,
        HttpTransportSettings.SETTING_CORS_ALLOW_METHODS,
        HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS,
        HttpTransportSettings.SETTING_HTTP_CONTENT_TYPE_REQUIRED,
        HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH,
        HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE,
        HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE,
        HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_COUNT,
        HttpTransportSettings.SETTING_HTTP_MAX_WARNING_HEADER_SIZE,
        HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH,
        HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT,
        HttpTransportSettings.SETTING_HTTP_RESET_COOKIES,
        HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY,
        HttpTransportSettings.SETTING_HTTP_TCP_KEEP_ALIVE,
        HttpTransportSettings.SETTING_HTTP_TCP_REUSE_ADDRESS,
        HttpTransportSettings.SETTING_HTTP_TCP_SEND_BUFFER_SIZE,
        HttpTransportSettings.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE,
        HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        HierarchyCircuitBreakerService.ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING,
        HierarchyCircuitBreakerService.ACCOUNTING_CIRCUIT_BREAKER_OVERHEAD_SETTING,
        IndexModule.NODE_STORE_ALLOW_MMAPFS,
        IndexModule.NODE_STORE_ALLOW_MMAP,
        ClusterService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
        ClusterService.USER_DEFINED_META_DATA,
        ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING,
        NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING,
        HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING,
        HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING,
        TransportSettings.HOST,
        TransportSettings.PUBLISH_HOST,
        TransportSettings.PUBLISH_HOST_PROFILE,
        TransportSettings.BIND_HOST,
        TransportSettings.BIND_HOST_PROFILE,
        TransportSettings.OLD_PORT,
        TransportSettings.PORT,
        TransportSettings.PORT_PROFILE,
        TransportSettings.PUBLISH_PORT,
        TransportSettings.PUBLISH_PORT_PROFILE,
        TransportSettings.OLD_TRANSPORT_COMPRESS,
        TransportSettings.TRANSPORT_COMPRESS,
        TransportSettings.PING_SCHEDULE,
        TransportSettings.TCP_CONNECT_TIMEOUT,
        TransportSettings.CONNECT_TIMEOUT,
        TransportSettings.DEFAULT_FEATURES_SETTING,
        TransportSettings.OLD_TCP_NO_DELAY,
        TransportSettings.TCP_NO_DELAY,
        TransportSettings.OLD_TCP_NO_DELAY_PROFILE,
        TransportSettings.TCP_NO_DELAY_PROFILE,
        TransportSettings.TCP_KEEP_ALIVE,
        TransportSettings.OLD_TCP_KEEP_ALIVE_PROFILE,
        TransportSettings.TCP_KEEP_ALIVE_PROFILE,
        TransportSettings.TCP_REUSE_ADDRESS,
        TransportSettings.OLD_TCP_REUSE_ADDRESS_PROFILE,
        TransportSettings.TCP_REUSE_ADDRESS_PROFILE,
        TransportSettings.TCP_SEND_BUFFER_SIZE,
        TransportSettings.OLD_TCP_SEND_BUFFER_SIZE_PROFILE,
        TransportSettings.TCP_SEND_BUFFER_SIZE_PROFILE,
        TransportSettings.TCP_RECEIVE_BUFFER_SIZE,
        TransportSettings.OLD_TCP_RECEIVE_BUFFER_SIZE_PROFILE,
        TransportSettings.TCP_RECEIVE_BUFFER_SIZE_PROFILE,
        TransportSettings.CONNECTIONS_PER_NODE_RECOVERY,
        TransportSettings.CONNECTIONS_PER_NODE_BULK,
        TransportSettings.CONNECTIONS_PER_NODE_REG,
        TransportSettings.CONNECTIONS_PER_NODE_STATE,
        TransportSettings.CONNECTIONS_PER_NODE_PING,
        TransportSettings.TRACE_LOG_EXCLUDE_SETTING,
        TransportSettings.TRACE_LOG_INCLUDE_SETTING,
        NetworkService.NETWORK_SERVER,
        NetworkService.GLOBAL_NETWORK_HOST_SETTING,
        NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING,
        NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING,
        NetworkService.TCP_NO_DELAY,
        NetworkService.TCP_KEEP_ALIVE,
        NetworkService.TCP_REUSE_ADDRESS,
        NetworkService.TCP_SEND_BUFFER_SIZE,
        NetworkService.TCP_RECEIVE_BUFFER_SIZE,
        NetworkService.TCP_CONNECT_TIMEOUT,
        IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING,
        IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY,
        HunspellService.HUNSPELL_LAZY_LOAD,
        HunspellService.HUNSPELL_IGNORE_CASE,
        HunspellService.HUNSPELL_DICTIONARY_OPTIONS,
        IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT,
        Environment.PATH_DATA_SETTING,
        Environment.PATH_HOME_SETTING,
        Environment.PATH_LOGS_SETTING,
        Environment.PATH_REPO_SETTING,
        Environment.PATH_SHARED_DATA_SETTING,
        Environment.PIDFILE_SETTING,
        NodeEnvironment.NODE_ID_SEED_SETTING,
        DiscoveryModule.DISCOVERY_TYPE_SETTING,
        DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING,
        SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING,
        SeedHostsResolver.DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING,
        SeedHostsResolver.DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING,
        DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING,
        Node.WRITE_PORTS_FILE_SETTING,
        Node.NODE_NAME_SETTING,
        Node.NODE_DATA_SETTING,
        Node.NODE_MASTER_SETTING,
        Node.NODE_ATTRIBUTES,
        Node.NODE_LOCAL_STORAGE_SETTING,
        TransportMasterNodeReadAction.FORCE_LOCAL_SETTING,
        ClusterName.CLUSTER_NAME_SETTING,
        ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING,
        EsExecutors.PROCESSORS_SETTING,
        ThreadContext.DEFAULT_HEADERS_SETTING,
        Loggers.LOG_DEFAULT_LEVEL_SETTING,
        Loggers.LOG_LEVEL_SETTING,
        NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING,
        NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING,
        OsService.REFRESH_INTERVAL_SETTING,
        ProcessService.REFRESH_INTERVAL_SETTING,
        JvmService.REFRESH_INTERVAL_SETTING,
        FsService.REFRESH_INTERVAL_SETTING,
        JvmGcMonitorService.ENABLED_SETTING,
        JvmGcMonitorService.REFRESH_INTERVAL_SETTING,
        JvmGcMonitorService.GC_SETTING,
        JvmGcMonitorService.GC_OVERHEAD_WARN_SETTING,
        JvmGcMonitorService.GC_OVERHEAD_INFO_SETTING,
        JvmGcMonitorService.GC_OVERHEAD_DEBUG_SETTING,
        PageCacheRecycler.TYPE_SETTING,
        PageCacheRecycler.LIMIT_HEAP_SETTING,
        PageCacheRecycler.WEIGHT_BYTES_SETTING,
        PageCacheRecycler.WEIGHT_INT_SETTING,
        PageCacheRecycler.WEIGHT_LONG_SETTING,
        PageCacheRecycler.WEIGHT_OBJECTS_SETTING,
        PluginsService.MANDATORY_SETTING,
        BootstrapSettings.MEMORY_LOCK_SETTING,
        BootstrapSettings.CTRLHANDLER_SETTING,
        IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING,
        IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING,
        IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING,
        IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING,
        IndexingMemoryController.SHARD_MEMORY_INTERVAL_TIME_SETTING,
        SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING,
        ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING,
        IndexGraveyard.SETTING_MAX_TOMBSTONES,
        PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING,
        PeerFinder.DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING,
        ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING,
        ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING,
        ElectionSchedulerFactory.ELECTION_BACK_OFF_TIME_SETTING,
        ElectionSchedulerFactory.ELECTION_MAX_TIMEOUT_SETTING,
        ElectionSchedulerFactory.ELECTION_DURATION_SETTING,
        Coordinator.PUBLISH_TIMEOUT_SETTING,
        JoinHelper.JOIN_TIMEOUT_SETTING,
        FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING,
        FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING,
        FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING,
        LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING,
        LeaderChecker.LEADER_CHECK_INTERVAL_SETTING,
        LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING,
        Reconfigurator.CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION,
        TransportAddVotingConfigExclusionsAction.MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING,
        ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING,
        ClusterBootstrapService.UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING,
        LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING);
}
