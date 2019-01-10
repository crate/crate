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
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.OperationRouting;
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
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.discovery.zen.SettingsBasedHostsProvider;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.IndicesRequestCache;
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
import org.elasticsearch.persistent.decider.EnableAssignmentDecider;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.fetch.subphase.highlight.FastVectorHighlighter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Encapsulates all valid cluster level settings.
 */
public final class ClusterSettings extends AbstractScopedSettings {
    public ClusterSettings(final Settings nodeSettings, final Set<Setting<?>> settingsSet) {
        this(nodeSettings, settingsSet, Collections.emptySet());
    }

    public ClusterSettings(
            final Settings nodeSettings, final Set<Setting<?>> settingsSet, final Set<SettingUpgrader<?>> settingUpgraders) {
        super(nodeSettings, settingsSet, settingUpgraders, Property.NodeScope);
        addSettingsUpdater(new LoggingSettingUpdater(nodeSettings));
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

    public static Set<Setting<?>> BUILT_IN_CLUSTER_SETTINGS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
                    TransportClient.CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL, // TODO these transport client settings are kind
                    // of odd here and should only be valid if we are a transport client
                    TransportClient.CLIENT_TRANSPORT_PING_TIMEOUT,
                    TransportClient.CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME,
                    TransportClient.CLIENT_TRANSPORT_SNIFF,
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
                    FsRepository.REPOSITORIES_COMPRESS_SETTING,
                    FsRepository.REPOSITORIES_LOCATION_SETTING,
                    IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING,
                    IndicesQueryCache.INDICES_CACHE_QUERY_COUNT_SETTING,
                    IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING,
                    MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING,
                    MetaData.SETTING_READ_ONLY_SETTING,
                    MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING,
                    MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE,
                    RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING,
                    RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING,
                    RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING,
                    RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING,
                    RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING,
                    RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
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
                    InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING,
                    InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING,
                    DestructiveOperations.REQUIRES_NAME_SETTING,
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
                    NetworkModule.HTTP_ENABLED,
                    NetworkModule.HTTP_DEFAULT_TYPE_SETTING,
                    NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING,
                    NetworkModule.HTTP_TYPE_SETTING,
                    NetworkModule.TRANSPORT_TYPE_SETTING,
                    HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS,
                    HttpTransportSettings.SETTING_CORS_ENABLED,
                    HttpTransportSettings.SETTING_CORS_MAX_AGE,
                    HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED,
                    HttpTransportSettings.SETTING_PIPELINING,
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
                    HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED,
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
                    ClusterService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    ClusterService.USER_DEFINED_META_DATA,
                    SearchService.DEFAULT_SEARCH_TIMEOUT_SETTING,
                    SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS,
                    ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING,
                    TransportSearchAction.SHARD_COUNT_LIMIT_SETTING,
                    RemoteClusterAware.REMOTE_CLUSTERS_SEEDS,
                    RemoteClusterAware.SEARCH_REMOTE_CLUSTERS_SEEDS,
                    RemoteClusterAware.REMOTE_CLUSTERS_PROXY,
                    RemoteClusterAware.SEARCH_REMOTE_CLUSTERS_PROXY,
                    RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE,
                    RemoteClusterService.SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE,
                    RemoteClusterService.REMOTE_CONNECTIONS_PER_CLUSTER,
                    RemoteClusterService.SEARCH_REMOTE_CONNECTIONS_PER_CLUSTER,
                    RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING,
                    RemoteClusterService.SEARCH_REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING,
                    RemoteClusterService.REMOTE_NODE_ATTRIBUTE,
                    RemoteClusterService.SEARCH_REMOTE_NODE_ATTRIBUTE,
                    RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
                    RemoteClusterService.SEARCH_ENABLE_REMOTE_CLUSTERS,
                    TransportService.TRACE_LOG_EXCLUDE_SETTING,
                    TransportService.TRACE_LOG_INCLUDE_SETTING,
                    TransportCloseIndexAction.CLUSTER_INDICES_CLOSE_ENABLE_SETTING,
                    ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING,
                    NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING,
                    HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING,
                    HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING,
                    Transport.TRANSPORT_TCP_COMPRESS,
                    TcpTransport.HOST,
                    TcpTransport.PUBLISH_HOST,
                    TcpTransport.BIND_HOST,
                    TcpTransport.PUBLISH_PORT,
                    TcpTransport.PORT,
                    TcpTransport.BIND_HOST_PROFILE,
                    TcpTransport.PUBLISH_HOST_PROFILE,
                    TcpTransport.PUBLISH_PORT_PROFILE,
                    TcpTransport.PORT_PROFILE,
                    TcpTransport.TCP_NO_DELAY_PROFILE,
                    TcpTransport.TCP_KEEP_ALIVE_PROFILE,
                    TcpTransport.TCP_REUSE_ADDRESS_PROFILE,
                    TcpTransport.TCP_SEND_BUFFER_SIZE_PROFILE,
                    TcpTransport.TCP_RECEIVE_BUFFER_SIZE_PROFILE,
                    TransportService.CONNECTIONS_PER_NODE_RECOVERY,
                    TransportService.CONNECTIONS_PER_NODE_BULK,
                    TransportService.CONNECTIONS_PER_NODE_REG,
                    TransportService.CONNECTIONS_PER_NODE_STATE,
                    TransportService.CONNECTIONS_PER_NODE_PING,
                    TransportService.TCP_CONNECT_TIMEOUT,
                    TcpTransport.PING_SCHEDULE,
                    NetworkService.NETWORK_SERVER,
                    TcpTransport.TCP_NO_DELAY,
                    TcpTransport.TCP_KEEP_ALIVE,
                    TcpTransport.TCP_REUSE_ADDRESS,
                    TcpTransport.TCP_SEND_BUFFER_SIZE,
                    TcpTransport.TCP_RECEIVE_BUFFER_SIZE,
                    NetworkService.GLOBAL_NETWORK_HOST_SETTING,
                    NetworkService.GLOBAL_NETWORK_BINDHOST_SETTING,
                    NetworkService.GLOBAL_NETWORK_PUBLISHHOST_SETTING,
                    NetworkService.TCP_NO_DELAY,
                    NetworkService.TCP_KEEP_ALIVE,
                    NetworkService.TCP_REUSE_ADDRESS,
                    NetworkService.TCP_SEND_BUFFER_SIZE,
                    NetworkService.TCP_RECEIVE_BUFFER_SIZE,
                    NetworkService.TCP_CONNECT_TIMEOUT,
                    IndexSettings.QUERY_STRING_ANALYZE_WILDCARD,
                    IndexSettings.QUERY_STRING_ALLOW_LEADING_WILDCARD,
                    ScriptService.SCRIPT_CACHE_SIZE_SETTING,
                    ScriptService.SCRIPT_CACHE_EXPIRE_SETTING,
                    ScriptService.SCRIPT_MAX_SIZE_IN_BYTES,
                    ScriptService.SCRIPT_MAX_COMPILATIONS_RATE,
                    ScriptService.TYPES_ALLOWED_SETTING,
                    ScriptService.CONTEXTS_ALLOWED_SETTING,
                    IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING,
                    IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY,
                    IndicesRequestCache.INDICES_CACHE_QUERY_SIZE,
                    IndicesRequestCache.INDICES_CACHE_QUERY_EXPIRE,
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
                    DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING,
                    DiscoveryModule.DISCOVERY_TYPE_SETTING,
                    DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING,
                    FaultDetection.PING_RETRIES_SETTING,
                    FaultDetection.PING_TIMEOUT_SETTING,
                    FaultDetection.REGISTER_CONNECTION_LISTENER_SETTING,
                    FaultDetection.PING_INTERVAL_SETTING,
                    FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING,
                    ZenDiscovery.PING_TIMEOUT_SETTING,
                    ZenDiscovery.JOIN_TIMEOUT_SETTING,
                    ZenDiscovery.JOIN_RETRY_ATTEMPTS_SETTING,
                    ZenDiscovery.JOIN_RETRY_DELAY_SETTING,
                    ZenDiscovery.MAX_PINGS_FROM_ANOTHER_MASTER_SETTING,
                    ZenDiscovery.SEND_LEAVE_REQUEST_SETTING,
                    ZenDiscovery.MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING,
                    ZenDiscovery.MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING,
                    ZenDiscovery.MAX_PENDING_CLUSTER_STATES_SETTING,
                    SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING,
                    UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING,
                    UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT,
                    SearchService.DEFAULT_KEEPALIVE_SETTING,
                    SearchService.KEEPALIVE_INTERVAL_SETTING,
                    SearchService.MAX_KEEPALIVE_SETTING,
                    MultiBucketConsumerService.MAX_BUCKET_SETTING,
                    SearchService.LOW_LEVEL_CANCELLATION_SETTING,
                    Node.WRITE_PORTS_FILE_SETTING,
                    Node.NODE_NAME_SETTING,
                    Node.NODE_DATA_SETTING,
                    Node.NODE_MASTER_SETTING,
                    Node.NODE_INGEST_SETTING,
                    Node.NODE_ATTRIBUTES,
                    Node.NODE_LOCAL_STORAGE_SETTING,
                    TransportMasterNodeReadAction.FORCE_LOCAL_SETTING,
                    AutoCreateIndex.AUTO_CREATE_INDEX_SETTING,
                    BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX,
                    ClusterName.CLUSTER_NAME_SETTING,
                    Client.CLIENT_TYPE_SETTING_S,
                    ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING,
                    EsExecutors.PROCESSORS_SETTING,
                    ThreadContext.DEFAULT_HEADERS_SETTING,
                    TcpTransport.DEFAULT_FEATURES_SETTING,
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
                    PageCacheRecycler.LIMIT_HEAP_SETTING,
                    PageCacheRecycler.WEIGHT_BYTES_SETTING,
                    PageCacheRecycler.WEIGHT_INT_SETTING,
                    PageCacheRecycler.WEIGHT_LONG_SETTING,
                    PageCacheRecycler.WEIGHT_OBJECTS_SETTING,
                    PageCacheRecycler.TYPE_SETTING,
                    PluginsService.MANDATORY_SETTING,
                    BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING,
                    BootstrapSettings.MEMORY_LOCK_SETTING,
                    BootstrapSettings.SYSTEM_CALL_FILTER_SETTING,
                    BootstrapSettings.CTRLHANDLER_SETTING,
                    KeyStoreWrapper.SEED_SETTING,
                    IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING,
                    IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING,
                    IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING,
                    IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING,
                    IndexingMemoryController.SHARD_MEMORY_INTERVAL_TIME_SETTING,
                    ResourceWatcherService.ENABLED,
                    ResourceWatcherService.RELOAD_INTERVAL_HIGH,
                    ResourceWatcherService.RELOAD_INTERVAL_MEDIUM,
                    ResourceWatcherService.RELOAD_INTERVAL_LOW,
                    SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING,
                    ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING,
                    FastVectorHighlighter.SETTING_TV_HIGHLIGHT_MULTI_VALUE,
                    Node.BREAKER_TYPE_KEY,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    IndexGraveyard.SETTING_MAX_TOMBSTONES,
                    EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING
            )));

    public static List<SettingUpgrader<?>> BUILT_IN_SETTING_UPGRADERS = Collections.unmodifiableList(Arrays.asList(
            RemoteClusterAware.SEARCH_REMOTE_CLUSTER_SEEDS_UPGRADER,
            RemoteClusterAware.SEARCH_REMOTE_CLUSTERS_PROXY_UPGRADER,
            RemoteClusterService.SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE_UPGRADER));

}
