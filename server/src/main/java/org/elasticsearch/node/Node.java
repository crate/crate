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

package org.elasticsearch.node;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.net.ssl.SNIHostName;

import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Assertions;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.metadata.TemplateUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.BatchedRerouteService;
import org.elasticsearch.cluster.routing.LazilyInitializedRerouteService;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdMonitor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.NodeAndClusterIdStateListener;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.SettingUpgrader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.GatewayModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;

import io.crate.auth.AlwaysOKAuthentication;
import io.crate.auth.AuthSettings;
import io.crate.auth.Authentication;
import io.crate.auth.AuthenticationModule;
import io.crate.auth.HostBasedAuthentication;
import io.crate.blob.BlobModule;
import io.crate.blob.BlobService;
import io.crate.blob.v2.BlobIndicesModule;
import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;
import io.crate.execution.TransportExecutorModule;
import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.collect.CollectOperationModule;
import io.crate.execution.engine.collect.files.CopyModule;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.execution.engine.window.WindowFunctionModule;
import io.crate.execution.jobs.JobModule;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.transport.NodeDisconnectJobMonitorService;
import io.crate.expression.operator.OperatorModule;
import io.crate.expression.predicate.PredicateModule;
import io.crate.expression.reference.sys.check.SysChecksModule;
import io.crate.expression.reference.sys.check.node.SysNodeChecksModule;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.tablefunctions.TableFunctionModule;
import io.crate.lucene.ArrayMapperService;
import io.crate.metadata.CustomMetadataUpgraderLoader;
import io.crate.metadata.DanglingArtifactsService;
import io.crate.metadata.MetadataModule;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.MetadataBlobModule;
import io.crate.metadata.information.MetadataInformationModule;
import io.crate.metadata.pgcatalog.PgCatalogModule;
import io.crate.metadata.settings.session.SessionSettingModule;
import io.crate.metadata.sys.MetadataSysModule;
import io.crate.metadata.upgrade.IndexTemplateUpgrader;
import io.crate.metadata.upgrade.MetadataIndexUpgrader;
import io.crate.module.CrateCommonModule;
import io.crate.monitor.MonitorModule;
import io.crate.netty.NettyBootstrap;
import io.crate.plugin.CopyPlugin;
import io.crate.protocols.postgres.PgClientFactory;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.protocols.ssl.SslContextProviderService;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.LogicalReplicationSettings;
import io.crate.replication.logical.ShardReplicationService;
import io.crate.types.DataTypes;
import io.crate.user.UserLookup;
import io.crate.user.UserLookupService;
import io.crate.user.UserManagementModule;

/**
 * A node represent a node within a cluster ({@code cluster.name}). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 */
public class Node implements Closeable {
    public static final Setting<Boolean> WRITE_PORTS_FILE_SETTING =
        Setting.boolSetting("node.portsfile", false, Property.NodeScope);
    public static final Setting<Boolean> NODE_DATA_SETTING = Setting.boolSetting("node.data", true, Property.NodeScope);
    public static final Setting<Boolean> NODE_MASTER_SETTING =
        Setting.boolSetting("node.master", true, Property.NodeScope);

    /**
    * controls whether the node is allowed to persist things like metadata to disk
    * Note that this does not control whether the node stores actual indices (see
    * {@link #NODE_DATA_SETTING}). However, if this is false, {@link #NODE_DATA_SETTING}
    * and {@link #NODE_MASTER_SETTING} must also be false.
    *
    */
    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING = Setting.boolSetting("node.local_storage", true, Property.NodeScope);
    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Property.NodeScope);
    public static final Setting.AffixSetting<String> NODE_ATTRIBUTES = Setting.prefixKeySetting(
        "node.attr.",
        (key) -> new Setting<>(
            key,
            "",
            (value) -> {
                if (value.length() > 0 && (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length() - 1)))) {
                    throw new IllegalArgumentException(key + " cannot have leading or trailing whitespace " + "[" + value + "]");
                }
                if (value.length() > 0 && "node.attr.server_name".equals(key)) {
                    try {
                        new SNIHostName(value);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("invalid node.attr.server_name [" + value + "]", e);
                    }
                }
                return value;
            },
            DataTypes.STRING,
            Property.NodeScope
        )
    );

    public static final Setting<TimeValue> INITIAL_STATE_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.initial_state_timeout", TimeValue.timeValueSeconds(30), Property.NodeScope);

    private final Lifecycle lifecycle = new Lifecycle();

    /**
     * Logger initialized in the ctor because if it were initialized statically
     * then it wouldn't get the node name.
     */
    private final Logger logger;
    private final Injector injector;
    private final Settings settings;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final PluginsService pluginsService;
    private final NodeClient client;
    private final Collection<LifecycleComponent> pluginLifecycleComponents;
    private final LocalNodeFactory localNodeFactory;
    private final NodeService nodeService;

    public Node(Environment environment) {
        this(environment, Collections.emptyList(), true);
    }

    /**
     * Constructs a node
     *
     * @param environment                the environment for this node
     * @param classpathPlugins           the plugins to be loaded from the classpath
     * @param forbidPrivateIndexSettings whether or not private index settings are forbidden when creating an index; this is used in the
     *                                   test framework for tests that rely on being able to set private settings
     */
    public Node(final Environment environment,
                Collection<Class<? extends Plugin>> classpathPlugins,
                boolean forbidPrivateIndexSettings) {
        logger = LogManager.getLogger(Node.class);
        final List<Closeable> resourcesToClose = new ArrayList<>(); // register everything we need to release in the case of an error
        boolean success = false;
        try {
            Settings tmpSettings = Settings.builder().put(environment.settings())
                .build();

            nodeEnvironment = new NodeEnvironment(tmpSettings, environment);
            resourcesToClose.add(nodeEnvironment);
            logger.info("node name [{}], node ID [{}], cluster name [{}]",
                    NODE_NAME_SETTING.get(tmpSettings), nodeEnvironment.nodeId(),
                    ClusterName.CLUSTER_NAME_SETTING.get(tmpSettings).value());

            final JvmInfo jvmInfo = JvmInfo.jvmInfo();
            logVersion(logger, jvmInfo);

            if (logger.isDebugEnabled()) {
                logger.debug("using config [{}], data [{}], logs [{}], plugins [{}]",
                             environment.configFile(),
                             Arrays.toString(environment.dataFiles()),
                             environment.logsFile(),
                             environment.pluginsFile());
            }

            this.pluginsService = new PluginsService(tmpSettings, environment.configFile(), environment.modulesFile(),
                environment.pluginsFile(), classpathPlugins);
            this.settings = pluginsService.updatedSettings();
            final Set<DiscoveryNodeRole> possibleRoles = Stream.concat(
                    DiscoveryNodeRole.BUILT_IN_ROLES.stream(),
                    pluginsService.filterPlugins(Plugin.class)
                            .stream()
                            .map(Plugin::getRoles)
                            .flatMap(Set::stream))
                    .collect(Collectors.toSet());
            DiscoveryNode.setPossibleRoles(possibleRoles);
            localNodeFactory = new LocalNodeFactory(settings, nodeEnvironment.nodeId());

            // create the environment based on the finalized (processed) view of the settings
            // this is just to makes sure that people get the same settings, no matter where they ask them from
            this.environment = new Environment(this.settings, environment.configFile());
            Environment.assertEquivalent(environment, this.environment);

            final ThreadPool threadPool = new ThreadPool(settings);
            resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));

            final List<Setting<?>> additionalSettings = new ArrayList<>(pluginsService.getPluginSettings());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            client = new NodeClient(settings, threadPool);
            final AnalysisModule analysisModule = new AnalysisModule(
                this.environment,
                pluginsService.filterPlugins(AnalysisPlugin.class)
            );
            // this is as early as we can validate settings at this point. we already pass them to ScriptModule as well as ThreadPool
            // so we might be late here already

            final Set<SettingUpgrader<?>> settingsUpgraders = pluginsService.filterPlugins(Plugin.class)
                .stream()
                .map(Plugin::getSettingUpgraders)
                .flatMap(List::stream)
                .collect(Collectors.toSet());

            final SettingsModule settingsModule = new SettingsModule(this.settings, additionalSettings, settingsUpgraders);
            final NetworkService networkService = new NetworkService(
                getCustomNameResolvers(pluginsService.filterPlugins(DiscoveryPlugin.class)));

            final List<ClusterPlugin> clusterPlugins = pluginsService.filterPlugins(ClusterPlugin.class);
            final ClusterService clusterService = new ClusterService(
                settings,
                settingsModule.getClusterSettings(),
                threadPool
            );
            resourcesToClose.add(clusterService);

            final LazilyInitializedRerouteService lazilyInitializedRerouteService = new LazilyInitializedRerouteService();
            final DiskThresholdMonitor diskThresholdMonitor = new DiskThresholdMonitor(
                settings,
                clusterService::state,
                clusterService.getClusterSettings(),
                client,
                threadPool::relativeTimeInMillis,
                lazilyInitializedRerouteService
            );
            final ClusterInfoService clusterInfoService = newClusterInfoService(
                settings,
                clusterService,
                threadPool,
                client);
            clusterInfoService.addListener(diskThresholdMonitor::onNewInfo);

            ModulesBuilder modules = new ModulesBuilder();
            // plugin modules must be added here, before others or we can get crazy injection errors...
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }

            modules.add(new BlobModule());
            if (Node.NODE_DATA_SETTING.get(settings)) {
                // the actual blob indices module is only available on data nodes. the blobservice on non data nodes will
                // handle the requests redirection to data nodes.
                modules.add(new BlobIndicesModule());
            }
            modules.add(new CrateCommonModule());
            modules.add(new TransportExecutorModule());
            modules.add(new JobModule());
            modules.add(new CollectOperationModule());
            modules.add(new MetadataModule());
            modules.add(new MetadataSysModule());
            modules.add(new MetadataBlobModule());
            modules.add(new PgCatalogModule());
            modules.add(new MetadataInformationModule());
            modules.add(new OperatorModule());
            modules.add(new PredicateModule());
            modules.add(new MonitorModule());
            modules.add(new SysChecksModule());
            modules.add(new SysNodeChecksModule());
            modules.add(new UserManagementModule());
            modules.add(new AuthenticationModule());
            modules.add(new SessionSettingModule());
            modules.add(new AggregationImplModule());
            modules.add(new ScalarFunctionModule());
            modules.add(new TableFunctionModule(settings));
            modules.add(new WindowFunctionModule());

            final MonitorService monitorService = new MonitorService(settings,
                                                                     nodeEnvironment,
                                                                     threadPool,
                                                                     clusterInfoService);
            ClusterModule clusterModule = new ClusterModule(settings,
                                                            clusterService,
                                                            clusterPlugins,
                                                            clusterInfoService);
            modules.add(clusterModule);
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            modules.add(indicesModule);

            BooleanQuery.setMaxClauseCount(SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING.get(settings));

            CircuitBreakerService circuitBreakerService = new HierarchyCircuitBreakerService(settings, settingsModule.getClusterSettings());
            resourcesToClose.add(circuitBreakerService);
            modules.add(new GatewayModule());


            PageCacheRecycler pageCacheRecycler = createPageCacheRecycler(settings);
            BigArrays bigArrays = createBigArrays(pageCacheRecycler, circuitBreakerService);
            modules.add(settingsModule);
            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(
                NetworkModule.getNamedWriteables().stream(),
                IndicesModule.getNamedWriteables().stream(),
                pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedWriteables().stream()),
                ClusterModule.getNamedWriteables().stream(),
                MetadataModule.getNamedWriteables().stream())
                .flatMap(Function.identity()).collect(Collectors.toList());
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedXContent().stream()),
                ClusterModule.getNamedXWriteables().stream(),
                MetadataModule.getNamedXContents().stream())
                .flatMap(Function.identity()).collect(Collectors.toList()));
            final MetaStateService metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry);
            final PersistedClusterStateService persistedClusterStateService
                = new PersistedClusterStateService(nodeEnvironment, xContentRegistry, bigArrays, clusterService.getClusterSettings(),
                threadPool::relativeTimeInMillis);

            // collect engine factory providers from server and from plugins
            final Collection<EnginePlugin> enginePlugins = pluginsService.filterPlugins(EnginePlugin.class);
            final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders =
                Stream.concat(
                    indicesModule.getEngineFactories().stream(),
                    enginePlugins.stream().map(plugin -> plugin::getEngineFactory))
                    .collect(Collectors.toList());


            final Map<String, IndexStorePlugin.DirectoryFactory> indexStoreFactories =
                pluginsService.filterPlugins(IndexStorePlugin.class)
                    .stream()
                    .map(IndexStorePlugin::getDirectoryFactories)
                    .flatMap(m -> m.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final IndicesService indicesService = new IndicesService(
                settings,
                pluginsService,
                nodeEnvironment,
                xContentRegistry,
                analysisModule.getAnalysisRegistry(),
                indicesModule.getMapperRegistry(),
                namedWriteableRegistry,
                threadPool,
                settingsModule.getIndexScopedSettings(),
                circuitBreakerService,
                bigArrays,
                client,
                metaStateService,
                engineFactoryProviders,
                indexStoreFactories);

            final AliasValidator aliasValidator = new AliasValidator();
            final ShardLimitValidator shardLimitValidator = new ShardLimitValidator(settings, clusterService);
            final MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
                settings,
                clusterService,
                indicesService,
                clusterModule.getAllocationService(),
                aliasValidator,
                shardLimitValidator,
                environment,
                settingsModule.getIndexScopedSettings(),
                threadPool,
                xContentRegistry,
                forbidPrivateIndexSettings
            );

            final Collection<Object> pluginComponents = pluginsService.filterPlugins(Plugin.class).stream()
                .flatMap(p -> p.createComponents(client, clusterService, threadPool,
                                                 xContentRegistry, environment, nodeEnvironment,
                                                 namedWriteableRegistry).stream())
                .collect(Collectors.toList());

            ActionModule actionModule = new ActionModule(
                settings,
                settingsModule.getClusterSettings(),
                pluginsService.filterPlugins(ActionPlugin.class));
            modules.add(actionModule);

            UserLookup userLookup = new UserLookupService(clusterService);
            var authentication = AuthSettings.AUTH_HOST_BASED_ENABLED_SETTING.get(settings)
                ? new HostBasedAuthentication(settings, userLookup, SystemDefaultDnsResolver.INSTANCE)
                : new AlwaysOKAuthentication(userLookup);

            final SslContextProvider sslContextProvider = new SslContextProvider(settings);
            final NettyBootstrap nettyBootstrap = new NettyBootstrap(settings);
            nettyBootstrap.start();
            final NetworkModule networkModule = new NetworkModule(
                settings,
                pluginsService.filterPlugins(NetworkPlugin.class),
                threadPool,
                bigArrays,
                pageCacheRecycler,
                circuitBreakerService,
                namedWriteableRegistry,
                xContentRegistry,
                networkService,
                nettyBootstrap,
                authentication,
                sslContextProvider,
                client);

            List<UnaryOperator<Map<String, Metadata.Custom>>> customMetadataUpgraders =
                pluginsService.filterPlugins(Plugin.class).stream()
                    .map(Plugin::getCustomMetadataUpgrader)
                    .collect(Collectors.toList());
            customMetadataUpgraders.add(new CustomMetadataUpgraderLoader(settings));

            List<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders =
                pluginsService.filterPlugins(Plugin.class).stream()
                    .map(Plugin::getIndexTemplateMetadataUpgrader)
                    .collect(Collectors.toList());
            indexTemplateMetadataUpgraders.add(new IndexTemplateUpgrader());

            List<BiFunction<IndexMetadata, IndexTemplateMetadata, IndexMetadata>> indexMetadataUpgraders =
                pluginsService.filterPlugins(Plugin.class).stream()
                    .map(Plugin::getIndexMetadataUpgrader).collect(Collectors.toList());
            indexMetadataUpgraders.add(new MetadataIndexUpgrader());

            final MetadataUpgrader metadataUpgrader = new MetadataUpgrader(customMetadataUpgraders,
                                                                           indexTemplateMetadataUpgraders);
            final MetadataIndexUpgradeService metadataIndexUpgradeService = new MetadataIndexUpgradeService(settings,
                                                                                                            xContentRegistry,
                                                                                                            indicesModule.getMapperRegistry(),
                                                                                                            settingsModule.getIndexScopedSettings(),
                                                                                                            indexMetadataUpgraders);
            new TemplateUpgradeService(client, clusterService, threadPool, indexTemplateMetadataUpgraders);
            final Netty4Transport transport = new Netty4Transport(
                settings,
                Version.CURRENT,
                threadPool,
                networkService,
                pageCacheRecycler,
                namedWriteableRegistry,
                circuitBreakerService,
                nettyBootstrap,
                authentication,
                sslContextProvider
            );
            final TransportService transportService = newTransportService(
                settings,
                transport,
                threadPool,
                localNodeFactory,
                settingsModule.getClusterSettings()
            );
            final GatewayMetaState gatewayMetaState = new GatewayMetaState();
            final HttpServerTransport httpServerTransport = newHttpTransport(networkModule);

            PgClientFactory pgClientFactory = newPgClientFactory(
                settings,
                transportService,
                transport,
                sslContextProvider,
                pageCacheRecycler,
                nettyBootstrap
            );
            RemoteClusters remoteClusters = new RemoteClusters(settings, threadPool, pgClientFactory, transportService);
            resourcesToClose.add(remoteClusters);

            final LogicalReplicationSettings logicalReplicationSettings = new LogicalReplicationSettings(
                settings,
                clusterService
            );
            final LogicalReplicationService logicalReplicationService = new LogicalReplicationService(
                settings,
                settingsModule.getIndexScopedSettings(),
                clusterService,
                remoteClusters,
                threadPool,
                client,
                clusterModule.getAllocationService(),
                logicalReplicationSettings
            );
            resourcesToClose.add(logicalReplicationService);


            LogicalReplicationSettings replicationSettings = new LogicalReplicationSettings(settings, clusterService);

            RepositoriesModule repositoriesModule = new RepositoriesModule(
                this.environment,
                pluginsService.filterPlugins(RepositoryPlugin.class),
                transportService,
                clusterService,
                logicalReplicationService,
                remoteClusters,
                threadPool,
                xContentRegistry,
                replicationSettings);
            modules.add(repositoriesModule);

            CopyModule copyModule = new CopyModule(pluginsService.filterPlugins(CopyPlugin.class));
            modules.add(copyModule);

            RepositoriesService repositoryService = repositoriesModule.repositoryService();
            logicalReplicationService.repositoriesService(repositoryService);

            final SnapshotsService snapshotsService = new SnapshotsService(
                settings,
                clusterService,
                repositoryService,
                threadPool
            );

            final SnapshotShardsService snapshotShardsService = new SnapshotShardsService(
                settings,
                clusterService,
                repositoryService,
                threadPool,
                transportService,
                indicesService
            );

            RestoreService restoreService = new RestoreService(
                clusterService,
                repositoryService,
                clusterModule.getAllocationService(),
                metadataCreateIndexService,
                metadataIndexUpgradeService,
                clusterService.getClusterSettings(),
                shardLimitValidator
            );
            logicalReplicationService.restoreService(restoreService);

            final RerouteService rerouteService = new BatchedRerouteService(
                clusterService,
                clusterModule.getAllocationService()::reroute
            );
            lazilyInitializedRerouteService.setRerouteService(rerouteService);
            final DiscoveryModule discoveryModule = new DiscoveryModule(
                this.settings,
                transportService,
                namedWriteableRegistry,
                networkService,
                clusterService.getMasterService(),
                clusterService.getClusterApplierService(),
                clusterService.getClusterSettings(),
                pluginsService.filterPlugins(DiscoveryPlugin.class),
                clusterModule.getAllocationService(),
                environment.configFile(),
                gatewayMetaState,
                rerouteService
            );
            this.nodeService = new NodeService(monitorService, indicesService, transportService);

            modules.add(b -> {
                    b.bind(Node.class).toInstance(this);
                    b.bind(NodeService.class).toInstance(nodeService);
                    b.bind(NamedXContentRegistry.class).toInstance(xContentRegistry);
                    b.bind(PluginsService.class).toInstance(pluginsService);
                    b.bind(Client.class).toInstance(client);
                    b.bind(NodeClient.class).toInstance(client);
                    b.bind(Environment.class).toInstance(this.environment);
                    b.bind(ThreadPool.class).toInstance(threadPool);
                    b.bind(NodeEnvironment.class).toInstance(nodeEnvironment);
                    b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                    b.bind(BigArrays.class).toInstance(bigArrays);
                    b.bind(PageCacheRecycler.class).toInstance(pageCacheRecycler);
                    b.bind(AnalysisRegistry.class).toInstance(analysisModule.getAnalysisRegistry());
                    b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                    b.bind(MetadataUpgrader.class).toInstance(metadataUpgrader);
                    b.bind(MetaStateService.class).toInstance(metaStateService);
                    b.bind(PersistedClusterStateService.class).toInstance(persistedClusterStateService);
                    b.bind(IndicesService.class).toInstance(indicesService);
                    b.bind(AliasValidator.class).toInstance(aliasValidator);
                    b.bind(MetadataCreateIndexService.class).toInstance(metadataCreateIndexService);
                    b.bind(Transport.class).toInstance(transport);
                    b.bind(Netty4Transport.class).toInstance(transport);
                    b.bind(TransportService.class).toInstance(transportService);
                    b.bind(NetworkService.class).toInstance(networkService);
                    b.bind(MetadataIndexUpgradeService.class).toInstance(metadataIndexUpgradeService);
                    b.bind(ClusterInfoService.class).toInstance(clusterInfoService);
                    b.bind(GatewayMetaState.class).toInstance(gatewayMetaState);
                    b.bind(RepositoriesService.class).toInstance(repositoryService);
                    b.bind(SnapshotsService.class).toInstance(snapshotsService);
                    b.bind(SnapshotShardsService.class).toInstance(snapshotShardsService);
                    b.bind(RestoreService.class).toInstance(restoreService);
                    b.bind(Discovery.class).toInstance(discoveryModule.getDiscovery());
                    {
                        RecoverySettings recoverySettings = new RecoverySettings(settings,
                                                                                 settingsModule.getClusterSettings());
                        processRecoverySettings(settingsModule.getClusterSettings(), recoverySettings);
                        b.bind(PeerRecoverySourceService.class).toInstance(new PeerRecoverySourceService(transportService,
                                                                                                         indicesService,
                                                                                                         recoverySettings));
                        b.bind(PeerRecoveryTargetService.class).toInstance(new PeerRecoveryTargetService(threadPool,
                                                                                                         transportService,
                                                                                                         recoverySettings,
                                                                                                         clusterService));
                    }
                    b.bind(HttpServerTransport.class).toInstance(httpServerTransport);
                    b.bind(ShardLimitValidator.class).toInstance(shardLimitValidator);
                    b.bind(NettyBootstrap.class).toInstance(nettyBootstrap);
                    b.bind(SslContextProvider.class).toInstance(sslContextProvider);
                    b.bind(RerouteService.class).toInstance(rerouteService);
                    b.bind(UserLookup.class).toInstance(userLookup);
                    b.bind(Authentication.class).toInstance(authentication);
                    b.bind(LogicalReplicationService.class).toInstance(logicalReplicationService);
                    b.bind(LogicalReplicationSettings.class).toInstance(logicalReplicationSettings);
                    b.bind(RemoteClusters.class).toInstance(remoteClusters);
                    pluginComponents.stream().forEach(p -> b.bind((Class) p.getClass()).toInstance(p));
                }
            );
            injector = modules.createInjector();

            // TODO hack around circular dependencies problems in AllocationService
            clusterModule.getAllocationService().setGatewayAllocator(injector.getInstance(GatewayAllocator.class));

            List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream()
                .filter(p -> p instanceof LifecycleComponent)
                .map(p -> (LifecycleComponent) p).collect(Collectors.toList());
            pluginLifecycleComponents.addAll(pluginsService.getGuiceServiceClasses().stream()
                                                 .map(injector::getInstance).collect(Collectors.toList()));
            resourcesToClose.addAll(pluginLifecycleComponents);
            resourcesToClose.add(injector.getInstance(PeerRecoverySourceService.class));
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);
            client.initialize(injector.getInstance(new Key<Map<ActionType, TransportAction>>() {}));

            logger.info("initialized");

            success = true;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to bind service", ex);
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(resourcesToClose);
            }
        }
    }


    // Overridable for testing
    protected PgClientFactory newPgClientFactory(Settings settings,
                                                 TransportService transportService,
                                                 Netty4Transport transport,
                                                 SslContextProvider sslContextProvider,
                                                 PageCacheRecycler pageCacheRecycler,
                                                 NettyBootstrap nettyBootstrap) {
        return new PgClientFactory(
            settings,
            transportService,
            transport,
            sslContextProvider,
            pageCacheRecycler,
            nettyBootstrap
        );
    }

    private void logVersion(Logger logger, JvmInfo jvmInfo) {
        logger.info(
            "version[{}], pid[{}], build[{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]",
            Version.displayVersion(Version.CURRENT, Version.CURRENT.isSnapshot()),
            jvmInfo.pid(),
            Build.CURRENT.hashShort(),
            Build.CURRENT.timestamp(),
            Constants.OS_NAME,
            Constants.OS_VERSION,
            Constants.OS_ARCH,
            Constants.JVM_VENDOR,
            Constants.JVM_NAME,
            Constants.JAVA_VERSION,
            Constants.JVM_VERSION);
        warnIfPreRelease(Version.CURRENT, Version.CURRENT.isSnapshot(), logger);
    }

    private static void warnIfPreRelease(final Version version, final boolean isSnapshot, final Logger logger) {
        if (!version.isRelease() || isSnapshot) {
            logger.warn(
                "version [{}] is a pre-release version of CrateDB and is not suitable for production",
                Version.displayVersion(version, isSnapshot));
        }
    }

    protected TransportService newTransportService(Settings settings,
                                                   Transport transport,
                                                   ThreadPool threadPool,
                                                   Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                                   ClusterSettings clusterSettings) {
        return new TransportService(settings, transport, threadPool, localNodeFactory, clusterSettings);
    }

    protected void processRecoverySettings(ClusterSettings clusterSettings, RecoverySettings recoverySettings) {
        // Noop in production, overridden by tests
    }

    /**
     * The settings that are used by this node. Contains original settings as well as additional settings provided by plugins.
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * A client that can be used to execute actions (operations) against the cluster.
     */
    public Client client() {
        return client;
    }

    /**
     * Returns the environment of the node
     */
    public Environment getEnvironment() {
        return environment;
    }

    /**
     * Returns the {@link NodeEnvironment} instance of this node
     */
    public NodeEnvironment getNodeEnvironment() {
        return nodeEnvironment;
    }


    /**
     * Start the node. If the node is already started, this method is no-op.
     */
    public Node start() throws NodeValidationException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        logger.info("starting ...");
        pluginLifecycleComponents.forEach(LifecycleComponent::start);

        injector.getInstance(BlobService.class).start();

        injector.getInstance(DecommissioningService.class).start();
        injector.getInstance(NodeDisconnectJobMonitorService.class).start();
        injector.getInstance(JobsLogService.class).start();
        injector.getInstance(PostgresNetty.class).start();
        injector.getInstance(TasksService.class).start();
        injector.getInstance(Schemas.class).start();
        injector.getInstance(ArrayMapperService.class).start();
        injector.getInstance(DanglingArtifactsService.class).start();
        injector.getInstance(SslContextProviderService.class).start();

        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(SnapshotShardsService.class).start();
        nodeService.getMonitorService().start();

        final ClusterService clusterService = injector.getInstance(ClusterService.class);

        final NodeConnectionsService nodeConnectionsService = injector.getInstance(NodeConnectionsService.class);
        nodeConnectionsService.start();
        clusterService.setNodeConnectionsService(nodeConnectionsService);

        injector.getInstance(GatewayService.class).start();
        Discovery discovery = injector.getInstance(Discovery.class);
        clusterService.getMasterService().setClusterStatePublisher(discovery::publish);

        HttpServerTransport httpServerTransport = injector.getInstance(HttpServerTransport.class);
        httpServerTransport.start();
        // CRATE_PATCH: add http publish address to the discovery node
        TransportAddress publishAddress = httpServerTransport.info().address().publishAddress();
        localNodeFactory.httpPublishAddress = publishAddress.getAddress() + ':' + publishAddress.getPort();

        // Start the transport service now so the publish address will be added to the local disco node in ClusterService
        TransportService transportService = injector.getInstance(TransportService.class);
        transportService.start();
        assert localNodeFactory.getNode() != null;
        assert transportService.getLocalNode().equals(localNodeFactory.getNode())
            : "transportService has a different local node than the factory provided";
        injector.getInstance(PeerRecoverySourceService.class).start();

        // Load (and maybe upgrade) the metadata stored on disk
        final GatewayMetaState gatewayMetaState = injector.getInstance(GatewayMetaState.class);
        gatewayMetaState.start(
            settings(),
            transportService,
            clusterService,
            injector.getInstance(MetaStateService.class),
            injector.getInstance(MetadataIndexUpgradeService.class),
            injector.getInstance(MetadataUpgrader.class),
            injector.getInstance(PersistedClusterStateService.class)
        );
        if (Assertions.ENABLED) {
            try {
                assert injector.getInstance(MetaStateService.class).loadFullState().v1().isEmpty();
                final NodeMetadata nodeMetaData = NodeMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY,
                                                                                      nodeEnvironment.nodeDataPaths());
                assert nodeMetaData != null;
                assert nodeMetaData.nodeVersion().equals(Version.CURRENT);
                assert nodeMetaData.nodeId().equals(localNodeFactory.getNode().getId());
            } catch (IOException e) {
                assert false : e;
            }
        }
        // we load the global state here (the persistent part of the cluster state stored on disk) to
        // pass it to the bootstrap checks to allow plugins to enforce certain preconditions based on the recovered state.
        final Metadata onDiskMetadata = gatewayMetaState.getPersistedState().getLastAcceptedState().metadata();
        assert onDiskMetadata != null : "metadata is null but shouldn't"; // this is never null
        validateNodeBeforeAcceptingRequests(transportService.boundAddress(),
            pluginsService.filterPlugins(Plugin.class).stream()
                .flatMap(p -> p.getBootstrapChecks().stream()).collect(Collectors.toList()));

        // start after transport service so the local disco is known
        discovery.start(); // start before cluster service so that it can set initial state on ClusterApplierService
        clusterService.start();
        assert clusterService.localNode().equals(localNodeFactory.getNode())
            : "clusterService has a different local node than the factory provided";
        transportService.acceptIncomingRequests();
        discovery.startInitialJoin();
        final TimeValue initialStateTimeout = INITIAL_STATE_TIMEOUT_SETTING.get(settings);
        configureNodeAndClusterIdStateListener(clusterService);

        if (initialStateTimeout.millis() > 0) {
            final ThreadPool thread = injector.getInstance(ThreadPool.class);
            ClusterState clusterState = clusterService.state();
            ClusterStateObserver observer = new ClusterStateObserver(clusterState, clusterService, null, logger);

            if (clusterState.nodes().getMasterNodeId() == null) {
                logger.debug("waiting to join the cluster. timeout [{}]", initialStateTimeout);
                final CountDownLatch latch = new CountDownLatch(1);
                observer.waitForNextChange(
                    new ClusterStateObserver.Listener() {

                        @Override
                        public void onNewClusterState(ClusterState state) {
                            latch.countDown();
                        }

                        @Override
                        public void onClusterServiceClose() {
                            latch.countDown();
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            logger.warn("timed out while waiting for initial discovery state - timeout: {}",
                                initialStateTimeout);
                            latch.countDown();
                        }
                    },
                    state -> state.nodes().getMasterNodeId() != null,
                    initialStateTimeout
                );
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new ElasticsearchTimeoutException("Interrupted while waiting for initial discovery state");
                }
            }
        }

        if (WRITE_PORTS_FILE_SETTING.get(settings)) {
            TransportService transport = injector.getInstance(TransportService.class);
            writePortsFile("transport", transport.boundAddress());
            HttpServerTransport http = injector.getInstance(HttpServerTransport.class);
            writePortsFile("http", http.boundAddress());
        }

        logger.info("started");

        pluginsService.filterPlugins(ClusterPlugin.class).forEach(ClusterPlugin::onNodeStarted);

        return this;
    }

    protected void configureNodeAndClusterIdStateListener(ClusterService clusterService) {
        NodeAndClusterIdStateListener.getAndSetNodeIdAndClusterId(clusterService);
    }

    private Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        logger.info("stopping ...");

        injector.getInstance(HttpServerTransport.class).stop();

        injector.getInstance(SnapshotsService.class).stop();
        injector.getInstance(SnapshotShardsService.class).stop();
        // stop any changes happening as a result of cluster state changes
        injector.getInstance(IndicesClusterStateService.class).stop();
        // close discovery early to not react to pings anymore.
        // This can confuse other nodes and delay things - mostly if we're the master and we're running tests.
        injector.getInstance(Discovery.class).stop();
        // we close indices first, so operations won't be allowed on it
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(NodeConnectionsService.class).stop();
        nodeService.getMonitorService().stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(TransportService.class).stop();

        injector.getInstance(DecommissioningService.class).stop();
        injector.getInstance(NodeDisconnectJobMonitorService.class).stop();
        injector.getInstance(JobsLogService.class).stop();
        injector.getInstance(PostgresNetty.class).stop();
        injector.getInstance(TasksService.class).stop();
        injector.getInstance(Schemas.class).stop();
        injector.getInstance(ArrayMapperService.class).stop();
        injector.getInstance(DanglingArtifactsService.class).stop();
        injector.getInstance(SslContextProviderService.class).stop();
        injector.getInstance(BlobService.class).stop();

        pluginLifecycleComponents.forEach(LifecycleComponent::stop);
        // we should stop this last since it waits for resources to get released
        // if we had scroll searchers etc or recovery going on we wait for to finish.
        injector.getInstance(IndicesService.class).stop();
        injector.getInstance(NettyBootstrap.class).stop();
        logger.info("stopped");

        return this;
    }

    // During concurrent close() calls we want to make sure that all of them return after the node has completed it's shutdown cycle.
    // If not, the hook that is added in Bootstrap#setup() will be useless:
    // close() might not be executed, in case another (for example api) call to close() has already set some lifecycles to stopped.
    // In this case the process will be terminated even if the first call to close() has not finished yet.
    @Override
    public synchronized void close() throws IOException {
        synchronized (lifecycle) {
            if (lifecycle.started()) {
                stop();
            }
            if (!lifecycle.moveToClosed()) {
                return;
            }
        }

        logger.info("closing ...");
        List<Closeable> toClose = new ArrayList<>();
        StopWatch stopWatch = new StopWatch("node_close");
        toClose.add(() -> stopWatch.start("node_service"));
        toClose.add(nodeService);
        toClose.add(() -> stopWatch.stop().start("http"));
        toClose.add(injector.getInstance(HttpServerTransport.class));

        toClose.add(() -> stopWatch.stop().start("logical_replication_service"));
        toClose.add(injector.getInstance(LogicalReplicationService.class));
        toClose.add(() -> stopWatch.stop().start("logical_replication_shard_service"));
        toClose.add(injector.getInstance(ShardReplicationService.class));

        toClose.add(() -> stopWatch.stop().start("snapshot_service"));
        toClose.add(injector.getInstance(SnapshotsService.class));
        toClose.add(injector.getInstance(SnapshotShardsService.class));
        toClose.add(() -> stopWatch.stop().start("client"));
        Releasables.close(injector.getInstance(Client.class));
        toClose.add(() -> stopWatch.stop().start("indices_cluster"));
        toClose.add(injector.getInstance(IndicesClusterStateService.class));
        toClose.add(() -> stopWatch.stop().start("indices"));
        toClose.add(injector.getInstance(IndicesService.class));
        // close filter/fielddata caches after indices
        toClose.add(injector.getInstance(IndicesStore.class));
        toClose.add(injector.getInstance(PeerRecoverySourceService.class));

        toClose.add(() -> stopWatch.stop().start("remote_clusters"));
        toClose.add(injector.getInstance(RemoteClusters.class));

        toClose.add(() -> stopWatch.stop().start("routing"));
        toClose.add(() -> stopWatch.stop().start("cluster"));
        toClose.add(injector.getInstance(ClusterService.class));
        toClose.add(() -> stopWatch.stop().start("node_connections_service"));
        toClose.add(injector.getInstance(NodeConnectionsService.class));
        toClose.add(() -> stopWatch.stop().start("discovery"));
        toClose.add(injector.getInstance(Discovery.class));
        toClose.add(() -> stopWatch.stop().start("monitor"));
        toClose.add(nodeService.getMonitorService());
        toClose.add(() -> stopWatch.stop().start("gateway"));
        toClose.add(injector.getInstance(GatewayService.class));
        toClose.add(() -> stopWatch.stop().start("transport"));
        toClose.add(injector.getInstance(TransportService.class));

        toClose.add(() -> stopWatch.stop().start("gateway_meta_state"));
        toClose.add(injector.getInstance(GatewayMetaState.class));

        toClose.add(() -> stopWatch.stop().start("node_environment"));
        toClose.add(injector.getInstance(NodeEnvironment.class));

        toClose.add(() -> stopWatch.stop().start("decommission_service"));
        toClose.add(injector.getInstance(DecommissioningService.class));
        toClose.add(() -> stopWatch.stop().start("node_disconnect_job_monitor_service"));
        toClose.add(injector.getInstance(NodeDisconnectJobMonitorService.class));
        toClose.add(() -> stopWatch.stop().start("jobs_log_service"));
        toClose.add(injector.getInstance(JobsLogService.class));
        toClose.add(() -> stopWatch.stop().start("postgres_netty"));
        toClose.add(injector.getInstance(PostgresNetty.class));
        toClose.add(() -> stopWatch.stop().start("tasks_service"));
        toClose.add(injector.getInstance(TasksService.class));
        toClose.add(() -> stopWatch.stop().start("schemas"));
        toClose.add(injector.getInstance(Schemas.class));
        toClose.add(() -> stopWatch.stop().start("array_mapper_service"));
        toClose.add(injector.getInstance(ArrayMapperService.class));
        toClose.add(() -> stopWatch.stop().start("dangling_artifacts_service"));
        toClose.add(injector.getInstance(DanglingArtifactsService.class));
        toClose.add(() -> stopWatch.stop().start("ssl_context_provider_service"));
        toClose.add(injector.getInstance(SslContextProviderService.class));
        toClose.add(() -> stopWatch.stop().start("blob_service"));
        toClose.add(injector.getInstance(BlobService.class));
        toClose.add(() -> stopWatch.stop().start("netty_bootstrap"));
        toClose.add(injector.getInstance(NettyBootstrap.class));

        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Plugin.class));

        toClose.add(() -> stopWatch.stop().start("thread_pool"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdown());
        // Don't call shutdownNow here, it might break ongoing operations on Lucene indices.
        // See https://issues.apache.org/jira/browse/LUCENE-7248. We call shutdownNow in
        // awaitClose if the node doesn't finish closing within the specified time.
        toClose.add(() -> stopWatch.stop());



        if (logger.isTraceEnabled()) {
            logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint());
        }
        IOUtils.close(toClose);
        logger.info("closed");
    }

    /**
     * Wait for this node to be effectively closed.
     */
    // synchronized to prevent running concurrently with close()
    public synchronized boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (lifecycle.closed() == false) {
            // We don't want to shutdown the threadpool or interrupt threads on a node that is not
            // closed yet.
            throw new IllegalStateException("Call close() first");
        }


        ThreadPool threadPool = injector.getInstance(ThreadPool.class);
        final boolean terminated = ThreadPool.terminate(threadPool, timeout, timeUnit);
        if (terminated) {
            // All threads terminated successfully. Because search, recovery and all other operations
            // that run on shards run in the threadpool, indices should be effectively closed by now.
            if (nodeService.awaitClose(0, TimeUnit.MILLISECONDS) == false) {
                throw new IllegalStateException("Some shards are still open after the threadpool terminated. " +
                        "Something is leaking index readers or store references.");
            }
        }
        return terminated;
    }

    /**
     * Returns {@code true} if the node is closed.
     */
    public boolean isClosed() {
        return lifecycle.closed();
    }

    public Injector injector() {
        return this.injector;
    }

    /**
     * Hook for validating the node after network
     * services are started but before the cluster service is started
     * and before the network service starts accepting incoming network
     * requests.
     *
     * @param boundTransportAddress the network addresses the node is
     *                              bound and publishing to
     */
    @SuppressWarnings("unused")
    protected void validateNodeBeforeAcceptingRequests(final BoundTransportAddress boundTransportAddress,
                                                       List<BootstrapCheck> bootstrapChecks) throws NodeValidationException {
    }

    /** Writes a file to the logs dir containing the ports for the given transport type */
    private void writePortsFile(String type, BoundTransportAddress boundAddress) {
        Path tmpPortsFile = environment.logsFile().resolve(type + ".ports.tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmpPortsFile, Charset.forName("UTF-8"))) {
            for (TransportAddress address : boundAddress.boundAddresses()) {
                InetAddress inetAddress = InetAddress.getByName(address.getAddress());
                writer.write(NetworkAddress.format(new InetSocketAddress(inetAddress, address.getPort())) + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write ports file", e);
        }
        Path portsFile = environment.logsFile().resolve(type + ".ports");
        try {
            Files.move(tmpPortsFile, portsFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rename ports file", e);
        }
    }

    /**
     * The {@link PluginsService} used to build this node's components.
     */
    protected PluginsService getPluginsService() {
        return pluginsService;
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    BigArrays createBigArrays(PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService) {
        return new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    PageCacheRecycler createPageCacheRecycler(Settings settings) {
        return new PageCacheRecycler(settings);
    }

    /**
     * Get Custom Name Resolvers list based on a Discovery Plugins list
     * @param discoveryPlugins Discovery plugins list
     */
    private List<NetworkService.CustomNameResolver> getCustomNameResolvers(List<DiscoveryPlugin> discoveryPlugins) {
        List<NetworkService.CustomNameResolver> customNameResolvers = new ArrayList<>();
        for (DiscoveryPlugin discoveryPlugin : discoveryPlugins) {
            NetworkService.CustomNameResolver customNameResolver = discoveryPlugin.getCustomNameResolver(settings);
            if (customNameResolver != null) {
                customNameResolvers.add(customNameResolver);
            }
        }
        return customNameResolvers;
    }

    /** Constructs a ClusterInfoService which may be mocked for tests. */
    protected ClusterInfoService newClusterInfoService(Settings settings,
                                                       ClusterService clusterService,
                                                       ThreadPool threadPool,
                                                       NodeClient client) {
        var service = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        // listen for state changes (this node starts/stops being the elected master, or new nodes are added)
        clusterService.addListener(service);
        return service;
    }

    /** Constructs a {@link org.elasticsearch.http.HttpServerTransport} which may be mocked for tests. */
    protected HttpServerTransport newHttpTransport(NetworkModule networkModule) {
        return networkModule.getHttpServerTransportSupplier().get();
    }

    private static class LocalNodeFactory implements Function<BoundTransportAddress, DiscoveryNode> {
        private final SetOnce<DiscoveryNode> localNode = new SetOnce<>();
        private final String persistentNodeId;
        private final Settings settings;

        @Nullable
        String httpPublishAddress;

        private LocalNodeFactory(Settings settings, String persistentNodeId) {
            this.persistentNodeId = persistentNodeId;
            this.settings = settings;
        }

        @Override
        public DiscoveryNode apply(BoundTransportAddress boundTransportAddress) {
            // CRATE_PATCH: use existing node attributes to pass and stream http_address between the nodes
            Map<String, String> attributes = new HashMap<>(NODE_ATTRIBUTES.getAsMap(settings));
            Set<DiscoveryNodeRole> roles = DiscoveryNode.getRolesFromSettings(settings);
            if (httpPublishAddress != null) {
                attributes.put("http_address", httpPublishAddress);
            }
            localNode.set(new DiscoveryNode(Node.NODE_NAME_SETTING.get(settings), persistentNodeId, boundTransportAddress.publishAddress(), attributes, roles, Version.CURRENT));
            return localNode.get();
        }

        DiscoveryNode getNode() {
            assert localNode.get() != null;
            return localNode.get();
        }
    }
}
