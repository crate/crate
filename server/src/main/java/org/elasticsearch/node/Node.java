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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.metadata.TemplateUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdMonitor;
import org.elasticsearch.cluster.service.ClusterService;
import javax.annotation.Nullable;
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
import io.crate.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import io.crate.common.io.IOUtils;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.GatewayModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
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
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;

import javax.net.ssl.SNIHostName;
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
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.node.DiscoveryNode.getRolesFromSettings;
import static org.elasticsearch.discovery.DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING;

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
            Property.NodeScope
        )
    );

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
    protected Node(final Environment environment,
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
            logger.info("node name [{}], node ID [{}]",
                        NODE_NAME_SETTING.get(tmpSettings), nodeEnvironment.nodeId());

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

            final DiskThresholdMonitor diskThresholdMonitor = new DiskThresholdMonitor(
                settings,
                clusterService::state,
                clusterService.getClusterSettings(),
                client);
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
            resourcesToClose.add(pageCacheRecycler);
            modules.add(settingsModule);
            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(
                NetworkModule.getNamedWriteables().stream(),
                indicesModule.getNamedWriteables().stream(),
                pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedWriteables().stream()),
                ClusterModule.getNamedWriteables().stream())
                .flatMap(Function.identity()).collect(Collectors.toList());
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Stream.of(
                NetworkModule.getNamedXContents().stream(),
                indicesModule.getNamedXContents().stream(),
                pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedXContent().stream()),
                ClusterModule.getNamedXWriteables().stream())
                .flatMap(Function.identity()).collect(toList()));
            final MetaStateService metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry);

            // collect engine factory providers from server and from plugins
            final Collection<EnginePlugin> enginePlugins = pluginsService.filterPlugins(EnginePlugin.class);
            final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders =
                Stream.concat(
                    indicesModule.getEngineFactories().stream(),
                    enginePlugins.stream().map(plugin -> plugin::getEngineFactory))
                    .collect(Collectors.toList());


            final Map<String, Function<IndexSettings, IndexStore>> indexStoreFactories =
                pluginsService.filterPlugins(IndexStorePlugin.class)
                    .stream()
                    .map(IndexStorePlugin::getIndexStoreFactories)
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
            final MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
                settings,
                clusterService,
                indicesService,
                clusterModule.getAllocationService(),
                aliasValidator,
                environment,
                settingsModule.getIndexScopedSettings(),
                threadPool,
                xContentRegistry,
                forbidPrivateIndexSettings);

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
                client);
            Collection<UnaryOperator<Map<String, Metadata.Custom>>> customMetadataUpgraders =
                pluginsService.filterPlugins(Plugin.class).stream()
                    .map(Plugin::getCustomMetadataUpgrader)
                    .collect(Collectors.toList());
            Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders =
                pluginsService.filterPlugins(Plugin.class).stream()
                    .map(Plugin::getIndexTemplateMetadataUpgrader)
                    .collect(Collectors.toList());
            Collection<UnaryOperator<IndexMetadata>> indexMetadataUpgraders = pluginsService.filterPlugins(Plugin.class).stream()
                .map(Plugin::getIndexMetadataUpgrader).collect(Collectors.toList());
            final MetadataUpgrader metadataUpgrader = new MetadataUpgrader(customMetadataUpgraders,
                                                                           indexTemplateMetadataUpgraders);
            final MetadataIndexUpgradeService metadataIndexUpgradeService = new MetadataIndexUpgradeService(settings,
                                                                                                            xContentRegistry,
                                                                                                            indicesModule.getMapperRegistry(),
                                                                                                            settingsModule.getIndexScopedSettings(),
                                                                                                            indexMetadataUpgraders);
            new TemplateUpgradeService(client, clusterService, threadPool, indexTemplateMetadataUpgraders);
            final Transport transport = networkModule.getTransportSupplier().get();
            final TransportService transportService = newTransportService(settings,
                                                                          transport,
                                                                          threadPool,
                                                                          networkModule.getTransportInterceptor(),
                                                                          localNodeFactory,
                                                                          settingsModule.getClusterSettings());
            final GatewayMetaState gatewayMetaState = new GatewayMetaState(settings,
                                                                           nodeEnvironment,
                                                                           metaStateService,
                                                                           metadataIndexUpgradeService,
                                                                           metadataUpgrader,
                                                                           transportService,
                                                                           clusterService,
                                                                           indicesService);
            final HttpServerTransport httpServerTransport = newHttpTransport(networkModule);


            modules.add(new RepositoriesModule(
                this.environment,
                pluginsService.filterPlugins(RepositoryPlugin.class),
                xContentRegistry, threadPool)
            );

            final DiscoveryModule discoveryModule = new DiscoveryModule(this.settings,
                                                                        threadPool,
                                                                        transportService,
                                                                        namedWriteableRegistry,
                                                                        networkService,
                                                                        clusterService.getMasterService(),
                                                                        clusterService.getClusterApplierService(),
                                                                        clusterService.getClusterSettings(),
                                                                        pluginsService.filterPlugins(DiscoveryPlugin.class),
                                                                        clusterModule.getAllocationService(),
                                                                        environment.configFile(),
                                                                        gatewayMetaState);
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
                    b.bind(IndicesService.class).toInstance(indicesService);
                    b.bind(AliasValidator.class).toInstance(aliasValidator);
                    b.bind(MetadataCreateIndexService.class).toInstance(metadataCreateIndexService);
                    b.bind(Transport.class).toInstance(transport);
                    b.bind(TransportService.class).toInstance(transportService);
                    b.bind(NetworkService.class).toInstance(networkService);
                    b.bind(MetadataIndexUpgradeService.class).toInstance(metadataIndexUpgradeService);
                    b.bind(ClusterInfoService.class).toInstance(clusterInfoService);
                    b.bind(GatewayMetaState.class).toInstance(gatewayMetaState);
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
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);
            client.initialize(injector.getInstance(new Key<Map<Action, TransportAction>>() {}));

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
                                                   TransportInterceptor interceptor,
                                                   Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                                   ClusterSettings clusterSettings) {
        return new TransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings);
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

        injector.getInstance(MappingUpdatedAction.class).setClient(client);
        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(SnapshotShardsService.class).start();
        injector.getInstance(RoutingService.class).start();
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
        final Metadata onDiskMetadata;
        // we load the global state here (the persistent part of the cluster state stored on disk) to
        // pass it to the bootstrap checks to allow plugins to enforce certain preconditions based on the recovered state.
        if (DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings)) {
            onDiskMetadata = injector.getInstance(GatewayMetaState.class).getMetadata();
        } else {
            onDiskMetadata = Metadata.EMPTY_METADATA;
        }
        assert onDiskMetadata != null : "metadata is null but shouldn't"; // this is never null
        validateNodeBeforeAcceptingRequests(transportService.boundAddress(),
            pluginsService.filterPlugins(Plugin.class).stream()
                .flatMap(p -> p.getBootstrapChecks().stream()).collect(Collectors.toList()));

        clusterService.addStateApplier(transportService.getTaskManager());
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
        injector.getInstance(RoutingService.class).stop();
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(NodeConnectionsService.class).stop();
        nodeService.getMonitorService().stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(TransportService.class).stop();

        pluginLifecycleComponents.forEach(LifecycleComponent::stop);
        // we should stop this last since it waits for resources to get released
        // if we had scroll searchers etc or recovery going on we wait for to finish.
        injector.getInstance(IndicesService.class).stop();
        logger.info("stopped");

        return this;
    }

    // During concurrent close() calls we want to make sure that all of them return after the node has completed it's shutdown cycle.
    // If not, the hook that is added in Bootstrap#setup() will be useless:
    // close() might not be executed, in case another (for example api) call to close() has already set some lifecycles to stopped.
    // In this case the process will be terminated even if the first call to close() has not finished yet.
    @Override
    public synchronized void close() throws IOException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }

        logger.info("closing ...");
        List<Closeable> toClose = new ArrayList<>();
        StopWatch stopWatch = new StopWatch("node_close");
        toClose.add(() -> stopWatch.start("node_service"));
        toClose.add(nodeService);
        toClose.add(() -> stopWatch.stop().start("http"));
        toClose.add(injector.getInstance(HttpServerTransport.class));
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
        toClose.add(() -> stopWatch.stop().start("routing"));
        toClose.add(injector.getInstance(RoutingService.class));
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

        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Plugin.class));

        toClose.add(() -> stopWatch.stop().start("thread_pool"));
        // TODO this should really use ThreadPool.terminate()
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdown());
        toClose.add(() -> {
            try {
                injector.getInstance(ThreadPool.class).awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        });

        toClose.add(() -> stopWatch.stop().start("thread_pool_force_shutdown"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdownNow());
        toClose.add(() -> stopWatch.stop());


        toClose.add(injector.getInstance(NodeEnvironment.class));
        toClose.add(injector.getInstance(PageCacheRecycler.class));

        if (logger.isTraceEnabled()) {
            logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint());
        }
        IOUtils.close(toClose);
        logger.info("closed");
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
        return new InternalClusterInfoService(settings, clusterService, threadPool, client);
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
            Set<DiscoveryNode.Role> roles = getRolesFromSettings(settings);
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
