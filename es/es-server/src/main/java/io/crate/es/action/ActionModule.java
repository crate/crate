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

package io.crate.es.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.crate.es.action.admin.cluster.health.ClusterHealthAction;
import io.crate.es.action.admin.cluster.health.TransportClusterHealthAction;
import io.crate.es.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import io.crate.es.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import io.crate.es.action.admin.cluster.repositories.put.PutRepositoryAction;
import io.crate.es.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import io.crate.es.action.admin.cluster.reroute.ClusterRerouteAction;
import io.crate.es.action.admin.cluster.reroute.TransportClusterRerouteAction;
import io.crate.es.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import io.crate.es.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import io.crate.es.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import io.crate.es.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import io.crate.es.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import io.crate.es.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import io.crate.es.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import io.crate.es.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import io.crate.es.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import io.crate.es.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import io.crate.es.action.admin.cluster.state.ClusterStateAction;
import io.crate.es.action.admin.cluster.state.TransportClusterStateAction;
import io.crate.es.action.admin.cluster.tasks.PendingClusterTasksAction;
import io.crate.es.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import io.crate.es.action.admin.indices.create.CreateIndexAction;
import io.crate.es.action.admin.indices.create.TransportCreateIndexAction;
import io.crate.es.action.admin.indices.delete.DeleteIndexAction;
import io.crate.es.action.admin.indices.delete.TransportDeleteIndexAction;
import io.crate.es.action.admin.indices.flush.FlushAction;
import io.crate.es.action.admin.indices.flush.SyncedFlushAction;
import io.crate.es.action.admin.indices.flush.TransportFlushAction;
import io.crate.es.action.admin.indices.flush.TransportSyncedFlushAction;
import io.crate.es.action.admin.indices.forcemerge.ForceMergeAction;
import io.crate.es.action.admin.indices.forcemerge.TransportForceMergeAction;
import io.crate.es.action.admin.indices.mapping.put.PutMappingAction;
import io.crate.es.action.admin.indices.mapping.put.TransportPutMappingAction;
import io.crate.es.action.admin.indices.recovery.RecoveryAction;
import io.crate.es.action.admin.indices.recovery.TransportRecoveryAction;
import io.crate.es.action.admin.indices.refresh.RefreshAction;
import io.crate.es.action.admin.indices.refresh.TransportRefreshAction;
import io.crate.es.action.admin.indices.settings.get.GetSettingsAction;
import io.crate.es.action.admin.indices.settings.get.TransportGetSettingsAction;
import io.crate.es.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import io.crate.es.action.admin.indices.settings.put.UpdateSettingsAction;
import io.crate.es.action.admin.indices.shrink.ResizeAction;
import io.crate.es.action.admin.indices.shrink.TransportResizeAction;
import io.crate.es.action.admin.indices.stats.IndicesStatsAction;
import io.crate.es.action.admin.indices.stats.TransportIndicesStatsAction;
import io.crate.es.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import io.crate.es.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import io.crate.es.action.admin.indices.template.get.GetIndexTemplatesAction;
import io.crate.es.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import io.crate.es.action.admin.indices.template.put.PutIndexTemplateAction;
import io.crate.es.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import io.crate.es.action.admin.indices.upgrade.post.TransportUpgradeAction;
import io.crate.es.action.admin.indices.upgrade.post.TransportUpgradeSettingsAction;
import io.crate.es.action.admin.indices.upgrade.post.UpgradeAction;
import io.crate.es.action.admin.indices.upgrade.post.UpgradeSettingsAction;
import io.crate.es.action.support.DestructiveOperations;
import io.crate.es.action.support.TransportAction;
import io.crate.es.client.node.NodeClient;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.node.DiscoveryNodes;
import io.crate.es.common.NamedRegistry;
import io.crate.es.common.inject.AbstractModule;
import io.crate.es.common.inject.multibindings.MapBinder;
import io.crate.es.common.settings.ClusterSettings;
import io.crate.es.common.settings.IndexScopedSettings;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.settings.SettingsFilter;
import io.crate.es.indices.breaker.CircuitBreakerService;
import io.crate.es.plugins.ActionPlugin;
import io.crate.es.plugins.ActionPlugin.ActionHandler;
import io.crate.es.rest.RestController;
import io.crate.es.rest.RestHandler;
import io.crate.es.tasks.Task;
import io.crate.es.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s
 */
public class ActionModule extends AbstractModule {

    private static final Logger logger = LogManager.getLogger(ActionModule.class);

    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;
    private final List<ActionPlugin> actionPlugins;
    private final Map<String, ActionHandler<?, ?>> actions;
    private final DestructiveOperations destructiveOperations;
    private final RestController restController;

    public ActionModule(Settings settings, IndexNameExpressionResolver indexNameExpressionResolver,
                        IndexScopedSettings indexScopedSettings, ClusterSettings clusterSettings, SettingsFilter settingsFilter,
                        ThreadPool threadPool, List<ActionPlugin> actionPlugins, NodeClient nodeClient,
            CircuitBreakerService circuitBreakerService) {
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.actionPlugins = actionPlugins;
        actions = setupActions(actionPlugins);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
        Set<String> headers = Stream.concat(
            actionPlugins.stream().flatMap(p -> p.getRestHeaders().stream()),
            Stream.of(Task.X_OPAQUE_ID)
        ).collect(Collectors.toSet());
        UnaryOperator<RestHandler> restWrapper = null;
        for (ActionPlugin plugin : actionPlugins) {
            UnaryOperator<RestHandler> newRestWrapper = plugin.getRestHandlerWrapper(threadPool.getThreadContext());
            if (newRestWrapper != null) {
                logger.debug("Using REST wrapper from plugin " + plugin.getClass().getName());
                if (restWrapper != null) {
                    throw new IllegalArgumentException("Cannot have more than one plugin implementing a REST wrapper");
                }
                restWrapper = newRestWrapper;
            }
        }
        restController = new RestController(settings, headers, restWrapper, nodeClient, circuitBreakerService);
    }


    public Map<String, ActionHandler<?, ?>> getActions() {
        return actions;
    }

    static Map<String, ActionHandler<?, ?>> setupActions(List<ActionPlugin> actionPlugins) {
        // Subclass NamedRegistry for easy registration
        class ActionRegistry extends NamedRegistry<ActionHandler<?, ?>> {
            ActionRegistry() {
                super("action");
            }

            public void register(ActionHandler<?, ?> handler) {
                register(handler.getAction().name(), handler);
            }

            public <Request extends ActionRequest, Response extends ActionResponse> void register(
                    GenericAction<Request, Response> action, Class<? extends TransportAction<Request, Response>> transportAction,
                    Class<?>... supportTransportActions) {
                register(new ActionHandler<>(action, transportAction, supportTransportActions));
            }
        }
        ActionRegistry actions = new ActionRegistry();

        actions.register(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        actions.register(ClusterHealthAction.INSTANCE, TransportClusterHealthAction.class);
        actions.register(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        actions.register(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        actions.register(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        actions.register(PutRepositoryAction.INSTANCE, TransportPutRepositoryAction.class);
        actions.register(DeleteRepositoryAction.INSTANCE, TransportDeleteRepositoryAction.class);
        actions.register(GetSnapshotsAction.INSTANCE, TransportGetSnapshotsAction.class);
        actions.register(DeleteSnapshotAction.INSTANCE, TransportDeleteSnapshotAction.class);
        actions.register(CreateSnapshotAction.INSTANCE, TransportCreateSnapshotAction.class);
        actions.register(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        actions.register(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        actions.register(CreateIndexAction.INSTANCE, TransportCreateIndexAction.class);
        actions.register(ResizeAction.INSTANCE, TransportResizeAction.class);
        actions.register(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        actions.register(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        actions.register(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        actions.register(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        actions.register(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        actions.register(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        actions.register(RefreshAction.INSTANCE, TransportRefreshAction.class);
        actions.register(FlushAction.INSTANCE, TransportFlushAction.class);
        actions.register(SyncedFlushAction.INSTANCE, TransportSyncedFlushAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(UpgradeAction.INSTANCE, TransportUpgradeAction.class);
        actions.register(UpgradeSettingsAction.INSTANCE, TransportUpgradeSettingsAction.class);
        actions.register(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);

        actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register);

        return unmodifiableMap(actions.getRegistry());
    }

    public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster) {
        Consumer<RestHandler> registerHandler = a -> {
        };
        for (ActionPlugin plugin : actionPlugins) {
            for (RestHandler handler : plugin.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings,
                    settingsFilter, indexNameExpressionResolver, nodesInCluster)) {
                registerHandler.accept(handler);
            }
        }
    }

    @Override
    protected void configure() {
        bind(DestructiveOperations.class).toInstance(destructiveOperations);
        bind(RestController.class).toInstance(restController);

        // register GenericAction -> transportAction Map used by NodeClient
        @SuppressWarnings("rawtypes")
        MapBinder<GenericAction, TransportAction> transportActionsBinder
                = MapBinder.newMapBinder(binder(), GenericAction.class, TransportAction.class);
        for (ActionHandler<?, ?> action : actions.values()) {
            // bind the action as eager singleton, so the map binder one will reuse it
            bind(action.getTransportAction()).asEagerSingleton();
            transportActionsBinder.addBinding(action.getAction()).to(action.getTransportAction()).asEagerSingleton();
            for (Class<?> supportAction : action.getSupportTransportActions()) {
                bind(supportAction).asEagerSingleton();
            }
        }
    }

    public RestController getRestController() {
        return restController;
    }
}
