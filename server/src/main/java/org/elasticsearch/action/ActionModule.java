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

package org.elasticsearch.action;

import static java.util.Collections.unmodifiableMap;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreatePartitionsAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.create.TransportCreatePartitionsAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushAction;
import org.elasticsearch.action.admin.indices.flush.TransportShardFlushAction;
import org.elasticsearch.action.admin.indices.flush.TransportSyncedFlushAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.TransportForceMergeAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.TransportRecoveryAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.TransportNodesListGatewayMetaState;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

import io.crate.blob.DeleteBlobAction;
import io.crate.blob.PutChunkAction;
import io.crate.blob.StartBlobAction;
import io.crate.blob.TransportDeleteBlobAction;
import io.crate.blob.TransportPutChunkAction;
import io.crate.blob.TransportStartBlobAction;
import io.crate.cluster.decommission.DecommissionNodeAction;
import io.crate.cluster.decommission.TransportDecommissionNodeAction;
import io.crate.execution.ddl.tables.RenameColumnAction;
import io.crate.execution.ddl.tables.TransportRenameColumnAction;
import io.crate.execution.dml.delete.ShardDeleteAction;
import io.crate.execution.dml.delete.TransportShardDeleteAction;
import io.crate.execution.dml.upsert.ShardUpsertAction;
import io.crate.execution.dml.upsert.TransportShardUpsertAction;
import io.crate.execution.engine.collect.stats.NodeStatsAction;
import io.crate.execution.engine.collect.stats.TransportNodeStatsAction;
import io.crate.execution.engine.distribution.DistributedResultAction;
import io.crate.execution.engine.distribution.TransportDistributedResultAction;
import io.crate.execution.engine.fetch.FetchNodeAction;
import io.crate.execution.engine.fetch.TransportFetchNodeAction;
import io.crate.execution.engine.profile.CollectProfileNodeAction;
import io.crate.execution.engine.profile.TransportCollectProfileNodeAction;
import io.crate.execution.jobs.kill.KillAllNodeAction;
import io.crate.execution.jobs.kill.KillJobsNodeAction;
import io.crate.execution.jobs.kill.TransportKillAllNodeAction;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.jobs.transport.JobAction;
import io.crate.execution.jobs.transport.TransportJobAction;
import io.crate.fdw.TransportCreateServerAction;
import io.crate.replication.logical.action.DropSubscriptionAction;
import io.crate.replication.logical.action.GetFileChunkAction;
import io.crate.replication.logical.action.GetStoreMetadataAction;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.action.ReleasePublisherResourcesAction;
import io.crate.replication.logical.action.ReplayChangesAction;
import io.crate.replication.logical.action.ShardChangesAction;
import io.crate.replication.logical.action.UpdateSubscriptionAction;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s
 */
public class ActionModule extends AbstractModule {

    private final Map<String, ActionHandler<?, ?>> actions;
    private final DestructiveOperations destructiveOperations;

    public ActionModule(Settings settings, ClusterSettings clusterSettings, List<ActionPlugin> actionPlugins) {
        actions = setupActions(actionPlugins);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
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

            public <Request extends TransportRequest, Response extends TransportResponse> void register(
                    ActionType<Response> action, Class<? extends TransportAction<Request, Response>> transportAction,
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
        actions.register(SyncedFlushAction.INSTANCE, TransportSyncedFlushAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);
        actions.register(AddVotingConfigExclusionsAction.INSTANCE, TransportAddVotingConfigExclusionsAction.class);
        actions.register(ClearVotingConfigExclusionsAction.INSTANCE, TransportClearVotingConfigExclusionsAction.class);
        actions.register(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        actions.register(ShardDeleteAction.INSTANCE, TransportShardDeleteAction.class);
        actions.register(ShardUpsertAction.INSTANCE, TransportShardUpsertAction.class);
        actions.register(CreatePartitionsAction.INSTANCE, TransportCreatePartitionsAction.class);
        actions.register(KillJobsNodeAction.INSTANCE, TransportKillJobsNodeAction.class);
        actions.register(KillAllNodeAction.INSTANCE, TransportKillAllNodeAction.class);
        actions.register(NodeStatsAction.INSTANCE, TransportNodeStatsAction.class);
        actions.register(CollectProfileNodeAction.INSTANCE, TransportCollectProfileNodeAction.class);
        actions.register(DecommissionNodeAction.INSTANCE, TransportDecommissionNodeAction.class);
        actions.register(DistributedResultAction.INSTANCE, TransportDistributedResultAction.class);
        actions.register(JobAction.INSTANCE, TransportJobAction.class);
        actions.register(FetchNodeAction.INSTANCE, TransportFetchNodeAction.class);
        actions.register(RenameColumnAction.INSTANCE, TransportRenameColumnAction.class);

        actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register);

        // internal actions
        actions.register(GlobalCheckpointSyncAction.TYPE, GlobalCheckpointSyncAction.class);
        actions.register(TransportNodesListGatewayMetaState.TYPE, TransportNodesListGatewayMetaState.class);
        actions.register(TransportVerifyShardBeforeCloseAction.TYPE, TransportVerifyShardBeforeCloseAction.class);
        actions.register(TransportNodesListGatewayStartedShards.TYPE, TransportNodesListGatewayStartedShards.class);
        actions.register(TransportNodesListShardStoreMetadata.TYPE, TransportNodesListShardStoreMetadata.class);
        actions.register(TransportShardFlushAction.TYPE, TransportShardFlushAction.class);
        actions.register(TransportShardRefreshAction.TYPE, TransportShardRefreshAction.class);

        // internal blob actions
        actions.register(PutChunkAction.INSTANCE, TransportPutChunkAction.class);
        actions.register(StartBlobAction.INSTANCE, TransportStartBlobAction.class);
        actions.register(DeleteBlobAction.INSTANCE, TransportDeleteBlobAction.class);

        actions.register(RetentionLeaseActions.Add.INSTANCE, RetentionLeaseActions.Add.TransportAction.class);
        actions.register(RetentionLeaseActions.Remove.INSTANCE, RetentionLeaseActions.Remove.TransportAction.class);
        actions.register(RetentionLeaseActions.Renew.INSTANCE, RetentionLeaseActions.Renew.TransportAction.class);
        actions.register(PublicationsStateAction.INSTANCE, PublicationsStateAction.TransportAction.class);
        actions.register(GetFileChunkAction.INSTANCE, GetFileChunkAction.TransportAction.class);
        actions.register(GetStoreMetadataAction.INSTANCE, GetStoreMetadataAction.TransportAction.class);
        actions.register(ReleasePublisherResourcesAction.INSTANCE, ReleasePublisherResourcesAction.TransportAction.class);
        actions.register(ShardChangesAction.INSTANCE, ShardChangesAction.TransportAction.class);
        actions.register(ReplayChangesAction.INSTANCE, ReplayChangesAction.TransportAction.class);
        actions.register(UpdateSubscriptionAction.INSTANCE, UpdateSubscriptionAction.TransportAction.class);
        actions.register(DropSubscriptionAction.INSTANCE, DropSubscriptionAction.TransportAction.class);

        actions.register(TransportCreateServerAction.ACTION, TransportCreateServerAction.class);

        return unmodifiableMap(actions.getRegistry());
    }

    @Override
    protected void configure() {
        bind(DestructiveOperations.class).toInstance(destructiveOperations);

        // register ActionType -> transportAction Map used by NodeClient
        @SuppressWarnings("rawtypes")
        MapBinder<ActionType, TransportAction> transportActionsBinder
                = MapBinder.newMapBinder(binder(), ActionType.class, TransportAction.class);
        for (ActionHandler<?, ?> action : actions.values()) {
            // bind the action as eager singleton, so the map binder one will reuse it
            bind(action.getTransportAction()).asEagerSingleton();
            transportActionsBinder.addBinding(action.getAction()).to(action.getTransportAction()).asEagerSingleton();
            for (Class<?> supportAction : action.getSupportTransportActions()) {
                bind(supportAction).asEagerSingleton();
            }
        }
    }
}
