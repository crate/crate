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

import java.util.Map;

import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusions;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusions;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealth;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStats;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepository;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepository;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshot;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshot;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshot;
import org.elasticsearch.action.admin.cluster.state.TransportClusterState;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.action.admin.indices.create.TransportCreatePartitions;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndex;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.TransportForceMergeAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefresh;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.admin.indices.retention.SyncRetentionLeasesAction;
import org.elasticsearch.action.admin.indices.retention.TransportSyncRetentionLeasesAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettings;
import org.elasticsearch.action.admin.indices.shrink.TransportResize;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStats;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

import io.crate.blob.TransportDeleteBlob;
import io.crate.blob.TransportPutChunk;
import io.crate.blob.TransportStartBlob;
import io.crate.cluster.decommission.DecommissionNodeAction;
import io.crate.cluster.decommission.TransportDecommissionNodeAction;
import io.crate.execution.ddl.TransportSwapRelations;
import io.crate.execution.ddl.index.TransportSwapAndDropIndexName;
import io.crate.execution.ddl.tables.TransportAddColumn;
import io.crate.execution.ddl.tables.TransportAlterTable;
import io.crate.execution.ddl.tables.TransportCloseTable;
import io.crate.execution.ddl.tables.TransportCreateBlobTable;
import io.crate.execution.ddl.tables.TransportCreateTable;
import io.crate.execution.ddl.tables.TransportDropColumn;
import io.crate.execution.ddl.tables.TransportDropConstraint;
import io.crate.execution.ddl.tables.TransportDropPartitionsAction;
import io.crate.execution.ddl.tables.TransportDropTable;
import io.crate.execution.ddl.tables.TransportGCDanglingArtifacts;
import io.crate.execution.ddl.tables.TransportOpenTable;
import io.crate.execution.ddl.tables.TransportRenameColumn;
import io.crate.execution.ddl.tables.TransportRenameTable;
import io.crate.execution.ddl.views.TransportCreateView;
import io.crate.execution.ddl.views.TransportDropView;
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
import io.crate.expression.udf.TransportCreateUserDefinedFunction;
import io.crate.expression.udf.TransportDropUserDefinedFunction;
import io.crate.fdw.TransportAlterServer;
import io.crate.fdw.TransportCreateForeignTable;
import io.crate.fdw.TransportCreateServer;
import io.crate.fdw.TransportCreateUserMapping;
import io.crate.fdw.TransportDropForeignTable;
import io.crate.fdw.TransportDropServer;
import io.crate.fdw.TransportDropUserMapping;
import io.crate.replication.logical.action.DropSubscriptionAction;
import io.crate.replication.logical.action.GetFileChunkAction;
import io.crate.replication.logical.action.GetStoreMetadataAction;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.action.ReleasePublisherResourcesAction;
import io.crate.replication.logical.action.ReplayChangesAction;
import io.crate.replication.logical.action.ShardChangesAction;
import io.crate.replication.logical.action.TransportAlterPublication;
import io.crate.replication.logical.action.TransportCreatePublication;
import io.crate.replication.logical.action.TransportCreateSubscription;
import io.crate.replication.logical.action.TransportDropPublication;
import io.crate.replication.logical.action.UpdateSubscriptionAction;
import io.crate.role.TransportAlterRole;
import io.crate.role.TransportCreateRole;
import io.crate.role.TransportDropRole;
import io.crate.role.TransportPrivileges;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s
 */
public class ActionModule extends AbstractModule {

    private final Map<String, ActionHandler<?, ?>> actions;

    public ActionModule() {
        actions = setupActions();
    }

    static Map<String, ActionHandler<?, ?>> setupActions() {
        // Subclass NamedRegistry for easy registration
        class ActionRegistry extends NamedRegistry<ActionHandler<?, ?>> {
            ActionRegistry() {
                super("action");
            }

            public void register(ActionHandler<?, ?> handler) {
                register(handler.action().name(), handler);
            }

            public <Request extends TransportRequest, Response extends TransportResponse> void register(
                    ActionType<Response> action, Class<? extends TransportAction<Request, Response>> transportAction,
                    Class<?>... supportTransportActions) {
                register(new ActionHandler<>(action, transportAction, supportTransportActions));
            }
        }

        ActionRegistry actions = new ActionRegistry();

        // Table actions
        actions.register(TransportCreateTable.ACTION, TransportCreateTable.class);
        actions.register(TransportCreatePartitions.ACTION, TransportCreatePartitions.class);
        actions.register(TransportDropTable.ACTION, TransportDropTable.class);
        actions.register(TransportDropPartitionsAction.ACTION, TransportDropPartitionsAction.class);
        actions.register(TransportAlterTable.ACTION, TransportAlterTable.class);
        actions.register(TransportRenameTable.ACTION, TransportRenameTable.class);
        actions.register(TransportOpenTable.ACTION, TransportOpenTable.class);
        actions.register(TransportCloseTable.ACTION, TransportCloseTable.class);
        actions.register(TransportAddColumn.ACTION, TransportAddColumn.class);
        actions.register(TransportDropColumn.ACTION, TransportDropColumn.class);
        actions.register(TransportRenameColumn.ACTION, TransportRenameColumn.class);
        actions.register(TransportDropConstraint.ACTION, TransportDropConstraint.class);
        actions.register(TransportResize.ACTION, TransportResize.class);
        actions.register(TransportUpdateSettings.ACTION, TransportUpdateSettings.class);
        actions.register(TransportRefresh.ACTION, TransportRefresh.class);
        actions.register(TransportSwapRelations.ACTION, TransportSwapRelations.class);

        // View actions
        actions.register(TransportCreateView.ACTION, TransportCreateView.class);
        actions.register(TransportDropView.ACTION, TransportDropView.class);

        // Blob table actions
        actions.register(TransportCreateBlobTable.ACTION, TransportCreateBlobTable.class);
        actions.register(TransportPutChunk.ACTION, TransportPutChunk.class);
        actions.register(TransportStartBlob.ACTION, TransportStartBlob.class);
        actions.register(TransportDeleteBlob.ACTION, TransportDeleteBlob.class);

        // UDF actions
        actions.register(TransportCreateUserDefinedFunction.ACTION, TransportCreateUserDefinedFunction.class);
        actions.register(TransportDropUserDefinedFunction.ACTION, TransportDropUserDefinedFunction.class);

        // Repository & Snapshot actions
        actions.register(TransportPutRepository.ACTION, TransportPutRepository.class);
        actions.register(TransportDeleteRepository.ACTION, TransportDeleteRepository.class);
        actions.register(TransportDeleteSnapshot.ACTION, TransportDeleteSnapshot.class);
        actions.register(TransportCreateSnapshot.ACTION, TransportCreateSnapshot.class);
        actions.register(TransportRestoreSnapshot.ACTION, TransportRestoreSnapshot.class);

        // Roles & Privileges actions
        actions.register(TransportPrivileges.ACTION, TransportPrivileges.class);
        actions.register(TransportCreateRole.ACTION, TransportCreateRole.class);
        actions.register(TransportDropRole.ACTION, TransportDropRole.class);
        actions.register(TransportAlterRole.ACTION, TransportAlterRole.class);

        // Logical replication actions
        actions.register(TransportCreatePublication.ACTION, TransportCreatePublication.class);
        actions.register(TransportAlterPublication.ACTION, TransportAlterPublication.class);
        actions.register(TransportDropPublication.ACTION, TransportDropPublication.class);
        actions.register(TransportCreateSubscription.ACTION, TransportCreateSubscription.class);
        actions.register(PublicationsStateAction.INSTANCE, PublicationsStateAction.TransportAction.class);
        actions.register(UpdateSubscriptionAction.INSTANCE, UpdateSubscriptionAction.TransportAction.class);
        actions.register(DropSubscriptionAction.INSTANCE, DropSubscriptionAction.TransportAction.class);
        actions.register(ReleasePublisherResourcesAction.INSTANCE, ReleasePublisherResourcesAction.TransportAction.class);

        // FDW actions
        actions.register(TransportCreateServer.ACTION, TransportCreateServer.class);
        actions.register(TransportAlterServer.ACTION, TransportAlterServer.class);
        actions.register(TransportCreateForeignTable.ACTION, TransportCreateForeignTable.class);
        actions.register(TransportCreateUserMapping.ACTION, TransportCreateUserMapping.class);
        actions.register(TransportDropServer.ACTION, TransportDropServer.class);
        actions.register(TransportDropForeignTable.ACTION, TransportDropForeignTable.class);
        actions.register(TransportDropUserMapping.ACTION, TransportDropUserMapping.class);

        // Cluster & internal actions
        actions.register(TransportClusterState.ACTION, TransportClusterState.class);
        actions.register(TransportClusterHealth.ACTION, TransportClusterHealth.class);
        actions.register(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        actions.register(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        actions.register(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(SyncRetentionLeasesAction.INSTANCE, TransportSyncRetentionLeasesAction.class);
        actions.register(TransportAddVotingConfigExclusions.ACTION, TransportAddVotingConfigExclusions.class);
        actions.register(TransportClearVotingConfigExclusions.ACTION, TransportClearVotingConfigExclusions.class);
        actions.register(TransportNodesStats.ACTION, TransportNodesStats.class);
        actions.register(ShardDeleteAction.INSTANCE, TransportShardDeleteAction.class);
        actions.register(ShardUpsertAction.INSTANCE, TransportShardUpsertAction.class);
        actions.register(KillJobsNodeAction.INSTANCE, TransportKillJobsNodeAction.class);
        actions.register(KillAllNodeAction.INSTANCE, TransportKillAllNodeAction.class);
        actions.register(NodeStatsAction.INSTANCE, TransportNodeStatsAction.class);
        actions.register(CollectProfileNodeAction.INSTANCE, TransportCollectProfileNodeAction.class);
        actions.register(DecommissionNodeAction.INSTANCE, TransportDecommissionNodeAction.class);
        actions.register(DistributedResultAction.INSTANCE, TransportDistributedResultAction.class);
        actions.register(JobAction.INSTANCE, TransportJobAction.class);
        actions.register(FetchNodeAction.INSTANCE, TransportFetchNodeAction.class);
        actions.register(GlobalCheckpointSyncAction.TYPE, GlobalCheckpointSyncAction.class);
        actions.register(TransportVerifyShardBeforeCloseAction.TYPE, TransportVerifyShardBeforeCloseAction.class);
        actions.register(TransportNodesListGatewayStartedShards.TYPE, TransportNodesListGatewayStartedShards.class);
        actions.register(TransportNodesListShardStoreMetadata.TYPE, TransportNodesListShardStoreMetadata.class);
        actions.register(TransportShardRefreshAction.TYPE, TransportShardRefreshAction.class);
        actions.register(RetentionLeaseActions.Add.INSTANCE, RetentionLeaseActions.Add.TransportAction.class);
        actions.register(RetentionLeaseActions.Remove.INSTANCE, RetentionLeaseActions.Remove.TransportAction.class);
        actions.register(RetentionLeaseActions.Renew.INSTANCE, RetentionLeaseActions.Renew.TransportAction.class);
        actions.register(GetFileChunkAction.INSTANCE, GetFileChunkAction.TransportAction.class);
        actions.register(GetStoreMetadataAction.INSTANCE, GetStoreMetadataAction.TransportAction.class);
        actions.register(ShardChangesAction.INSTANCE, ShardChangesAction.TransportAction.class);
        actions.register(ReplayChangesAction.INSTANCE, ReplayChangesAction.TransportAction.class);

        // Misc actions
        actions.register(TransportIndicesStats.ACTION, TransportIndicesStats.class);
        actions.register(TransportDeleteIndex.ACTION, TransportDeleteIndex.class);
        actions.register(TransportSwapAndDropIndexName.ACTION, TransportSwapAndDropIndexName.class);
        actions.register(TransportGCDanglingArtifacts.ACTION, TransportGCDanglingArtifacts.class);

        return unmodifiableMap(actions.getRegistry());
    }

    @Override
    protected void configure() {

        // register ActionType -> transportAction Map used by NodeClient
        @SuppressWarnings("rawtypes")
        MapBinder<ActionType, TransportAction> transportActionsBinder
                = MapBinder.newMapBinder(binder(), ActionType.class, TransportAction.class);
        for (ActionHandler<?, ?> action : actions.values()) {
            // bind the action as eager singleton, so the map binder one will reuse it
            bind(action.transportAction()).asEagerSingleton();
            transportActionsBinder.addBinding(action.action()).to(action.transportAction()).asEagerSingleton();
            for (Class<?> supportAction : action.supportTransportActions()) {
                bind(supportAction).asEagerSingleton();
            }
        }
    }

    /**
     * Create a record of an action, the {@linkplain TransportAction} that handles it, and any supporting {@linkplain TransportActions}
     * that are needed by that {@linkplain TransportAction}.
     */
    private record ActionHandler<Request extends TransportRequest, Response extends TransportResponse>(
        ActionType<Response> action, Class<? extends TransportAction<Request, Response>> transportAction,
        Class<?>... supportTransportActions) {

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder().append(action.name()).append(" is handled by ").append(transportAction.getName());
            if (supportTransportActions.length > 0) {
                b.append('[').append(Strings.arrayToCommaDelimitedString(supportTransportActions)).append(']');
            }
            return b.toString();
        }
    }
}
