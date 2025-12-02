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
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;

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
import io.crate.metadata.TransportCreateSchema;
import io.crate.metadata.TransportDropSchema;
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

    public ActionModule() {
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void bind(MapBinder<ActionType, TransportAction> binder,
                      ActionType actionType,
                      Class transportAction) {
        bind(transportAction).asEagerSingleton();
        binder.addBinding(actionType).to(transportAction).asEagerSingleton();
    }

    @Override
    protected void configure() {
        // register ActionType -> transportAction Map used by NodeClient
        @SuppressWarnings("rawtypes")
        MapBinder<ActionType, TransportAction> binder = MapBinder.newMapBinder(binder(), ActionType.class, TransportAction.class);

        // Table actions
        bind(binder, TransportCreateTable.ACTION, TransportCreateTable.class);
        bind(binder, TransportCreatePartitions.ACTION, TransportCreatePartitions.class);
        bind(binder, TransportDropTable.ACTION, TransportDropTable.class);
        bind(binder, TransportDropPartitionsAction.ACTION, TransportDropPartitionsAction.class);
        bind(binder, TransportAlterTable.ACTION, TransportAlterTable.class);
        bind(binder, TransportRenameTable.ACTION, TransportRenameTable.class);
        bind(binder, TransportOpenTable.ACTION, TransportOpenTable.class);
        bind(binder, TransportCloseTable.ACTION, TransportCloseTable.class);
        bind(binder, TransportAddColumn.ACTION, TransportAddColumn.class);
        bind(binder, TransportDropColumn.ACTION, TransportDropColumn.class);
        bind(binder, TransportRenameColumn.ACTION, TransportRenameColumn.class);
        bind(binder, TransportDropConstraint.ACTION, TransportDropConstraint.class);
        bind(binder, TransportResize.ACTION, TransportResize.class);
        bind(binder, TransportUpdateSettings.ACTION, TransportUpdateSettings.class);
        bind(binder, TransportRefresh.ACTION, TransportRefresh.class);
        bind(binder, TransportSwapRelations.ACTION, TransportSwapRelations.class);

        // View actions
        bind(binder, TransportCreateView.ACTION, TransportCreateView.class);
        bind(binder, TransportDropView.ACTION, TransportDropView.class);

        // Blob table actions
        bind(binder, TransportCreateBlobTable.ACTION, TransportCreateBlobTable.class);
        bind(binder, TransportPutChunk.ACTION, TransportPutChunk.class);
        bind(binder, TransportStartBlob.ACTION, TransportStartBlob.class);
        bind(binder, TransportDeleteBlob.ACTION, TransportDeleteBlob.class);

        // UDF actions
        bind(binder, TransportCreateUserDefinedFunction.ACTION, TransportCreateUserDefinedFunction.class);
        bind(binder, TransportDropUserDefinedFunction.ACTION, TransportDropUserDefinedFunction.class);

        // Repository & Snapshot actions
        bind(binder, TransportPutRepository.ACTION, TransportPutRepository.class);
        bind(binder, TransportDeleteRepository.ACTION, TransportDeleteRepository.class);
        bind(binder, TransportDeleteSnapshot.ACTION, TransportDeleteSnapshot.class);
        bind(binder, TransportCreateSnapshot.ACTION, TransportCreateSnapshot.class);
        bind(binder, TransportRestoreSnapshot.ACTION, TransportRestoreSnapshot.class);

        // Roles & Privileges actions
        bind(binder, TransportPrivileges.ACTION, TransportPrivileges.class);
        bind(binder, TransportCreateRole.ACTION, TransportCreateRole.class);
        bind(binder, TransportDropRole.ACTION, TransportDropRole.class);
        bind(binder, TransportAlterRole.ACTION, TransportAlterRole.class);

        // Logical replication actions
        bind(binder, TransportCreatePublication.ACTION, TransportCreatePublication.class);
        bind(binder, TransportAlterPublication.ACTION, TransportAlterPublication.class);
        bind(binder, TransportDropPublication.ACTION, TransportDropPublication.class);
        bind(binder, TransportCreateSubscription.ACTION, TransportCreateSubscription.class);
        bind(binder, PublicationsStateAction.INSTANCE, PublicationsStateAction.TransportAction.class);
        bind(binder, UpdateSubscriptionAction.INSTANCE, UpdateSubscriptionAction.TransportAction.class);
        bind(binder, DropSubscriptionAction.INSTANCE, DropSubscriptionAction.TransportAction.class);
        bind(binder, ReleasePublisherResourcesAction.INSTANCE, ReleasePublisherResourcesAction.TransportAction.class);

        // FDW actions
        bind(binder, TransportCreateServer.ACTION, TransportCreateServer.class);
        bind(binder, TransportAlterServer.ACTION, TransportAlterServer.class);
        bind(binder, TransportCreateForeignTable.ACTION, TransportCreateForeignTable.class);
        bind(binder, TransportCreateUserMapping.ACTION, TransportCreateUserMapping.class);
        bind(binder, TransportDropServer.ACTION, TransportDropServer.class);
        bind(binder, TransportDropForeignTable.ACTION, TransportDropForeignTable.class);
        bind(binder, TransportDropUserMapping.ACTION, TransportDropUserMapping.class);

        // Cluster & internal actions
        bind(binder, TransportClusterState.ACTION, TransportClusterState.class);
        bind(binder, TransportClusterHealth.ACTION, TransportClusterHealth.class);
        bind(binder, ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        bind(binder, ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        bind(binder, PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        bind(binder, ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        bind(binder, SyncRetentionLeasesAction.INSTANCE, TransportSyncRetentionLeasesAction.class);
        bind(binder, TransportAddVotingConfigExclusions.ACTION, TransportAddVotingConfigExclusions.class);
        bind(binder, TransportClearVotingConfigExclusions.ACTION, TransportClearVotingConfigExclusions.class);
        bind(binder, TransportNodesStats.ACTION, TransportNodesStats.class);
        bind(binder, ShardDeleteAction.INSTANCE, TransportShardDeleteAction.class);
        bind(binder, ShardUpsertAction.INSTANCE, TransportShardUpsertAction.class);
        bind(binder, KillJobsNodeAction.INSTANCE, TransportKillJobsNodeAction.class);
        bind(binder, KillAllNodeAction.INSTANCE, TransportKillAllNodeAction.class);
        bind(binder, NodeStatsAction.INSTANCE, TransportNodeStatsAction.class);
        bind(binder, CollectProfileNodeAction.INSTANCE, TransportCollectProfileNodeAction.class);
        bind(binder, DecommissionNodeAction.INSTANCE, TransportDecommissionNodeAction.class);
        bind(binder, DistributedResultAction.INSTANCE, TransportDistributedResultAction.class);
        bind(binder, JobAction.INSTANCE, TransportJobAction.class);
        bind(binder, FetchNodeAction.INSTANCE, TransportFetchNodeAction.class);
        bind(binder, GlobalCheckpointSyncAction.TYPE, GlobalCheckpointSyncAction.class);
        bind(binder, TransportVerifyShardBeforeCloseAction.TYPE, TransportVerifyShardBeforeCloseAction.class);
        bind(binder, TransportNodesListGatewayStartedShards.TYPE, TransportNodesListGatewayStartedShards.class);
        bind(binder, TransportNodesListShardStoreMetadata.TYPE, TransportNodesListShardStoreMetadata.class);
        bind(binder, TransportShardRefreshAction.TYPE, TransportShardRefreshAction.class);
        bind(binder, RetentionLeaseActions.Add.INSTANCE, RetentionLeaseActions.Add.TransportAction.class);
        bind(binder, RetentionLeaseActions.Remove.INSTANCE, RetentionLeaseActions.Remove.TransportAction.class);
        bind(binder, RetentionLeaseActions.Renew.INSTANCE, RetentionLeaseActions.Renew.TransportAction.class);
        bind(binder, GetFileChunkAction.INSTANCE, GetFileChunkAction.TransportAction.class);
        bind(binder, GetStoreMetadataAction.INSTANCE, GetStoreMetadataAction.TransportAction.class);
        bind(binder, ShardChangesAction.INSTANCE, ShardChangesAction.TransportAction.class);
        bind(binder, ReplayChangesAction.INSTANCE, ReplayChangesAction.TransportAction.class);

        // Misc actions
        bind(binder, TransportIndicesStats.ACTION, TransportIndicesStats.class);
        bind(binder, TransportDeleteIndex.ACTION, TransportDeleteIndex.class);
        bind(binder, TransportSwapAndDropIndexName.ACTION, TransportSwapAndDropIndexName.class);
        bind(binder, TransportGCDanglingArtifacts.ACTION, TransportGCDanglingArtifacts.class);

        bind(binder, TransportCreateSchema.ACTION, TransportCreateSchema.class);
        bind(binder, TransportDropSchema.ACTION, TransportDropSchema.class);
    }
}
