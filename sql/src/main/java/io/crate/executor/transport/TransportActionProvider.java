/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport;

import io.crate.action.job.TransportJobAction;
import io.crate.action.sql.TransportSQLAction;
import io.crate.executor.transport.kill.TransportKillAllNodeAction;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.action.bulk.SymbolBasedTransportShardUpsertActionDelegateImpl;
import org.elasticsearch.action.bulk.TransportShardUpsertActionDelegateImpl;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;

public class TransportActionProvider {

    private final Provider<TransportFetchNodeAction> transportFetchNodeActionProvider;

    private final Provider<TransportCreateIndexAction> transportCreateIndexActionProvider;
    private final Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider;
    private final Provider<TransportGetIndexTemplatesAction> transportGetIndexTemplatesActionProvider;
    private final Provider<TransportPutIndexTemplateAction> transportPutIndexTemplateActionProvider;
    private final Provider<TransportDeleteIndexTemplateAction> transportDeleteIndexTemplateActionProvider;
    private final Provider<TransportClusterUpdateSettingsAction> transportClusterUpdateSettingsActionProvider;
    private final Provider<TransportDeleteByQueryAction> transportDeleteByQueryActionProvider;
    private final Provider<TransportDeleteAction> transportDeleteActionProvider;

    private final Provider<TransportGetAction> transportGetActionProvider;
    private final Provider<TransportMultiGetAction> transportMultiGetActionProvider;
    private final Provider<SymbolBasedTransportShardUpsertAction> symbolBasedTransportShardUpsertActionProvider;
    private final Provider<TransportShardUpsertAction> transportShardUpsertActionProvider;
    private final Provider<TransportPutMappingAction> transportPutMappingActionProvider;
    private final Provider<TransportRefreshAction> transportRefreshActionProvider;
    private final Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider;
    private final Provider<TransportBulkCreateIndicesAction> transportBulkCreateIndicesActionProvider;

    private final Provider<TransportJobAction> transportJobInitActionProvider;
    private final Provider<TransportKillAllNodeAction> transportKillAllNodeActionProvider;
    private final Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider;

    private final Provider<TransportPutRepositoryAction> transportPutRepositoryActionProvider;
    private final Provider<TransportDeleteRepositoryAction> transportDeleteRepositoryActionProvider;

    private final Provider<TransportCreateSnapshotAction> transportCreateSnapshotActionProvider;
    private final Provider<TransportDeleteSnapshotAction> transportDeleteSnapshotActionProvider;
    private final Provider<TransportRestoreSnapshotAction> transportRestoreSnapshotActionProvider;

    private final Provider<TransportSQLAction> transportSQLActionProvider;

    @Inject
    public TransportActionProvider(Provider<TransportFetchNodeAction> transportFetchNodeActionProvider,
                                   Provider<TransportCreateIndexAction> transportCreateIndexActionProvider,
                                   Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider,
                                   Provider<TransportGetIndexTemplatesAction> transportGetIndexTemplatesActionProvider,
                                   Provider<TransportPutIndexTemplateAction> transportPutIndexTemplateActionProvider,
                                   Provider<TransportDeleteIndexTemplateAction> transportDeleteIndexTemplateActionProvider,
                                   Provider<TransportClusterUpdateSettingsAction> transportClusterUpdateSettingsActionProvider,
                                   Provider<TransportDeleteByQueryAction> transportDeleteByQueryActionProvider,
                                   Provider<TransportDeleteAction> transportDeleteActionProvider,
                                   Provider<TransportGetAction> transportGetActionProvider,
                                   Provider<TransportMultiGetAction> transportMultiGetActionProvider,
                                   Provider<SymbolBasedTransportShardUpsertAction> symbolBasedTransportShardUpsertActionProvider,
                                   Provider<TransportKillAllNodeAction> transportKillAllNodeActionProvider,
                                   Provider<TransportShardUpsertAction> transportShardUpsertActionProvider,
                                   Provider<TransportPutMappingAction> transportPutMappingActionProvider,
                                   Provider<TransportRefreshAction> transportRefreshActionProvider,
                                   Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider,
                                   Provider<TransportJobAction> transportJobInitActionProvider,
                                   Provider<TransportBulkCreateIndicesAction> transportBulkCreateIndicesActionProvider,
                                   Provider<TransportKillJobsNodeAction> transportKillJobsNodeActionProvider,
                                   Provider<TransportPutRepositoryAction> transportPutRepositoryActionProvider,
                                   Provider<TransportDeleteRepositoryAction> transportDeleteRepositoryActionProvider,
                                   Provider<TransportDeleteSnapshotAction> transportDeleteSnapshotActionProvider,
                                   Provider<TransportCreateSnapshotAction> transportCreateSnapshotActionProvider,
                                   Provider<TransportRestoreSnapshotAction> transportRestoreSnapshotActionProvider,
                                   Provider<TransportSQLAction> transportSQLActionProvider) {
        this.transportCreateIndexActionProvider = transportCreateIndexActionProvider;
        this.transportDeleteIndexActionProvider = transportDeleteIndexActionProvider;
        this.transportPutIndexTemplateActionProvider = transportPutIndexTemplateActionProvider;
        this.transportGetIndexTemplatesActionProvider = transportGetIndexTemplatesActionProvider;
        this.transportDeleteIndexTemplateActionProvider = transportDeleteIndexTemplateActionProvider;
        this.transportClusterUpdateSettingsActionProvider = transportClusterUpdateSettingsActionProvider;
        this.transportDeleteByQueryActionProvider = transportDeleteByQueryActionProvider;
        this.transportDeleteActionProvider = transportDeleteActionProvider;
        this.transportGetActionProvider = transportGetActionProvider;
        this.transportMultiGetActionProvider = transportMultiGetActionProvider;
        this.symbolBasedTransportShardUpsertActionProvider = symbolBasedTransportShardUpsertActionProvider;
        this.transportKillAllNodeActionProvider = transportKillAllNodeActionProvider;
        this.transportShardUpsertActionProvider = transportShardUpsertActionProvider;
        this.transportFetchNodeActionProvider = transportFetchNodeActionProvider;
        this.transportPutMappingActionProvider = transportPutMappingActionProvider;
        this.transportRefreshActionProvider = transportRefreshActionProvider;
        this.transportUpdateSettingsActionProvider = transportUpdateSettingsActionProvider;
        this.transportJobInitActionProvider = transportJobInitActionProvider;
        this.transportBulkCreateIndicesActionProvider = transportBulkCreateIndicesActionProvider;
        this.transportKillJobsNodeActionProvider = transportKillJobsNodeActionProvider;
        this.transportPutRepositoryActionProvider = transportPutRepositoryActionProvider;
        this.transportDeleteRepositoryActionProvider = transportDeleteRepositoryActionProvider;
        this.transportDeleteSnapshotActionProvider = transportDeleteSnapshotActionProvider;
        this.transportCreateSnapshotActionProvider = transportCreateSnapshotActionProvider;
        this.transportRestoreSnapshotActionProvider = transportRestoreSnapshotActionProvider;
        this.transportSQLActionProvider = transportSQLActionProvider;
    }

    public TransportCreateIndexAction transportCreateIndexAction() {
        return transportCreateIndexActionProvider.get();
    }

    public TransportBulkCreateIndicesAction transportBulkCreateIndicesAction() {
        return transportBulkCreateIndicesActionProvider.get();
    }

    public TransportDeleteIndexAction transportDeleteIndexAction() {
        return transportDeleteIndexActionProvider.get();
    }

    public TransportGetIndexTemplatesAction transportGetIndexTemplatesAction() {
        return transportGetIndexTemplatesActionProvider.get();
    }

    public TransportPutIndexTemplateAction transportPutIndexTemplateAction() {
        return transportPutIndexTemplateActionProvider.get();
    }

    public TransportDeleteIndexTemplateAction transportDeleteIndexTemplateAction() {
        return transportDeleteIndexTemplateActionProvider.get();
    }

    public TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction() {
        return transportClusterUpdateSettingsActionProvider.get();
    }

    public TransportDeleteByQueryAction transportDeleteByQueryAction() {
        return transportDeleteByQueryActionProvider.get();
    }

    public TransportDeleteAction transportDeleteAction() {
        return transportDeleteActionProvider.get();
    }

    public TransportGetAction transportGetAction() {
        return transportGetActionProvider.get();
    }

    public TransportMultiGetAction transportMultiGetAction() {
        return transportMultiGetActionProvider.get();
    }

    public BulkRequestExecutor<ShardUpsertRequest, ShardUpsertResponse> transportShardUpsertActionDelegate() {
        return new TransportShardUpsertActionDelegateImpl(transportShardUpsertActionProvider.get());
    }

    public BulkRequestExecutor<SymbolBasedShardUpsertRequest, ShardUpsertResponse> symbolBasedTransportShardUpsertActionDelegate() {
        return new SymbolBasedTransportShardUpsertActionDelegateImpl(symbolBasedTransportShardUpsertActionProvider.get());
    }

    public TransportJobAction transportJobInitAction() {
        return transportJobInitActionProvider.get();
    }

    public TransportFetchNodeAction transportFetchNodeAction() {
        return transportFetchNodeActionProvider.get();
    }

    public TransportPutMappingAction transportPutMappingAction() {
        return transportPutMappingActionProvider.get();
    }

    public TransportRefreshAction transportRefreshAction() {
        return transportRefreshActionProvider.get();
    }

    public TransportUpdateSettingsAction transportUpdateSettingsAction() {
        return transportUpdateSettingsActionProvider.get();
    }

    public TransportKillAllNodeAction transportKillAllNodeAction() {
        return transportKillAllNodeActionProvider.get();
    }

    public TransportKillJobsNodeAction transportKillJobsNodeAction() {
        return transportKillJobsNodeActionProvider.get();
    }

    public TransportPutRepositoryAction transportPutRepositoryAction() {
        return transportPutRepositoryActionProvider.get();
    }

    public TransportDeleteRepositoryAction transportDeleteRepositoryAction() {
        return transportDeleteRepositoryActionProvider.get();
    }

    public TransportDeleteSnapshotAction transportDeleteSnapshotAction() {
        return transportDeleteSnapshotActionProvider.get();
    }

    public TransportSQLAction transportSQLAction() {
        return transportSQLActionProvider.get();
    }

    public TransportCreateSnapshotAction transportCreateSnapshotAction() {
        return transportCreateSnapshotActionProvider.get();
    }

    public TransportRestoreSnapshotAction transportRestoreSnapshotAction() {
        return transportRestoreSnapshotActionProvider.get();
    }
}
