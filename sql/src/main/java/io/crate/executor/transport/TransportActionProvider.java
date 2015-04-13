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
import io.crate.executor.transport.merge.TransportDistributedResultAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.bulk.SymbolBasedTransportShardUpsertActionDelegate;
import org.elasticsearch.action.bulk.SymbolBasedTransportShardUpsertActionDelegateImpl;
import org.elasticsearch.action.bulk.TransportShardUpsertActionDelegate;
import org.elasticsearch.action.bulk.TransportShardUpsertActionDelegateImpl;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;

public class TransportActionProvider {

    private final Provider<TransportDistributedResultAction> transportDistributedResultActionProvider;
    private final Provider<TransportFetchNodeAction> transportFetchNodeActionProvider;
    private final Provider<TransportCloseContextNodeAction> transportCloseContextNodeActionProvider;

    private final Provider<TransportCreateIndexAction> transportCreateIndexActionProvider;
    private final Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider;
    private final Provider<TransportGetIndexTemplatesAction> transportGetIndexTemplatesActionProvider;
    private final Provider<TransportPutIndexTemplateAction> transportPutIndexTemplateActionProvider;
    private final Provider<TransportDeleteIndexTemplateAction> transportDeleteIndexTemplateActionProvider;
    private final Provider<TransportClusterUpdateSettingsAction> transportClusterUpdateSettingsActionProvider;
    private final Provider<TransportCountAction> transportCountActionProvider;
    private final Provider<TransportDeleteByQueryAction> transportDeleteByQueryActionProvider;
    private final Provider<TransportDeleteAction> transportDeleteActionProvider;

    private final Provider<TransportGetAction> transportGetActionProvider;
    private final Provider<TransportMultiGetAction> transportMultiGetActionProvider;
    private final Provider<SymbolBasedTransportShardUpsertAction> symbolBasedTransportShardUpsertActionProvider;
    private final Provider<TransportShardUpsertAction> transportShardUpsertActionProvider;
    private final Provider<TransportPutMappingAction> transportPutMappingActionProvider;
    private final Provider<TransportRefreshAction> transportRefreshActionProvider;
    private final Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider;

    private final Provider<TransportJobAction> transportJobInitActionProvider;

    @Inject
    public TransportActionProvider(Provider<TransportDistributedResultAction> transportDistributedResultActionProvider,
                                   Provider<TransportFetchNodeAction> transportFetchNodeActionProvider,
                                   Provider<TransportCloseContextNodeAction> transportCloseContextNodeActionProvider,
                                   Provider<TransportCreateIndexAction> transportCreateIndexActionProvider,
                                   Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider,
                                   Provider<TransportGetIndexTemplatesAction> transportGetIndexTemplatesActionProvider,
                                   Provider<TransportPutIndexTemplateAction> transportPutIndexTemplateActionProvider,
                                   Provider<TransportDeleteIndexTemplateAction> transportDeleteIndexTemplateActionProvider,
                                   Provider<TransportClusterUpdateSettingsAction> transportClusterUpdateSettingsActionProvider,
                                   Provider<TransportCountAction> transportCountActionProvider,
                                   Provider<TransportDeleteByQueryAction> transportDeleteByQueryActionProvider,
                                   Provider<TransportDeleteAction> transportDeleteActionProvider,
                                   Provider<TransportGetAction> transportGetActionProvider,
                                   Provider<TransportMultiGetAction> transportMultiGetActionProvider,
                                   Provider<SymbolBasedTransportShardUpsertAction> symbolBasedTransportShardUpsertActionProvider,
                                   Provider<TransportShardUpsertAction> transportShardUpsertActionProvider,
                                   Provider<TransportPutMappingAction> transportPutMappingActionProvider,
                                   Provider<TransportRefreshAction> transportRefreshActionProvider,
                                   Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider, Provider<TransportJobAction> transportJobInitActionProvider) {
        this.transportCreateIndexActionProvider = transportCreateIndexActionProvider;
        this.transportDeleteIndexActionProvider = transportDeleteIndexActionProvider;
        this.transportPutIndexTemplateActionProvider = transportPutIndexTemplateActionProvider;
        this.transportGetIndexTemplatesActionProvider = transportGetIndexTemplatesActionProvider;
        this.transportDeleteIndexTemplateActionProvider = transportDeleteIndexTemplateActionProvider;
        this.transportClusterUpdateSettingsActionProvider = transportClusterUpdateSettingsActionProvider;
        this.transportCountActionProvider = transportCountActionProvider;
        this.transportDeleteByQueryActionProvider = transportDeleteByQueryActionProvider;
        this.transportDeleteActionProvider = transportDeleteActionProvider;
        this.transportGetActionProvider = transportGetActionProvider;
        this.transportMultiGetActionProvider = transportMultiGetActionProvider;
        this.symbolBasedTransportShardUpsertActionProvider = symbolBasedTransportShardUpsertActionProvider;
        this.transportShardUpsertActionProvider = transportShardUpsertActionProvider;
        this.transportDistributedResultActionProvider = transportDistributedResultActionProvider;
        this.transportFetchNodeActionProvider = transportFetchNodeActionProvider;
        this.transportCloseContextNodeActionProvider = transportCloseContextNodeActionProvider;
        this.transportPutMappingActionProvider = transportPutMappingActionProvider;
        this.transportRefreshActionProvider = transportRefreshActionProvider;
        this.transportUpdateSettingsActionProvider = transportUpdateSettingsActionProvider;
        this.transportJobInitActionProvider = transportJobInitActionProvider;
    }


    public TransportCreateIndexAction transportCreateIndexAction() {
        return transportCreateIndexActionProvider.get();
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

    public TransportCountAction transportCountAction() {
        return transportCountActionProvider.get();
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

    public TransportShardUpsertActionDelegate transportShardUpsertActionDelegate() {
        return new TransportShardUpsertActionDelegateImpl(transportShardUpsertActionProvider.get());
    }

    public SymbolBasedTransportShardUpsertActionDelegate symbolBasedTransportShardUpsertActionDelegate() {
        return new SymbolBasedTransportShardUpsertActionDelegateImpl(symbolBasedTransportShardUpsertActionProvider.get());
    }

    public TransportJobAction transportJobInitAction() {
        return transportJobInitActionProvider.get();
    }

    public TransportDistributedResultAction transportDistributedResultAction() {
        return transportDistributedResultActionProvider.get();
    }

    public TransportFetchNodeAction transportFetchNodeAction() {
        return transportFetchNodeActionProvider.get();
    }

    public TransportCloseContextNodeAction transportCloseContextNodeAction() {
        return transportCloseContextNodeActionProvider.get();
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
}
