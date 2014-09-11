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

import io.crate.action.sql.query.TransportQueryShardAction;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;

public class TransportActionProvider {

    private final Provider<TransportShardBulkAction> transportShardBulkActionProvider;
    private final Provider<TransportCollectNodeAction> transportCollectNodeActionProvider;
    private final Provider<TransportMergeNodeAction> transportMergeNodeActionProvider;
    private final Provider<TransportSearchAction> transportSearchActionProvider;

    private final Provider<TransportCreateIndexAction> transportCreateIndexActionProvider;
    private final Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider;
    private final Provider<TransportPutIndexTemplateAction> transportPutIndexTemplateActionProvider;
    private final Provider<TransportDeleteIndexTemplateAction> transportDeleteIndexTemplateActionProvider;
    private final Provider<TransportClusterUpdateSettingsAction> transportClusterUpdateSettingsActionProvider;
    private final Provider<TransportCountAction> transportCountActionProvider;
    private final Provider<TransportDeleteByQueryAction> transportDeleteByQueryActionProvider;
    private final Provider<TransportDeleteAction> transportDeleteActionProvider;

    private final Provider<TransportGetAction> transportGetActionProvider;
    private final Provider<TransportMultiGetAction> transportMultiGetActionProvider;
    private final Provider<TransportIndexAction> transportIndexActionProvider;
    private final Provider<TransportQueryShardAction> transportQueryShardActionProvider;
    private final Provider<TransportUpdateAction> transportUpdateActionProvider;

    @Inject
    public TransportActionProvider(Provider<TransportShardBulkAction> transportShardBulkActionProvider,
                                   Provider<TransportCollectNodeAction> transportCollectNodeActionProvider,
                                   Provider<TransportMergeNodeAction> transportMergeNodeActionProvider,
                                   Provider<TransportCreateIndexAction> transportCreateIndexActionProvider,
                                   Provider<TransportDeleteIndexAction> transportDeleteIndexActionProvider,
                                   Provider<TransportPutIndexTemplateAction> transportPutIndexTemplateActionProvider,
                                   Provider<TransportDeleteIndexTemplateAction> transportDeleteIndexTemplateActionProvider,
                                   Provider<TransportClusterUpdateSettingsAction> transportClusterUpdateSettingsActionProvider,
                                   Provider<TransportCountAction> transportCountActionProvider,
                                   Provider<TransportDeleteByQueryAction> transportDeleteByQueryActionProvider,
                                   Provider<TransportDeleteAction> transportDeleteActionProvider,
                                   Provider<TransportGetAction> transportGetActionProvider,
                                   Provider<TransportMultiGetAction> transportMultiGetActionProvider,
                                   Provider<TransportIndexAction> transportIndexActionProvider,
                                   Provider<TransportUpdateAction> transportUpdateActionProvider,
                                   Provider<TransportQueryShardAction> transportQueryShardActionProvider,
                                   Provider<TransportSearchAction> transportSearchActionProvider) {
        this.transportCreateIndexActionProvider = transportCreateIndexActionProvider;
        this.transportDeleteIndexActionProvider = transportDeleteIndexActionProvider;
        this.transportPutIndexTemplateActionProvider = transportPutIndexTemplateActionProvider;
        this.transportDeleteIndexTemplateActionProvider = transportDeleteIndexTemplateActionProvider;
        this.transportClusterUpdateSettingsActionProvider = transportClusterUpdateSettingsActionProvider;
        this.transportCountActionProvider = transportCountActionProvider;
        this.transportDeleteByQueryActionProvider = transportDeleteByQueryActionProvider;
        this.transportDeleteActionProvider = transportDeleteActionProvider;
        this.transportGetActionProvider = transportGetActionProvider;
        this.transportMultiGetActionProvider = transportMultiGetActionProvider;
        this.transportIndexActionProvider = transportIndexActionProvider;
        this.transportQueryShardActionProvider = transportQueryShardActionProvider;
        this.transportUpdateActionProvider = transportUpdateActionProvider;
        this.transportShardBulkActionProvider = transportShardBulkActionProvider;
        this.transportCollectNodeActionProvider = transportCollectNodeActionProvider;
        this.transportMergeNodeActionProvider = transportMergeNodeActionProvider;
        this.transportSearchActionProvider = transportSearchActionProvider;
    }


    public TransportCreateIndexAction transportCreateIndexAction() {
        return transportCreateIndexActionProvider.get();
    }

    public TransportDeleteIndexAction transportDeleteIndexAction() {
        return transportDeleteIndexActionProvider.get();
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

    public TransportIndexAction transportIndexAction() {
        return transportIndexActionProvider.get();
    }

    public TransportUpdateAction transportUpdateAction() {
        return transportUpdateActionProvider.get();
    }

    public TransportShardBulkAction transportShardBulkAction() {
        return transportShardBulkActionProvider.get();
    }

    public TransportCollectNodeAction transportCollectNodeAction() {
        return transportCollectNodeActionProvider.get();
    }

    public TransportMergeNodeAction transportMergeNodeAction() {
        return transportMergeNodeActionProvider.get();
    }

    public TransportQueryShardAction transportQueryShardAction() {
        return transportQueryShardActionProvider.get();
    }

    public TransportSearchAction transportSearchAction() {
        return transportSearchActionProvider.get();
    }
}
