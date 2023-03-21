/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.ddl.tables;

import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.setIndexVersionCreatedSetting;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.validateSoftDeletesSetting;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.validateAndAddTemplate;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.view.ViewsMetadata;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Action to perform creation of tables on the master but avoid race conditions with creating views.
 *
 * Regular tables are created through the creation of ES indices, see {@link TransportCreateIndexAction}.
 * Partitioned tables are created through ES templates, see {@link TransportPutIndexTemplateAction}.
 *
 * To atomically run the actions on the master, this action wraps around the ES actions and runs them
 * inside this action on the master with checking for views beforehand.
 *
 * See also: {@link io.crate.execution.ddl.views.TransportCreateViewAction}
 */
public class TransportCreateTableAction extends TransportMasterNodeAction<CreateTableRequest, CreateTableResponse> {

    public static final String NAME = "internal:crate:sql/tables/admin/create";

    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;
    private final IndicesService indicesService;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportCreateTableAction(TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      TransportCreateIndexAction transportCreateIndexAction,
                                      TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                                      IndicesService indicesService,
                                      NamedXContentRegistry xContentRegistry) {
        super(
            NAME,
            transportService,
            clusterService, threadPool,
            CreateTableRequest::new
        );
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
        this.indicesService = indicesService;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateTableResponse read(StreamInput in) throws IOException {
        return new CreateTableResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateTableRequest request, ClusterState state) {
        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_5_3_0)) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.relationName().indexNameOrAlias());
        }

        if (request.getCreateIndexRequest() != null) {
            CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
            return transportCreateIndexAction.checkBlock(createIndexRequest, state);
        } else if (request.getPutIndexTemplateRequest() != null) {
            PutIndexTemplateRequest putIndexTemplateRequest = request.getPutIndexTemplateRequest();
            return transportPutIndexTemplateAction.checkBlock(putIndexTemplateRequest, state);
        } else {
            throw new IllegalStateException("Unknown table request");
        }
    }

    @Override
    protected void masterOperation(final CreateTableRequest request,
                                   final ClusterState state,
                                   final ActionListener<CreateTableResponse> listener) throws Exception {
        final RelationName relationName = request.getTableName();
        if (viewsExists(relationName, state)) {
            listener.onFailure(new RelationAlreadyExists(relationName));
            return;
        }

        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_5_3_0)) {
            validateSettings(request.settings(), state);
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());

            if (request.isPartitioned()) {

                // TODO: migrate more settings/request validations from MetadataIndexTemplateService
                IndexTemplateMetadata templateMetadata = createTemplate(state,
                                                                        request.relationName().schema(),
                                                                        request.relationName().name(),
                                                                        request.settings(),
                                                                        request.mapping(),
                                                                        request.relationName().indexNameOrAlias()
                );
                metadataBuilder.put(templateMetadata);
            } else {
                ActionListener<CreateIndexResponse> wrappedListener = ActionListener.wrap(
                    response -> listener.onResponse(new CreateTableResponse(response.isShardsAcknowledged())),
                    listener::onFailure
                );
                transportCreateIndexAction.masterOperation(createIndexRequest, state, wrappedListener);
            }
        } else {
            createTablePre5_3(request, state, listener);
        }
    }

    private IndexTemplateMetadata createTemplate(ClusterState state,
                                                 String schemaName,
                                                 String tableName,
                                                 Settings settings,
                                                 String mapping,
                                                 String indexNameOrAlias) throws Exception {
        var templateBuilder = new IndexTemplateMetadata.Builder(PartitionName.templateName(schemaName, tableName));
        validateAndAddTemplate(
            settings,
            List.of(PartitionName.templatePrefix(schemaName, tableName)),
            mapping,
            List.of(new Alias(indexNameOrAlias)),
            templateBuilder,
            indicesService,
            xContentRegistry,
            state
        );
        return templateBuilder.build();
    }


    /**
     * Depending on table (partitioned or not), either creates only a template or only an index.
     * Kept for BWC reasons, to be removed in 5.4
     */
    private void createTablePre5_3(CreateTableRequest request, ClusterState state, ActionListener<CreateTableResponse> listener) {
        if (request.getCreateIndexRequest() != null) {
            CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
            validateSettings(createIndexRequest.settings(), state);

            ActionListener<CreateIndexResponse> wrappedListener = ActionListener.wrap(
                response -> listener.onResponse(new CreateTableResponse(response.isShardsAcknowledged())),
                listener::onFailure
            );
            transportCreateIndexAction.masterOperation(createIndexRequest, state, wrappedListener);
        } else if (request.getPutIndexTemplateRequest() != null) {
            PutIndexTemplateRequest putIndexTemplateRequest = request.getPutIndexTemplateRequest();
            validateSettings(putIndexTemplateRequest.settings(), state);

            ActionListener<AcknowledgedResponse> wrappedListener = ActionListener.wrap(
                response -> listener.onResponse(new CreateTableResponse(response.isAcknowledged())),
                listener::onFailure
            );
            transportPutIndexTemplateAction.masterOperation(putIndexTemplateRequest, state, wrappedListener);
        } else {
            throw new IllegalStateException("Unknown table request");
        }
    }

    private static boolean viewsExists(RelationName relationName, ClusterState state) {
        ViewsMetadata views = state.metadata().custom(ViewsMetadata.TYPE);
        return views != null && views.contains(relationName);
    }

    private static void validateSettings(Settings settings, ClusterState state) {
        var indexSettingsBuilder = Settings.builder();
        indexSettingsBuilder.put(settings);
        setIndexVersionCreatedSetting(indexSettingsBuilder, state);
        validateSoftDeletesSetting(indexSettingsBuilder.build());
    }
}
