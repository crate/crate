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

import static org.elasticsearch.action.support.master.AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.setIndexVersionCreatedSetting;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.validateSoftDeletesSetting;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

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

    private final MetadataCreateIndexService createIndexService;
    private final MetadataIndexTemplateService indexTemplateService;
    private final NodeContext nodeContext;
    private final IndexScopedSettings indexScopedSettings;

    @Inject
    public TransportCreateTableAction(TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      MetadataCreateIndexService createIndexService,
                                      MetadataIndexTemplateService indexTemplateService,
                                      NodeContext nodeContext,
                                      IndexScopedSettings indexScopedSettings) {
        super(
            NAME,
            transportService,
            clusterService, threadPool,
            CreateTableRequest::new
        );
        this.createIndexService = createIndexService;
        this.indexTemplateService = indexTemplateService;
        this.nodeContext = nodeContext;
        this.indexScopedSettings = indexScopedSettings;
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
        var relationName = request.getTableName();
        assert relationName != null : "relationName must not be null";

        var isPartitioned = request.getPutIndexTemplateRequest() != null || request.partitionedBy().isEmpty() == false;
        if (isPartitioned) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        } else {
            return state.blocks().indexBlockedException(
                ClusterBlockLevel.METADATA_WRITE,
                relationName.indexNameOrAlias()
            );
        }
    }

    @Override
    protected void masterOperation(final CreateTableRequest createTableRequest,
                                   final ClusterState state,
                                   final ActionListener<CreateTableResponse> listener) {
        final RelationName relationName = createTableRequest.getTableName();
        if (state.metadata().contains(relationName)) {
            listener.onFailure(new RelationAlreadyExists(relationName));
            return;
        }

        validateSettings(createTableRequest.settings(), state);

        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_5_4_0)) {
            if (createTableRequest.partitionedBy().isEmpty()) {
                createIndex(createTableRequest, listener, null);
            } else {
                createTemplate(createTableRequest, listener, null);
            }
        } else {
            // TODO: Remove BWC branch in 5.5
            assert createTableRequest.getCreateIndexRequest() != null || createTableRequest.getPutIndexTemplateRequest() != null : "Unknown request type";
            if (createTableRequest.getCreateIndexRequest() != null) {
                assert createTableRequest.getCreateIndexRequest().mapping() != null : "Pre 5.4 createTableRequest must have not-null mapping.";
                createIndex(createTableRequest, listener, createTableRequest.getCreateIndexRequest().mapping());
            } else {
                assert createTableRequest.getPutIndexTemplateRequest().mapping() != null : "Pre 5.4 createTableRequest must have not-null mapping.";
                createTemplate(createTableRequest, listener, createTableRequest.getPutIndexTemplateRequest().mapping());
            }
        }
    }

    /**
     * Similar to {@link TransportCreateIndexAction#masterOperation}
     * but also can pass on CrateDB specific objects to build mapping only at the latest stage.
     *
     * @param mapping is NOT NULL if passed mapping without OID-s can be used directly (for Pre 5.4 code)
     * or NULL if we have to build it and assign OID out of references.
     */
    private void createIndex(CreateTableRequest createTableRequest, ActionListener<CreateTableResponse> listener, @Nullable String mapping) {
        ActionListener<CreateIndexResponse> wrappedListener = ActionListener.wrap(
            response -> listener.onResponse(new CreateTableResponse(response.isShardsAcknowledged())),
            listener::onFailure
        );
        String cause = "api"; // Before we used CreateIndexRequest with an empty cause which turned into "api".

        final String indexName = createTableRequest.getTableName().indexNameOrAlias(); // getTableName call is BWC.
        final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
            cause, indexName, indexName)
            .ackTimeout(DEFAULT_ACK_TIMEOUT) // Before we used CreateIndexRequest with default ack timeout.
            .masterNodeTimeout(createTableRequest.masterNodeTimeout())
            .settings(createTableRequest.settings())
            .mapping(mapping)
            .aliases(new HashSet<>()) // Before we used CreateIndexRequest with an empty set, it's changed only on resizing indices.
            .waitForActiveShards(ActiveShardCount.DEFAULT); // Before we used CreateIndexRequest with default active shards count, it's changed only on resizing indices.

        createIndexService.createIndex(
            nodeContext,
            updateRequest,
            createTableRequest,
            wrappedListener.map(response ->
                new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName))
        );

    }

    /**
     * Similar to {@link TransportPutIndexTemplateAction#masterOperation}
     * but also can pass on CrateDB specific objects to build mapping only at the latest stage.
     *
     * @param mapping is NOT NULL if passed mapping without OID-s can be used directly (for Pre 5.4 code)
     * or NULL if we have to build it and assign OID out of references.
     */
    private void createTemplate(CreateTableRequest createTableRequest, ActionListener<CreateTableResponse> listener, @Nullable String mapping) {
        ActionListener<AcknowledgedResponse> wrappedListener = ActionListener.wrap(
            response -> listener.onResponse(new CreateTableResponse(response.isAcknowledged())),
            listener::onFailure
        );

        RelationName relationName = createTableRequest.getTableName(); // getTableName call is BWC.
        final Settings.Builder templateSettingsBuilder = Settings.builder();
        templateSettingsBuilder.put(createTableRequest.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        indexScopedSettings.validate(templateSettingsBuilder.build(), true); // templates must be consistent with regards to dependencies
        String name = PartitionName.templateName(relationName.schema(), relationName.name());
        indexTemplateService.putTemplate(new MetadataIndexTemplateService.PutRequest("api", name)
                .patterns(Collections.singletonList(PartitionName.templatePrefix(
                    relationName.schema(),
                    relationName.name())))
                .settings(templateSettingsBuilder.build())
                .mapping(mapping)
                .aliases(Set.of(new Alias(relationName.indexNameOrAlias()))) // We used PutIndexTemplateRequest which creates a single alias
                .create(true) // We used PutIndexTemplateRequest with explicit 'true'
                .masterTimeout(createTableRequest.masterNodeTimeout())
                .version(null),
            // We used PutIndexTemplateRequest with default version value

            createTableRequest,
            new MetadataIndexTemplateService.PutListener() {
                @Override
                public void onResponse(MetadataIndexTemplateService.PutResponse response) {
                    wrappedListener.onResponse(new AcknowledgedResponse(response.acknowledged()));
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> new ParameterizedMessage("failed to put template [{}]", name), e);
                    wrappedListener.onFailure(e);
                }
            });
    }

    private static void validateSettings(Settings settings, ClusterState state) {
        var indexSettingsBuilder = Settings.builder();
        indexSettingsBuilder.put(settings);
        setIndexVersionCreatedSetting(indexSettingsBuilder, state);
        validateSoftDeletesSetting(indexSettingsBuilder.build());
    }
}
