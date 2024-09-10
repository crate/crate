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
import static org.elasticsearch.cluster.metadata.MetadataIndexService.setIndexVersionCreatedSetting;
import static org.elasticsearch.cluster.metadata.MetadataIndexService.validateSoftDeletesSetting;

import java.io.IOException;
import java.util.HashSet;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetadataIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.exceptions.RelationAlreadyExists;
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
@Singleton
public class TransportCreateTableAction extends TransportMasterNodeAction<CreateTableRequest, CreateTableResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<CreateTableResponse> {

        public static final String NAME = "internal:crate:sql/tables/admin/create";

        public Action() {
            super(NAME);
        }
    }

    private final MetadataIndexService createIndexService;
    private final MetadataIndexTemplateService indexTemplateService;

    @Inject
    public TransportCreateTableAction(TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      MetadataIndexService createIndexService,
                                      MetadataIndexTemplateService indexTemplateService) {
        super(
            ACTION.name(),
            transportService,
            clusterService, threadPool,
            CreateTableRequest::new
        );
        this.createIndexService = createIndexService;
        this.indexTemplateService = indexTemplateService;
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

        var isPartitioned = request.partitionedBy().isEmpty() == false;
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

        if (createTableRequest.partitionedBy().isEmpty()) {
            createIndex(createTableRequest, listener);
        } else {
            indexTemplateService.putTemplate(createTableRequest, listener);
        }
    }

    /**
     * Similar to {@link TransportCreateIndexAction#masterOperation}
     * but also can pass on CrateDB specific objects to build mapping only at the latest stage.
     *
     * @param mapping is NOT NULL if passed mapping without OID-s can be used directly (for Pre 5.4 code)
     * or NULL if we have to build it and assign OID out of references.
     */
    private void createIndex(CreateTableRequest createTableRequest, ActionListener<CreateTableResponse> listener) {
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
            .aliases(new HashSet<>()) // Before we used CreateIndexRequest with an empty set, it's changed only on resizing indices.
            .waitForActiveShards(ActiveShardCount.DEFAULT); // Before we used CreateIndexRequest with default active shards count, it's changed only on resizing indices.

        createIndexService.create(
            updateRequest,
            createTableRequest,
            wrappedListener.map(response ->
                new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName))
        );

    }

    private static void validateSettings(Settings settings, ClusterState state) {
        var indexSettingsBuilder = Settings.builder();
        indexSettingsBuilder.put(settings);
        setIndexVersionCreatedSetting(indexSettingsBuilder, state);
        validateSoftDeletesSetting(indexSettingsBuilder.build());
    }
}
