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

package io.crate.execution.ddl.tables;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.RelationName;
import io.crate.metadata.view.ViewsMetaData;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

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

    public static final String NAME = "tables:admin/create";

    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;

    @Inject
    public TransportCreateTableAction(Settings settings,
                                      TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      TransportCreateIndexAction transportCreateIndexAction,
                                      TransportPutIndexTemplateAction transportPutIndexTemplateAction) {
        super(settings,
            NAME,
            transportService,
            clusterService, threadPool,
            actionFilters,
            indexNameExpressionResolver,
            CreateTableRequest::new);
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateTableResponse newResponse() {
        return new CreateTableResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(CreateTableRequest request, ClusterState state) {
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
    protected void masterOperation(final CreateTableRequest request, final ClusterState state, final ActionListener<CreateTableResponse> listener) {
        final RelationName relationName = request.getTableName();
        if (viewsExists(relationName, state)) {
            listener.onFailure(new RelationAlreadyExists(relationName));
            return;
        }
        if (request.getCreateIndexRequest() != null) {
            CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
            ActionListener<CreateIndexResponse> wrappedListener = ActionListener.wrap(
                response -> listener.onResponse(new CreateTableResponse(response.isShardsAcked())),
                listener::onFailure
            );
            transportCreateIndexAction.masterOperation(createIndexRequest, state, wrappedListener);
        } else if (request.getPutIndexTemplateRequest() != null) {
            PutIndexTemplateRequest putIndexTemplateRequest = request.getPutIndexTemplateRequest();
            ActionListener<PutIndexTemplateResponse> wrappedListener = ActionListener.wrap(
                response -> listener.onResponse(new CreateTableResponse(response.isAcknowledged())),
                listener::onFailure
            );
            transportPutIndexTemplateAction.masterOperation(putIndexTemplateRequest, state, wrappedListener);
        } else {
            throw new IllegalStateException("Unknown table request");
        }
    }

    private static boolean viewsExists(RelationName relationName, ClusterState state) {
        ViewsMetaData views = state.metaData().custom(ViewsMetaData.TYPE);
        return views != null && views.contains(relationName);
    }
}
