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

package io.crate.execution.ddl.views;

import io.crate.metadata.TableIdent;
import io.crate.metadata.view.ViewsMetaData;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public final class TransportCreateViewAction extends TransportMasterNodeAction<CreateViewRequest, CreateViewResponse> {

    @Inject
    public TransportCreateViewAction(Settings settings,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings,
            "crate/sql/views/create",
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            CreateViewRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected CreateViewResponse newResponse() {
        return new CreateViewResponse(false);
    }

    @Override
    protected void masterOperation(CreateViewRequest request, ClusterState state, ActionListener<CreateViewResponse> listener) {
        ViewsMetaData views = state.metaData().custom(ViewsMetaData.TYPE);
        if (conflictsWithTable(request.name(), state.metaData()) || conflictsWithView(request, views)) {
            listener.onResponse(new CreateViewResponse(true));
        } else {
            clusterService.submitStateUpdateTask("views/create [" + request.name() + "]",
                new AckedClusterStateUpdateTask<CreateViewResponse>(Priority.HIGH, request, listener) {

                    boolean alreadyExitsFailure = false;

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        ViewsMetaData views = currentState.metaData().custom(ViewsMetaData.TYPE);
                        if (conflictsWithTable(request.name(), currentState.metaData()) || conflictsWithView(request, views)) {
                            alreadyExitsFailure = true;
                            return currentState;
                        }
                        return ClusterState.builder(currentState)
                            .metaData(
                                MetaData.builder()
                                    .putCustom(
                                        ViewsMetaData.TYPE,
                                        ViewsMetaData.addOrReplace(views, request.name(), request.query()))
                                    .build()
                            ).build();
                    }

                    @Override
                    protected CreateViewResponse newResponse(boolean acknowledged) {
                        return new CreateViewResponse(alreadyExitsFailure);
                    }
                });
        }
    }

    private boolean conflictsWithTable(TableIdent viewName, MetaData indexMetaData) {
        return indexMetaData.hasIndex(viewName.indexName());
    }

    private boolean conflictsWithView(CreateViewRequest request, ViewsMetaData views) {
        return !request.replaceExisting() && views != null && views.contains(request.name().fqn());
    }

    @Override
    protected ClusterBlockException checkBlock(CreateViewRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
