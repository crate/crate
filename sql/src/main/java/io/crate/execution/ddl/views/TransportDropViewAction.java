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

import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.view.ViewsMetaData;
import io.crate.es.action.ActionListener;
import io.crate.es.action.support.master.TransportMasterNodeAction;
import io.crate.es.cluster.AckedClusterStateUpdateTask;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.cluster.block.ClusterBlockLevel;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.metadata.MetaData;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.Priority;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.settings.Settings;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

import java.util.List;

public final class TransportDropViewAction extends TransportMasterNodeAction<DropViewRequest, DropViewResponse> {

    private final DDLClusterStateService ddlClusterStateService;

    @Inject
    public TransportDropViewAction(Settings settings,
                                   TransportService transportService,
                                   ClusterService clusterService,
                                   ThreadPool threadPool,
                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                   DDLClusterStateService ddlClusterStateService) {
        super(settings,
            "internal:crate:sql/views/drop",
            transportService,
            clusterService,
            threadPool,
            indexNameExpressionResolver,
            DropViewRequest::new);
        this.ddlClusterStateService = ddlClusterStateService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected DropViewResponse newResponse() {
        return new DropViewResponse();
    }

    @Override
    protected void masterOperation(DropViewRequest request, ClusterState state, ActionListener<DropViewResponse> listener) {
        clusterService.submitStateUpdateTask("views/drop",
            new AckedClusterStateUpdateTask<DropViewResponse>(Priority.HIGH, request, listener) {

                private List<RelationName> missing;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    ViewsMetaData views = currentState.metaData().custom(ViewsMetaData.TYPE);
                    if (views == null) {
                        missing = request.names();
                        return currentState;
                    }
                    ViewsMetaData.RemoveResult removeResult = views.remove(request.names());
                    missing = removeResult.missing();
                    if (!removeResult.missing().isEmpty() && !request.ifExists()) {
                        // We missed a view -> This is an error case so we must not update the cluster state
                        return currentState;
                    }
                    currentState = ddlClusterStateService.onDropView(currentState, request.names());
                    return ClusterState.builder(currentState)
                        .metaData(
                            MetaData.builder(currentState.metaData())
                                .putCustom(ViewsMetaData.TYPE, removeResult.updatedViews())
                                .build()
                        )
                        .build();
                }

                @Override
                protected DropViewResponse newResponse(boolean acknowledged) {
                    return new DropViewResponse(missing);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(DropViewRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
