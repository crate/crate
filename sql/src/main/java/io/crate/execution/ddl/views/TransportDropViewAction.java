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

import java.util.List;

public final class TransportDropViewAction extends TransportMasterNodeAction<DropViewRequest, DropViewResponse> {

    @Inject
    public TransportDropViewAction(Settings settings,
                                   TransportService transportService,
                                   ClusterService clusterService,
                                   ThreadPool threadPool,
                                   ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings,
            "crate/sql/views/drop",
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            DropViewRequest::new);
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

                private List<TableIdent> missing;

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
                    return ClusterState.builder(currentState)
                        .metaData(MetaData.builder().putCustom(ViewsMetaData.TYPE, removeResult.updatedViews()).build())
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
