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

package io.crate.es.action.admin.cluster.repositories.put;

import io.crate.es.action.ActionListener;
import io.crate.es.action.support.master.AcknowledgedResponse;
import io.crate.es.action.support.master.TransportMasterNodeAction;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.ack.ClusterStateUpdateResponse;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.cluster.block.ClusterBlockLevel;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.settings.Settings;
import io.crate.es.repositories.RepositoriesService;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

/**
 * Transport action for register repository operation
 */
public class TransportPutRepositoryAction extends TransportMasterNodeAction<PutRepositoryRequest, AcknowledgedResponse> {

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportPutRepositoryAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        RepositoriesService repositoriesService, ThreadPool threadPool,
                                        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutRepositoryAction.NAME, transportService, clusterService, threadPool, indexNameExpressionResolver, PutRepositoryRequest::new);
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutRepositoryRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(final PutRepositoryRequest request, ClusterState state, final ActionListener<AcknowledgedResponse> listener) {

        repositoriesService.registerRepository(
                new RepositoriesService.RegisterRepositoryRequest("put_repository [" + request.name() + "]",
                        request.name(), request.type(), request.verify())
                .settings(request.settings())
                .masterNodeTimeout(request.masterNodeTimeout())
                .ackTimeout(request.timeout()), new ActionListener<ClusterStateUpdateResponse>() {

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

}
