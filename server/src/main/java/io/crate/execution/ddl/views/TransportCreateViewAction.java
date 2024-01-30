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

package io.crate.execution.ddl.views;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.metadata.view.ViewsMetadata;

public final class TransportCreateViewAction extends TransportMasterNodeAction<CreateViewRequest, CreateViewResponse> {

    @Inject
    public TransportCreateViewAction(TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool) {
        super(
            "internal:crate:sql/views/create",
            transportService,
            clusterService,
            threadPool,
            CreateViewRequest::new
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected CreateViewResponse read(StreamInput in) throws IOException {
        return new CreateViewResponse(in);
    }

    @Override
    protected void masterOperation(CreateViewRequest request,
                                   ClusterState state,
                                   ActionListener<CreateViewResponse> listener) {

        if (!request.replaceExisting() && state.metadata().contains(request.name())) {
            listener.onResponse(new CreateViewResponse(true));
        } else {
            clusterService.submitStateUpdateTask("views/create [" + request.name() + "]",
                new AckedClusterStateUpdateTask<CreateViewResponse>(Priority.HIGH, request, listener) {

                    boolean alreadyExitsFailure = false;

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        ViewsMetadata views = currentState.metadata().custom(ViewsMetadata.TYPE);
                        if (currentState.metadata().contains(request.name())) {
                            boolean replacesView = request.replaceExisting() && views != null && views.contains(request.name());
                            if (!replacesView) {
                                alreadyExitsFailure = true;
                                return currentState;
                            }
                        }
                        return ClusterState.builder(currentState)
                            .metadata(
                                Metadata.builder(currentState.metadata())
                                    .putCustom(
                                        ViewsMetadata.TYPE,
                                        ViewsMetadata.addOrReplace(
                                            views,
                                            request.name(),
                                            request.query(),
                                            request.owner(),
                                            request.searchPath()))
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

    @Override
    protected ClusterBlockException checkBlock(CreateViewRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
