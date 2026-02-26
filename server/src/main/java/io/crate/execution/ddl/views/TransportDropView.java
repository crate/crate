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
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;

public final class TransportDropView extends TransportMasterNodeAction<DropViewRequest, DropViewResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<DropViewResponse> {
        private static final String NAME = "internal:crate:sql/views/drop";

        private Action() {
            super(NAME);
        }
    }

    private final DDLClusterStateService ddlClusterStateService;

    @Inject
    public TransportDropView(TransportService transportService,
                             ClusterService clusterService,
                             ThreadPool threadPool,
                             DDLClusterStateService ddlClusterStateService) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            DropViewRequest::new
        );
        this.ddlClusterStateService = ddlClusterStateService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected DropViewResponse read(StreamInput in) throws IOException {
        return new DropViewResponse(in);
    }

    @Override
    protected void masterOperation(DropViewRequest request,
                                   ClusterState state,
                                   ActionListener<DropViewResponse> listener) {
        clusterService.submitStateUpdateTask("views/drop",
            new AckedClusterStateUpdateTask<>(Priority.HIGH, request, listener) {

                private final List<RelationName> missing = new ArrayList<>();

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return doDropView(request, currentState, missing);
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

    @VisibleForTesting
    ClusterState doDropView(DropViewRequest request, ClusterState currentState, List<RelationName> missing) {
        for (RelationName name : request.names()) {
            RelationMetadata view = currentState.metadata().getRelation(name);
            if (view instanceof RelationMetadata.View == false) {
                missing.add(name);
            }
        }
        if (missing.isEmpty() == false && request.ifExists() == false) {
            // We missed a view -> This is an error case so we must not update the cluster state
            return currentState;
        }

        currentState = ddlClusterStateService.onDropView(currentState, request.names());
        Metadata.Builder mdBuilder = new Metadata.Builder(currentState.metadata());
        for (RelationName name : request.names()) {
            if (missing.contains(name) == false) {
                mdBuilder.dropRelation(name);
            }
        }
        return ClusterState.builder(currentState)
            .metadata(mdBuilder)
            .build();
    }
}
