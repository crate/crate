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

package io.crate.metadata;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SchemaMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.exceptions.SchemaAlreadyExists;

@Singleton
public class TransportCreateSchema extends TransportMasterNodeAction<CreateSchemaRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "schema/create";

        private Action() {
            super(NAME);
        }
    }

    @Inject
    public TransportCreateSchema(TransportService transportService,
                                 ClusterService clusterService,
                                 ThreadPool threadPool) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            CreateSchemaRequest::new
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(CreateSchemaRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        if (state.nodes().getMinNodeVersion().before(Version.V_6_2_0)) {
            throw new IllegalStateException(
                "Cannot execute CREATE SCHEMA while there are <6.2.0 nodes in the cluster");
        }
        CreateSchemaTask task = new CreateSchemaTask(request);
        task.completionFuture().whenComplete(listener);
        clusterService.submitStateUpdateTask("create-schema", task);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateSchemaRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }


    static class CreateSchemaTask extends AckedClusterStateUpdateTask<AcknowledgedResponse> {

        private final CreateSchemaRequest request;

        protected CreateSchemaTask(CreateSchemaRequest request) {
            super(Priority.NORMAL, request);
            this.request = request;
        }

        @Override
        protected AcknowledgedResponse newResponse(boolean acknowledged) {
            return new AcknowledgedResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            Metadata currentMetadata = currentState.metadata();
            ImmutableOpenMap<String, SchemaMetadata> schemas = currentMetadata.schemas();
            String schemaName = request.name();
            if (schemas.containsKey(schemaName)) {
                if (request.ifNotExists()) {
                    return currentState;
                }
                throw new SchemaAlreadyExists(schemaName);
            }
            return ClusterState.builder(currentState)
                .metadata(
                    Metadata.builder(currentMetadata)
                        .createSchema(schemaName)
                )
                .build();
        }
    }
}
