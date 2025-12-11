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
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Builder;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.metadata.SchemaMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.exceptions.DependentObjectsExists;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.metadata.view.ViewsMetadata.RemoveResult;
import io.crate.sql.tree.CascadeMode;

public class TransportDropSchema extends TransportMasterNodeAction<DropSchemaRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "schema/drop";

        private Action() {
            super(NAME);
        }
    }

    private final MetadataDeleteIndexService deleteIndexService;
    private final DDLClusterStateService ddlClusterStateService;

    @Inject
    public TransportDropSchema(TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool,
                               MetadataDeleteIndexService deleteIndexService,
                               DDLClusterStateService ddlClusterStateService) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            DropSchemaRequest::new
        );
        this.deleteIndexService = deleteIndexService;
        this.ddlClusterStateService = ddlClusterStateService;
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
    protected void masterOperation(DropSchemaRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        if (state.nodes().getMinNodeVersion().before(Version.V_6_2_0)) {
            throw new IllegalStateException(
                "Cannot execute DROP SCHEMA while there are <6.2.0 nodes in the cluster");
        }
        DropSchemaTask task = new DropSchemaTask(request, deleteIndexService, ddlClusterStateService);
        task.completionFuture().whenComplete(listener);
        clusterService.submitStateUpdateTask("drop-schema", task);
    }

    @Override
    protected ClusterBlockException checkBlock(DropSchemaRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }


    static class DropSchemaTask extends AckedClusterStateUpdateTask<AcknowledgedResponse> {

        private final DropSchemaRequest request;
        private final MetadataDeleteIndexService deleteIndexService;
        private final DDLClusterStateService ddlClusterStateService;

        protected DropSchemaTask(DropSchemaRequest request,
                                 MetadataDeleteIndexService deleteIndexService,
                                 DDLClusterStateService ddlClusterStateService) {
            super(Priority.NORMAL, request);
            this.request = request;
            this.deleteIndexService = deleteIndexService;
            this.ddlClusterStateService = ddlClusterStateService;
        }

        @Override
        protected AcknowledgedResponse newResponse(boolean acknowledged) {
            return new AcknowledgedResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            Metadata currentMetadata = currentState.metadata();
            ImmutableOpenMap<String, SchemaMetadata> schemas = currentMetadata.schemas();
            Builder newMetadata = Metadata.builder(currentMetadata);
            List<RelationName> droppedRelations = new ArrayList<>();
            List<Index> droppedIndices = new ArrayList<>();

            for (String schema : request.names()) {
                SchemaMetadata schemaMetadata = schemas.get(schema);
                if (schemaMetadata == null) {
                    if (request.ifExists()) {
                        continue;
                    }
                    throw new SchemaUnknownException(schema);
                }
                if (schemaMetadata.relations().isEmpty()) {
                    newMetadata.dropSchema(schema);
                } else if (request.cascadeMode() == CascadeMode.RESTRICT) {
                    throw new DependentObjectsExists("schema", schema);
                } else {
                    newMetadata.dropSchema(schema);
                    for (var cursor : schemaMetadata.relations()) {
                        RelationMetadata relation = cursor.value;
                        newMetadata.dropRelation(relation.name());
                        droppedRelations.add(relation.name());
                        for (String indexUUID : relation.indexUUIDs()) {
                            IndexMetadata index = currentMetadata.index(indexUUID);
                            droppedIndices.add(index.getIndex());
                        }
                    }
                }
            }

            ViewsMetadata views = currentMetadata.custom(ViewsMetadata.TYPE);
            List<RelationName> droppedViews;
            if (views == null) {
                droppedViews = List.of();
            } else {
                droppedViews = new ArrayList<>();
                for (String fqViewName : views.names()) {
                    IndexParts indexParts = IndexName.decode(fqViewName);
                    RelationName relationName = indexParts.toRelationName();
                    if (request.names().contains(relationName.schema())) {
                        if (request.cascadeMode() == CascadeMode.RESTRICT) {
                            throw new DependentObjectsExists("schema", relationName.schema());
                        }
                        droppedViews.add(relationName);
                    }
                }
                if (!droppedViews.isEmpty()) {
                    RemoveResult remove = views.remove(droppedViews);
                    newMetadata.putCustom(ViewsMetadata.TYPE, remove.updatedViews());
                }
            }

            ClusterState newState = ClusterState.builder(currentState)
                .metadata(newMetadata)
                .build();

            newState = deleteIndexService.deleteIndices(newState, droppedIndices);
            for (RelationName droppedRelation : droppedRelations) {
                newState = ddlClusterStateService.onDropTable(newState, droppedRelation);
            }
            newState = ddlClusterStateService.onDropView(newState, droppedViews);
            return newState;
        }
    }
}
