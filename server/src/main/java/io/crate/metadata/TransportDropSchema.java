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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.analyze.RelationNames;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.view.ViewInfo;
import io.crate.metadata.view.ViewInfoFactory;

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
    private final UserDefinedFunctionService udfService;
    private final ViewInfoFactory viewInfoFactory;

    @Inject
    public TransportDropSchema(TransportService transportService,
                               ClusterService clusterService,
                               MetadataDeleteIndexService deleteIndexService,
                               DDLClusterStateService ddlClusterStateService,
                               UserDefinedFunctionService udfService,
                               ThreadPool threadPool,
                               NodeContext nodeCtx) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            DropSchemaRequest::new
        );
        this.deleteIndexService = deleteIndexService;
        this.ddlClusterStateService = ddlClusterStateService;
        this.udfService = udfService;
        this.viewInfoFactory = new ViewInfoFactory(new RelationAnalyzer(nodeCtx));
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
        if (state.nodes().getMinNodeVersion().before(Version.V_6_3_0)) {
            throw new IllegalStateException(
                "Cannot execute DROP SCHEMA while there are <6.3.0 nodes in the cluster");
        }
        DropSchemaTask task = new DropSchemaTask(
            request,
            deleteIndexService,
            ddlClusterStateService,
            udfService,
            viewInfoFactory);
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
        private final UserDefinedFunctionService udfService;
        private final ViewInfoFactory viewInfoFactory;

        protected DropSchemaTask(DropSchemaRequest request,
                                 MetadataDeleteIndexService deleteIndexService,
                                 DDLClusterStateService ddlClusterStateService,
                                 UserDefinedFunctionService udfService,
                                 ViewInfoFactory viewInfoFactory) {
            super(Priority.NORMAL, request);
            this.request = request;
            this.deleteIndexService = deleteIndexService;
            this.ddlClusterStateService = ddlClusterStateService;
            this.udfService = udfService;
            this.viewInfoFactory = viewInfoFactory;
        }

        @Override
        protected AcknowledgedResponse newResponse(boolean acknowledged) {
            return new AcknowledgedResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            Metadata currentMetadata = currentState.metadata();
            Builder mdBuilder = Metadata.builder(currentMetadata);
            for (String schema : request.names()) {
                SchemaMetadata schemaMetadata = currentMetadata.schemas().get(schema);
                if (schemaMetadata == null && request.ifExists()) {
                    continue;
                }
                Set<RelationName> viewsToDrop = getViewsToDrop(viewInfoFactory, currentState, schema);
                mdBuilder.canDropSchema(schema, request.mode(), viewsToDrop);

                for (RelationMetadata relationMetadata : schemaMetadata.relations().values()) {
                    if (relationMetadata instanceof RelationMetadata.Table table) {
                        Collection<Index> indices = currentMetadata.getIndices(
                            table.name(),
                            List.of(),
                            false,
                            IndexMetadata::getIndex
                        );
                        mdBuilder.dropRelation(table.name());
                        currentState = ClusterState.builder(currentState).metadata(mdBuilder).build();
                        currentState = deleteIndexService.deleteIndices(currentState, indices);
                        currentState = ddlClusterStateService.onDropTable(currentState, table.name());
                        mdBuilder = Metadata.builder(currentState.metadata());
                    } else {
                        mdBuilder = mdBuilder.dropRelation(relationMetadata.name());
                    }
                }
                for (UserDefinedFunctionMetadata udf : schemaMetadata.udfs()) {
                    mdBuilder = mdBuilder.dropUDF(schema, udf.name(), udf.argumentTypes(), false);
                }
                for (RelationName view : viewsToDrop) {
                    mdBuilder = mdBuilder.dropRelation(view);
                }
                mdBuilder = mdBuilder.dropSchema(schema);
            }
            Metadata newMetadata = mdBuilder.build();
            udfService.updateImplementations(newMetadata);
            return ClusterState.builder(currentState)
                .metadata(newMetadata)
                .build();
        }
    }

    static Set<RelationName> getViewsToDrop(ViewInfoFactory viewInfoFactory, ClusterState currentState, String schema) {
        Set<RelationName> viewsToDrop = new HashSet<>();
        for (RelationMetadata.View view : currentState.metadata().relations(RelationMetadata.View.class)) {
            if (schema.equals(view.name().schema()) == false) {
                ViewInfo viewInfo = viewInfoFactory.create(view.name(), currentState);
                Consumer<RelationName> ensureNotInSchema = relationName -> {
                    if (schema.equals(relationName.schema())) {
                        viewsToDrop.add(view.name());
                    }
                };
                viewInfo.forDependentObjects(ensureNotInSchema, symbol -> {
                    if (RelationNames.getDeep(symbol).stream().anyMatch(rn -> schema.equals(rn.schema()))) {
                        viewsToDrop.add(view.name());
                    }
                    if (symbol instanceof io.crate.expression.symbol.Function fn
                        && schema.equals(fn.signature().getName().schema())) {
                        viewsToDrop.add(view.name());
                    }
                });
            }
        }
        return viewsToDrop;
    }
}
