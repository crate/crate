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
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.exceptions.DependentObjectsExists;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.view.ViewInfo;
import io.crate.metadata.view.ViewInfoFactory;
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
    private final ViewInfoFactory viewInfoFactory;

    @Inject
    public TransportDropSchema(TransportService transportService,
                               ClusterService clusterService,
                               MetadataDeleteIndexService deleteIndexService,
                               DDLClusterStateService ddlClusterStateService,
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
            viewInfoFactory);
        task.completionFuture().whenComplete(listener);
        clusterService.submitStateUpdateTask("drop-schema", task);
    }

    @Override
    protected ClusterBlockException checkBlock(DropSchemaRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }


    static class NewState {
        Metadata.Builder builder;
        ClusterState state;

        private NewState(ClusterState state) {
            this.state = state;
            this.builder = new Metadata.Builder(state.metadata());
        }
    }

    static class DropSchemaTask extends AckedClusterStateUpdateTask<AcknowledgedResponse> {

        private final DropSchemaRequest request;
        private final MetadataDeleteIndexService deleteIndexService;
        private final DDLClusterStateService ddlClusterStateService;
        private final ViewInfoFactory viewInfoFactory;

        protected DropSchemaTask(DropSchemaRequest request,
                                 MetadataDeleteIndexService deleteIndexService,
                                 DDLClusterStateService ddlClusterStateService,
                                 ViewInfoFactory viewInfoFactory) {
            super(Priority.NORMAL, request);
            this.request = request;
            this.deleteIndexService = deleteIndexService;
            this.ddlClusterStateService = ddlClusterStateService;
            this.viewInfoFactory = viewInfoFactory;
        }

        @Override
        protected AcknowledgedResponse newResponse(boolean acknowledged) {
            return new AcknowledgedResponse(acknowledged);
        }

        private void removeRelation(NewState newState, RelationMetadata relation) {
            switch (relation) {
                case RelationMetadata.Table table -> {
                    ClusterState state = newState.state;
                    Collection<Index> indices = state.metadata().getIndices(
                        table.name(),
                        List.of(),
                        false,
                        IndexMetadata::getIndex
                    );
                    state = ClusterState.builder(state)
                        .metadata(newState.builder.dropRelation(table.name()))
                        .build();
                    state = deleteIndexService.deleteIndices(state, indices);
                    state = ddlClusterStateService.onDropTable(state, table.name());

                    newState.builder = new Metadata.Builder(state.metadata());
                    newState.state = state;
                }
                case RelationMetadata.View view -> {
                    newState.builder.dropRelation(view.name());
                }
                case RelationMetadata.ForeignTable foreignTable -> {
                    newState.builder.dropRelation(foreignTable.name());
                }
                case RelationMetadata.BlobTable _ -> {
                    // Blobs can't be in custom schema
                }
            }
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            Metadata currentMetadata = currentState.metadata();
            NewState newState = new NewState(currentState);
            CascadeMode mode = request.mode();
            for (String schema : request.names()) {
                SchemaMetadata schemaMetadata = currentMetadata.schemas().get(schema);
                if (schemaMetadata == null) {
                    if (request.ifExists()) {
                        continue;
                    }
                    throw new SchemaUnknownException(schema);
                }

                if (mode == CascadeMode.RESTRICT && !schemaMetadata.isEmpty()) {
                    throw new DependentObjectsExists("schema", schema);
                } else if (mode == CascadeMode.CASCADE) {

                    Predicate<Symbol> isUdfInSchema = symbol ->
                        symbol instanceof Function fn && schema.equals(fn.signature().getName().schema());
                    var usesSchemaVisitor = new UsesObjectVisitor(
                        relation -> schema.equals(relation.schema()),
                        fn -> schema.equals(fn.schema())
                    );

                    // First phase: check anything that depends on the schema
                    // (views or udfs)
                    ArrayList<RelationName> dependentObjects = new ArrayList<>();
                    for (var relation : currentMetadata.relations(RelationMetadata.class)) {
                        if (schema.equals(relation.name().schema())) {
                            removeRelation(newState, relation);
                            continue;
                        }
                        switch (relation) {
                            case RelationMetadata.View view -> {
                                ViewInfo viewInfo = viewInfoFactory.create(view.name(), currentState);
                                AnalyzedRelation viewRelation = viewInfo.relation();
                                if (viewRelation == null) {
                                    continue;
                                }
                                if (usesSchemaVisitor.hasMatch(viewRelation)) {
                                    newState.builder.dropRelation(view.name());
                                    dependentObjects.add(view.name());
                                }
                            }
                            case RelationMetadata.Table table -> {
                                for (var column : table.columns()) {
                                    if (column.any(isUdfInSchema)
                                            || column instanceof GeneratedReference genRef && genRef.generatedExpression().any(isUdfInSchema)) {
                                        removeRelation(newState, table);
                                        dependentObjects.add(table.name());
                                        break;
                                    }
                                }
                            }
                            case RelationMetadata.ForeignTable _ -> {
                                // can't use udfs/relations
                            }
                            case RelationMetadata.BlobTable _ -> {
                                // can't use udfs/relations
                            }
                        }
                    }

                    // Second phase: Recursively delete anything using deleted dependent objects
                    dropCascade(newState, currentState, dependentObjects);
                }

                newState.builder.dropSchema(schema);
            }
            Metadata newMetadata = newState.builder.build();
            return ClusterState.builder(newState.state)
                .metadata(newMetadata)
                .build();
        }

        private void dropCascade(NewState newState,
                                 ClusterState currentState,
                                 ArrayList<RelationName> dependentObjects) {
            if (dependentObjects.isEmpty()) {
                return;
            }
            ArrayList<RelationName> newDependentObjects = new ArrayList<>();
            UsesObjectVisitor usesObjectVisitor = new UsesObjectVisitor(
                name -> dependentObjects.contains(name),
                fn -> false
            );
            for (var relation : newState.builder.relations()) {
                switch (relation) {
                    case RelationMetadata.View view -> {
                        ViewInfo viewInfo = viewInfoFactory.create(view.name(), currentState);
                        AnalyzedRelation viewRelation = viewInfo.relation();
                        if (viewRelation == null) {
                            continue;
                        }
                        if (usesObjectVisitor.hasMatch(viewRelation)) {
                            newState.builder.dropRelation(view.name());
                            newDependentObjects.add(view.name());
                        }
                    }
                    case RelationMetadata.Table _ -> {
                        // Table can only use UDFs - schema was checked in first loop
                    }
                    case RelationMetadata.ForeignTable _ -> {
                        // can't use udfs/relations
                    }
                    case RelationMetadata.BlobTable _ -> {
                        // can't use udfs/relations
                    }
                }
            }
            dropCascade(newState, currentState, newDependentObjects);
        }
    }

    /// Traverses into child symbols and recurses into SelectSymbol
    static class UsesObjectVisitor extends AnalyzedRelationVisitor<Void, Void> {

        private final Predicate<RelationName> relIsMatch;
        private final Predicate<FunctionName> fnIsMatch;
        boolean foundMatch = false;

        private UsesObjectVisitor(Predicate<RelationName> relIsMatch, Predicate<FunctionName> fnIsMatch) {
            this.relIsMatch = relIsMatch;
            this.fnIsMatch = fnIsMatch;
        }

        public boolean hasMatch(AnalyzedRelation relation) {
            foundMatch = false;
            relation.accept(this, null);
            if (foundMatch) {
                return true;
            }
            relation.visitSymbols(this::onRootSymbol);
            return foundMatch;
        }

        @Override
        protected Void visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            if (relIsMatch.test(relation.relationName())) {
                foundMatch = true;
            }
            return null;
        }

        @Override
        public Void visitView(AnalyzedView view, Void context) {
            if (relIsMatch.test(view.relationName())) {
                foundMatch = true;
            } else {
                view.relation().accept(this, context);
            }
            return null;
        }

        private boolean onChildSymbol(Symbol symbol) {
            if (symbol instanceof SelectSymbol selectSymbol) {
                AnalyzedRelation relation = selectSymbol.relation();
                relation.accept(this, null);
                if (foundMatch) {
                    return true;
                }
                relation.visitSymbols(this::onRootSymbol);
                if (foundMatch) {
                    return true;
                }
            } else if (symbol instanceof Function fn && fnIsMatch.test(fn.signature().getName())) {
                foundMatch = true;
                return true;
            }
            return false;
        }

        public void onRootSymbol(Symbol rootSymbol) {
            rootSymbol.any(this::onChildSymbol);
        }
    }
}
