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

package io.crate.execution.ddl;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.metadata.SchemaMetadata;
import org.elasticsearch.index.Index;

import io.crate.common.collections.Lists;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;

public class SwapRelationsOperation {

    private final DDLClusterStateService ddlClusterStateService;
    private final MetadataDeleteIndexService deleteIndexService;

    public SwapRelationsOperation(MetadataDeleteIndexService deleteIndexService,
                                  DDLClusterStateService ddlClusterStateService) {
        this.deleteIndexService = deleteIndexService;
        this.ddlClusterStateService = ddlClusterStateService;
    }

    public ClusterState execute(ClusterState state, SwapRelationsRequest swapRelationsRequest) {
        Metadata metadata = state.metadata();
        Metadata.Builder builder = new Metadata.Builder(metadata);
        List<RelationNameSwap> swapActions = swapRelationsRequest.swapActions();
        List<RelationName> dropRelations = swapRelationsRequest.dropRelations();
        for (RelationNameSwap nameSwap : swapActions) {
            RelationName source = nameSwap.source();
            RelationName target = nameSwap.target();

            SchemaMetadata sourceSchema = metadata.schemas().get(source.schema());
            SchemaMetadata targetSchema = metadata.schemas().get(target.schema());

            RelationMetadata.Table sourceRelation = (RelationMetadata.Table) sourceSchema.get(source);
            RelationMetadata.Table targetRelation = (RelationMetadata.Table) targetSchema.get(target);

            builder
                .dropTable(source)
                .dropTable(target)
                .addTable(
                    target,
                    Lists.map(
                        sourceRelation.columns(),
                        ref -> ref.withReferenceIdent(new ReferenceIdent(target, ref.column()))
                    ),
                    sourceRelation.settings(),
                    sourceRelation.routingColumn(),
                    sourceRelation.columnPolicy(),
                    sourceRelation.pkConstraintName(),
                    sourceRelation.checkConstraints(),
                    sourceRelation.primaryKeys(),
                    sourceRelation.partitionedBy(),
                    sourceRelation.state(),
                    sourceRelation.indexUUIDs()
                )
                .addTable(
                    source,
                    Lists.map(
                        targetRelation.columns(),
                        ref -> ref.withReferenceIdent(new ReferenceIdent(source, ref.column()))
                    ),
                    targetRelation.settings(),
                    targetRelation.routingColumn(),
                    targetRelation.columnPolicy(),
                    targetRelation.pkConstraintName(),
                    targetRelation.checkConstraints(),
                    targetRelation.primaryKeys(),
                    targetRelation.partitionedBy(),
                    targetRelation.state(),
                    targetRelation.indexUUIDs()
                );
        }
        List<Index> toDelete = new ArrayList<>();
        for (RelationName relation : dropRelations) {
            RelationMetadata relationMetadata = builder.getRelation(relation);
            if (relationMetadata instanceof RelationMetadata.Table table) {
                for (String indexUUID : table.indexUUIDs()) {
                    IndexMetadata indexMetadata = metadata.indexByUUID(indexUUID);
                    if (indexMetadata != null) {
                        toDelete.add(indexMetadata.getIndex());
                    }
                }
            }
            builder.dropTable(relation);
        }
        ClusterState updatedState = ClusterState.builder(state).metadata(builder).build();
        updatedState = deleteIndexService.deleteIndices(updatedState, toDelete);
        for (RelationNameSwap swapAction : swapActions) {
            updatedState = ddlClusterStateService.onSwapRelations(
                updatedState,
                swapAction.source(),
                swapAction.target()
            );
        }
        for (RelationName relation : dropRelations) {
            updatedState = ddlClusterStateService.onDropTable(updatedState, relation);
        }
        return updatedState;
    }
}
