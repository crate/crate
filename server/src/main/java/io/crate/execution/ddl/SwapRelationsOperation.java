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

import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.index.Index;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class SwapRelationsOperation {

    private final AllocationService allocationService;
    private final DDLClusterStateService ddlClusterStateService;
    private final IndexNameExpressionResolver indexNameResolver;

    public SwapRelationsOperation(AllocationService allocationService,
                                  DDLClusterStateService ddlClusterStateService,
                                  IndexNameExpressionResolver indexNameResolver) {
        this.allocationService = allocationService;
        this.ddlClusterStateService = ddlClusterStateService;
        this.indexNameResolver = indexNameResolver;
    }

    static class UpdatedState {
        ClusterState newState;
        Set<String> newIndices;

        UpdatedState(ClusterState newState, Set<String> newIndices) {
            this.newState = newState;
            this.newIndices = newIndices;
        }
    }

    public UpdatedState execute(ClusterState state, SwapRelationsRequest swapRelationsRequest) {
        UpdatedState stateAfterRename = applyRenameActions(state, swapRelationsRequest);
        List<RelationName> dropRelations = swapRelationsRequest.dropRelations();
        if (dropRelations.isEmpty()) {
            return stateAfterRename;
        } else {
            return applyDropRelations(state, stateAfterRename, dropRelations);
        }
    }

    private UpdatedState applyDropRelations(ClusterState state, UpdatedState updatedState, List<RelationName> dropRelations) {
        ClusterState stateAfterRename = updatedState.newState;
        Metadata.Builder updatedMetadata = Metadata.builder(stateAfterRename.metadata());
        RoutingTable.Builder routingBuilder = RoutingTable.builder(stateAfterRename.routingTable());

        for (RelationName dropRelation : dropRelations) {
            for (Index index : indexNameResolver.concreteIndices(
                state, IndicesOptions.LENIENT_EXPAND_OPEN, dropRelation.indexNameOrAlias())) {

                String indexName = index.getName();
                updatedMetadata.remove(indexName);
                routingBuilder.remove(indexName);
                updatedState.newIndices.remove(indexName);
            }
        }
        ClusterState stateAfterDropRelations = ClusterState
            .builder(stateAfterRename)
            .metadata(updatedMetadata)
            .routingTable(routingBuilder.build())
            .build();
        return new UpdatedState(
            allocationService.reroute(
                applyDropTableClusterStateModifiers(stateAfterDropRelations, dropRelations),
                "indices drop after name switch"
            ),
            updatedState.newIndices
        );
    }

    private ClusterState applyDropTableClusterStateModifiers(ClusterState stateAfterDropRelations,
                                                             List<RelationName> dropRelations) {
        ClusterState updatedState = stateAfterDropRelations;
        for (RelationName dropRelation : dropRelations) {
            updatedState = ddlClusterStateService.onDropTable(updatedState, dropRelation);
        }
        return updatedState;
    }

    private UpdatedState applyRenameActions(ClusterState state,
                                            SwapRelationsRequest swapRelationsRequest) {
        HashSet<String> newIndexNames = new HashSet<>();
        Metadata metadata = state.getMetadata();
        Metadata.Builder updatedMetadata = Metadata.builder(state.getMetadata());
        RoutingTable.Builder routingBuilder = RoutingTable.builder(state.routingTable());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(state.blocks());

        // Remove all involved indices first so that rename operations are independent of each other
        for (RelationNameSwap swapAction : swapRelationsRequest.swapActions()) {
            removeOccurrences(state, blocksBuilder, routingBuilder, updatedMetadata, swapAction.source());
            removeOccurrences(state, blocksBuilder, routingBuilder, updatedMetadata, swapAction.target());
        }
        for (RelationNameSwap relationNameSwap : swapRelationsRequest.swapActions()) {
            RelationName source = relationNameSwap.source();
            RelationName target = relationNameSwap.target();
            addSourceIndicesRenamedToTargetName(
                state, metadata, updatedMetadata, blocksBuilder, routingBuilder, source, target, newIndexNames::add);
            addSourceIndicesRenamedToTargetName(
                state, metadata, updatedMetadata, blocksBuilder, routingBuilder, target, source, newIndexNames::add);
        }
        ClusterState stateAfterSwap = ClusterState.builder(state)
            .metadata(updatedMetadata)
            .routingTable(routingBuilder.build())
            .blocks(blocksBuilder)
            .build();
        ClusterState reroutedState = allocationService.reroute(
            applyClusterStateModifiers(stateAfterSwap, swapRelationsRequest.swapActions()),
            "indices name switch"
        );
        return new UpdatedState(reroutedState, newIndexNames);
    }

    private ClusterState applyClusterStateModifiers(ClusterState newState, List<RelationNameSwap> swapActions) {
        ClusterState updatedState = newState;
        for (RelationNameSwap swapAction : swapActions) {
            updatedState = ddlClusterStateService.onSwapRelations(updatedState, swapAction.source(), swapAction.target());
        }
        return updatedState;
    }

    private void removeOccurrences(ClusterState state,
                                   ClusterBlocks.Builder blocksBuilder,
                                   RoutingTable.Builder routingBuilder,
                                   Metadata.Builder updatedMetadata,
                                   RelationName name) {
        String aliasOrIndexName = name.indexNameOrAlias();
        String templateName = PartitionName.templateName(name.schema(), name.name());
        for (Index index : indexNameResolver.concreteIndices(
            state, IndicesOptions.LENIENT_EXPAND_OPEN, aliasOrIndexName)) {

            String indexName = index.getName();
            routingBuilder.remove(indexName);
            updatedMetadata.remove(indexName);
            blocksBuilder.removeIndexBlocks(indexName);
        }
        updatedMetadata.removeTemplate(templateName);
    }

    private void addSourceIndicesRenamedToTargetName(ClusterState state,
                                                     Metadata metadata,
                                                     Metadata.Builder updatedMetadata,
                                                     ClusterBlocks.Builder blocksBuilder,
                                                     RoutingTable.Builder routingBuilder,
                                                     RelationName source,
                                                     RelationName target,
                                                     Consumer<String> onProcessedIndex) {
        String sourceTemplateName = PartitionName.templateName(source.schema(), source.name());
        IndexTemplateMetadata sourceTemplate = metadata.templates().get(sourceTemplateName);

        for (Index sourceIndex : indexNameResolver.concreteIndices(
            state, IndicesOptions.LENIENT_EXPAND_OPEN, source.indexNameOrAlias())) {

            String sourceIndexName = sourceIndex.getName();
            IndexMetadata sourceMd = metadata.getIndexSafe(sourceIndex);
            IndexMetadata targetMd;
            if (sourceTemplate == null) {
                targetMd = IndexMetadata.builder(sourceMd)
                    .removeAllAliases()
                    .index(target.indexNameOrAlias())
                    .build();
                onProcessedIndex.accept(target.indexNameOrAlias());
            } else {
                PartitionName partitionName = PartitionName.fromIndexOrTemplate(sourceIndexName);
                String targetIndexName = IndexParts.toIndexName(target, partitionName.ident());
                targetMd = IndexMetadata.builder(sourceMd)
                    .removeAllAliases()
                    .putAlias(AliasMetadata.builder(target.indexNameOrAlias()).build())
                    .index(targetIndexName)
                    .build();
                onProcessedIndex.accept(targetIndexName);
            }
            updatedMetadata.put(targetMd, true);
            blocksBuilder.addBlocks(targetMd);
            routingBuilder.addAsFromCloseToOpen(targetMd);
        }
        if (sourceTemplate != null) {
            IndexTemplateMetadata.Builder templateBuilder = Templates.copyWithNewName(sourceTemplate, target);
            updatedMetadata.put(templateBuilder);
        }
    }
}
