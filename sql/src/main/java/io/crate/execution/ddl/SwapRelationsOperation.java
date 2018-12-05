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

package io.crate.execution.ddl;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class SwapRelationsOperation {

    private final AllocationService allocationService;
    private final IndexNameExpressionResolver indexNameResolver;

    public SwapRelationsOperation(AllocationService allocationService,
                                  IndexNameExpressionResolver indexNameResolver) {
        this.allocationService = allocationService;
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
        MetaData.Builder updatedMetaData = MetaData.builder(stateAfterRename.metaData());
        RoutingTable.Builder routingBuilder = RoutingTable.builder(stateAfterRename.routingTable());

        for (RelationName dropRelation : dropRelations) {
            for (Index index : indexNameResolver.concreteIndices(
                state, IndicesOptions.LENIENT_EXPAND_OPEN, dropRelation.indexNameOrAlias())) {
                updatedMetaData.remove(index.getName());
                routingBuilder.remove(index.getName());
                updatedState.newIndices.remove(index.getName());
            }
        }
        ClusterState stateAfterDropRelations = allocationService.reroute(
            ClusterState
                .builder(stateAfterRename)
                .metaData(updatedMetaData)
                .routingTable(routingBuilder.build())
                .build(),
            "indices drop after name switch"
        );
        return new UpdatedState(stateAfterDropRelations, updatedState.newIndices);
    }

    private UpdatedState applyRenameActions(ClusterState state,
                                            SwapRelationsRequest swapRelationsRequest) {
        HashSet<String> newIndexNames = new HashSet<>();
        MetaData metaData = state.getMetaData();
        MetaData.Builder updatedMetaData = MetaData.builder(state.getMetaData());
        RoutingTable.Builder routingBuilder = RoutingTable.builder(state.routingTable());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(state.blocks());

        // Remove all involved indices first so that rename operations are independent of each other
        for (RelationNameSwap swapAction : swapRelationsRequest.swapActions()) {
            removeOccurrences(state, blocksBuilder, routingBuilder, updatedMetaData, swapAction.source());
            removeOccurrences(state, blocksBuilder, routingBuilder, updatedMetaData, swapAction.target());
        }
        for (RelationNameSwap relationNameSwap : swapRelationsRequest.swapActions()) {
            RelationName source = relationNameSwap.source();
            RelationName target = relationNameSwap.target();
            addSourceIndicesRenamedToTargetName(
                state, metaData, updatedMetaData, blocksBuilder, routingBuilder, source, target, newIndexNames::add);
            addSourceIndicesRenamedToTargetName(
                state, metaData, updatedMetaData, blocksBuilder, routingBuilder, target, source, newIndexNames::add);
        }
        ClusterState newState = allocationService.reroute(
            ClusterState.builder(state)
                .metaData(updatedMetaData)
                .routingTable(routingBuilder.build())
                .blocks(blocksBuilder)
                .build(),
            "indices name switch"
        );
        return new UpdatedState(newState, newIndexNames);
    }

    private void removeOccurrences(ClusterState state,
                                   ClusterBlocks.Builder blocksBuilder,
                                   RoutingTable.Builder routingBuilder,
                                   MetaData.Builder updatedMetaData,
                                   RelationName name) {
        String aliasOrIndexName = name.indexNameOrAlias();
        String templateName = PartitionName.templateName(name.schema(), name.name());
        for (Index index : indexNameResolver.concreteIndices(
            state, IndicesOptions.LENIENT_EXPAND_OPEN, aliasOrIndexName)) {

            String indexName = index.getName();
            routingBuilder.remove(indexName);
            updatedMetaData.remove(indexName);
            blocksBuilder.removeIndexBlocks(indexName);
        }
        updatedMetaData.removeTemplate(templateName);
    }

    private void addSourceIndicesRenamedToTargetName(ClusterState state,
                                                     MetaData metaData,
                                                     MetaData.Builder updatedMetaData,
                                                     ClusterBlocks.Builder blocksBuilder,
                                                     RoutingTable.Builder routingBuilder,
                                                     RelationName source,
                                                     RelationName target,
                                                     Consumer<String> onProcessedIndex) {
        String sourceTemplateName = PartitionName.templateName(source.schema(), source.name());
        IndexTemplateMetaData sourceTemplate = metaData.templates().get(sourceTemplateName);

        for (Index sourceIndex : indexNameResolver.concreteIndices(
            state, IndicesOptions.LENIENT_EXPAND_OPEN, source.indexNameOrAlias())) {

            String sourceIndexName = sourceIndex.getName();
            IndexMetaData sourceMd = metaData.getIndexSafe(sourceIndex);
            IndexMetaData targetMd;
            if (sourceTemplate == null) {
                targetMd = IndexMetaData.builder(sourceMd)
                    .removeAllAliases()
                    .index(target.indexNameOrAlias())
                    .build();
                onProcessedIndex.accept(target.indexNameOrAlias());
            } else {
                PartitionName partitionName = PartitionName.fromIndexOrTemplate(sourceIndexName);
                String targetIndexName = IndexParts.toIndexName(target, partitionName.ident());
                targetMd = IndexMetaData.builder(sourceMd)
                    .removeAllAliases()
                    .putAlias(AliasMetaData.builder(target.indexNameOrAlias()).build())
                    .index(targetIndexName)
                    .build();
                onProcessedIndex.accept(targetIndexName);
            }
            updatedMetaData.put(targetMd, true);
            blocksBuilder.addBlocks(targetMd);
            routingBuilder.addAsFromCloseToOpen(targetMd);
        }
        if (sourceTemplate != null) {
            String targetTemplateName = PartitionName.templateName(target.schema(), target.name());
            String targetAlias = target.indexNameOrAlias();
            IndexTemplateMetaData.Builder templateBuilder = IndexTemplateMetaData
                .builder(targetTemplateName)
                .patterns(Collections.singletonList(PartitionName.templatePrefix(target.schema(), target.name())))
                .settings(sourceTemplate.settings())
                .order(sourceTemplate.order())
                .putAlias(AliasMetaData.builder(targetAlias).build())
                .version(sourceTemplate.version());

            for (ObjectObjectCursor<String, CompressedXContent> mapping : sourceTemplate.mappings()) {
                try {
                    templateBuilder.putMapping(mapping.key, mapping.value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            updatedMetaData.put(templateBuilder);
        }
    }
}
