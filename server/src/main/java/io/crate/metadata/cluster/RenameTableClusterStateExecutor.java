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

package io.crate.metadata.cluster;

import io.crate.execution.ddl.Templates;
import io.crate.execution.ddl.tables.RenameTableRequest;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexTemplateMissingException;

public class RenameTableClusterStateExecutor {

    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);

    private final Logger logger;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final DDLClusterStateService ddlClusterStateService;
    private final AllocationService allocationService;

    public RenameTableClusterStateExecutor(IndexNameExpressionResolver indexNameExpressionResolver,
                                           AllocationService allocationService,
                                           DDLClusterStateService ddlClusterStateService) {
        this.allocationService = allocationService;
        logger = LogManager.getLogger(getClass());
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.ddlClusterStateService = ddlClusterStateService;
    }

    public ClusterState execute(ClusterState currentState, RenameTableRequest request) throws Exception {
        RelationName source = request.sourceTableIdent();
        RelationName target = request.targetTableIdent();
        boolean isPartitioned = request.isPartitioned();

        Metadata currentMetadata = currentState.getMetadata();
        Metadata.Builder newMetadata = Metadata.builder(currentMetadata);

        if (isPartitioned) {
            IndexTemplateMetadata indexTemplateMetadata = DDLClusterStateHelpers.templateMetadata(currentMetadata, source);
            if (indexTemplateMetadata == null) {
                throw new IndexTemplateMissingException("Template for partitioned table is missing");
            }
            renameTemplate(newMetadata, indexTemplateMetadata, target);
        }

        RoutingTable.Builder newRoutingTable = RoutingTable.builder(currentState.routingTable());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

        logger.info("renaming table '{}' to '{}'", source.fqn(), target.fqn());

        try {
            Index[] sourceIndices = indexNameExpressionResolver.concreteIndices(
                currentState, STRICT_INDICES_OPTIONS, source.indexNameOrAlias());

            for (Index sourceIndex : sourceIndices) {
                IndexMetadata sourceIndexMetadata = currentMetadata.getIndexSafe(sourceIndex);
                String sourceIndexName = sourceIndex.getName();
                newMetadata.remove(sourceIndexName);
                newRoutingTable.remove(sourceIndexName);
                blocksBuilder.removeIndexBlocks(sourceIndexName);

                IndexMetadata targetMd;
                if (isPartitioned) {
                    PartitionName partitionName = PartitionName.fromIndexOrTemplate(sourceIndexName);
                    String targetIndexName = IndexParts.toIndexName(target, partitionName.ident());
                    targetMd = IndexMetadata.builder(sourceIndexMetadata)
                        .removeAllAliases()
                        .putAlias(AliasMetadata.builder(target.indexNameOrAlias()).build())
                        .index(targetIndexName)
                        .build();
                } else {
                    targetMd = IndexMetadata.builder(sourceIndexMetadata)
                        .index(target.indexNameOrAlias())
                        .build();
                }
                newMetadata.put(targetMd, true);
                newRoutingTable.addAsFromCloseToOpen(targetMd);
                blocksBuilder.addBlocks(targetMd);
            }
        } catch (IndexNotFoundException e) {
            if (isPartitioned == false) {
                throw e;
            }
            // empty partition case, no indices, just a template exists
        }

        ClusterState clusterStateAfterRename = ClusterState.builder(currentState)
            .metadata(newMetadata)
            .routingTable(newRoutingTable.build())
            .blocks(blocksBuilder)
            .build();

        return allocationService.reroute(
            ddlClusterStateService.onRenameTable(clusterStateAfterRename, source, target, request.isPartitioned()),
            "rename-table"
        );
    }

    private static void renameTemplate(Metadata.Builder newMetadata,
                                       IndexTemplateMetadata sourceTemplateMetadata,
                                       RelationName target) {
        IndexTemplateMetadata.Builder updatedTemplate = Templates.copyWithNewName(sourceTemplateMetadata, target);
        newMetadata
            .removeTemplate(sourceTemplateMetadata.getName())
            .put(updatedTemplate);
    }
}
