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
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
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

        MetaData currentMetaData = currentState.getMetaData();
        MetaData.Builder newMetaData = MetaData.builder(currentMetaData);

        if (isPartitioned) {
            IndexTemplateMetaData indexTemplateMetaData = DDLClusterStateHelpers.templateMetaData(currentMetaData, source);
            if (indexTemplateMetaData == null) {
                throw new IndexTemplateMissingException("Template for partitioned table is missing");
            }
            renameTemplate(newMetaData, indexTemplateMetaData, target);
        }

        RoutingTable.Builder newRoutingTable = RoutingTable.builder(currentState.routingTable());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

        logger.info("renaming table '{}' to '{}'", source.fqn(), target.fqn());

        try {
            Index[] sourceIndices = indexNameExpressionResolver.concreteIndices(
                currentState, STRICT_INDICES_OPTIONS, source.indexNameOrAlias());

            for (Index sourceIndex : sourceIndices) {
                IndexMetaData sourceIndexMetaData = currentMetaData.getIndexSafe(sourceIndex);
                String sourceIndexName = sourceIndex.getName();
                newMetaData.remove(sourceIndexName);
                newRoutingTable.remove(sourceIndexName);
                blocksBuilder.removeIndexBlocks(sourceIndexName);

                IndexMetaData targetMd;
                if (isPartitioned) {
                    PartitionName partitionName = PartitionName.fromIndexOrTemplate(sourceIndexName);
                    String targetIndexName = IndexParts.toIndexName(target, partitionName.ident());
                    targetMd = IndexMetaData.builder(sourceIndexMetaData)
                        .removeAllAliases()
                        .putAlias(AliasMetaData.builder(target.indexNameOrAlias()).build())
                        .index(targetIndexName)
                        .build();
                } else {
                    targetMd = IndexMetaData.builder(sourceIndexMetaData)
                        .index(target.indexNameOrAlias())
                        .build();
                }
                newMetaData.put(targetMd, true);
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
            .metaData(newMetaData)
            .routingTable(newRoutingTable.build())
            .blocks(blocksBuilder)
            .build();

        return allocationService.reroute(
            ddlClusterStateService.onRenameTable(clusterStateAfterRename, source, target, request.isPartitioned()),
            "rename-table"
        );
    }

    private static void renameTemplate(MetaData.Builder newMetaData,
                                       IndexTemplateMetaData sourceTemplateMetaData,
                                       RelationName target) {
        IndexTemplateMetaData.Builder updatedTemplate = Templates.copyWithNewName(sourceTemplateMetaData, target);
        newMetaData
            .removeTemplate(sourceTemplateMetaData.getName())
            .put(updatedTemplate);
    }
}
