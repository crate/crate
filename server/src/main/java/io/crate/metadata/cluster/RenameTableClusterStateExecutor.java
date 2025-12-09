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

import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;

import io.crate.common.collections.Lists;
import io.crate.exceptions.RelationUnknown;
import io.crate.execution.ddl.tables.RenameTableRequest;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.view.ViewsMetadata;

public class RenameTableClusterStateExecutor {

    private final Logger logger;
    private final DDLClusterStateService ddlClusterStateService;
    private final AllocationService allocationService;

    public RenameTableClusterStateExecutor(AllocationService allocationService,
                                           DDLClusterStateService ddlClusterStateService) {
        this.allocationService = allocationService;
        logger = LogManager.getLogger(getClass());
        this.ddlClusterStateService = ddlClusterStateService;
    }

    public ClusterState execute(ClusterState currentState, RenameTableRequest request) throws Exception {
        RelationName source = request.sourceName();
        RelationName target = request.targetName();

        Metadata currentMetadata = currentState.metadata();
        Metadata.Builder newMetadata = Metadata.builder(currentMetadata);
        ViewsMetadata views = currentMetadata.custom(ViewsMetadata.TYPE);
        boolean isView = views != null && views.contains(source);

        boolean viewExists = views != null && views.contains(target);
        boolean tableExists = currentMetadata.getRelation(target) != null;
        if (viewExists || tableExists) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot rename %s %s to %s, %s %s already exists",
                isView ? "view" : "table", source, target, viewExists ? "view" : "table", target));
        }

        if (isView) {
            ViewsMetadata updatedViewsMetadata = views.rename(source, target);
            newMetadata.putCustom(ViewsMetadata.TYPE, updatedViewsMetadata);
            return ClusterState.builder(currentState)
                .metadata(newMetadata)
                .build();
        }

        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

        logger.info("renaming table '{}' to '{}'", source.fqn(), target.fqn());

        RelationMetadata.Table table = currentMetadata.getRelation(source);
        if (table == null) {
            throw new RelationUnknown(source);
        }
        newMetadata
            .dropRelation(source)
            .setTable(
                target,
                Lists.map(
                    table.columns(),
                    ref -> ref.withRelation(target)
                ),
                table.settings(),
                table.routingColumn(),
                table.columnPolicy(),
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                table.state(),
                table.indexUUIDs(),
                table.tableVersion() + 1
            );

        boolean needIndexAndRoutingUpdate = currentState.nodes().getSmallestNonClientNodeVersion().before(Version.V_6_1_0);
        RoutingTable.Builder newRoutingTable = null;
        if (needIndexAndRoutingUpdate) {
            newRoutingTable = RoutingTable.builder(currentState.routingTable());
            List<IndexMetadata> sourceIndices = currentState.metadata().getIndices(source, List.of(), true, x -> x);
            for (IndexMetadata sourceIndex : sourceIndices) {
                String sourceIndexUUID = sourceIndex.getIndexUUID();
                String targetIndexName;
                if (sourceIndex.partitionValues().isEmpty()) {
                    targetIndexName = target.indexNameOrAlias();
                } else {
                    PartitionName newPartitionName = new PartitionName(target, sourceIndex.partitionValues());
                    targetIndexName = newPartitionName.asIndexName();
                }

                newMetadata.remove(sourceIndexUUID);
                newRoutingTable.remove(sourceIndexUUID);
                blocksBuilder.removeIndexBlocks(sourceIndexUUID);

                IndexMetadata targetMd = IndexMetadata.builder(sourceIndex)
                    .indexName(targetIndexName)
                    .build();
                newMetadata.put(targetMd, true);
                newRoutingTable.addAsFromCloseToOpen(targetMd);
                blocksBuilder.addBlocks(targetMd);
            }
        }

        ClusterState clusterStateAfterRename = ClusterState.builder(currentState)
            .metadata(newMetadata)
            .routingTable(needIndexAndRoutingUpdate ? newRoutingTable.build() : currentState.routingTable())
            .blocks(blocksBuilder)
            .build();

        return allocationService.reroute(
            ddlClusterStateService.onRenameTable(clusterStateAfterRename, source, target, request.isPartitioned()),
            "rename-table"
        );
    }
}
