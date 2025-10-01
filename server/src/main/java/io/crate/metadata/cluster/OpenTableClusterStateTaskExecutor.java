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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;

import io.crate.execution.ddl.tables.OpenTableRequest;
import io.crate.execution.ddl.tables.TransportCloseTable;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;


public class OpenTableClusterStateTaskExecutor extends DDLClusterStateTaskExecutor<OpenTableRequest> {

    private record Context(List<IndexMetadata> closedIndices, PartitionName partitionName) {
    }

    private final AllocationService allocationService;
    private final DDLClusterStateService ddlClusterStateService;
    private final MetadataUpgradeService metadataIndexUpgradeService;
    private final IndicesService indicesService;

    public OpenTableClusterStateTaskExecutor(AllocationService allocationService,
                                             DDLClusterStateService ddlClusterStateService,
                                             MetadataUpgradeService metadataIndexUpgradeService,
                                             IndicesService indexServices) {
        this.allocationService = allocationService;
        this.ddlClusterStateService = ddlClusterStateService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        this.indicesService = indexServices;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, OpenTableRequest request) throws Exception {
        Context context = prepare(currentState, request);
        List<IndexMetadata> closedIndices = context.closedIndices();
        RelationMetadata.Table table = currentState.metadata().getRelation(request.relation());
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        if (request.partitionValues().isEmpty()) {
            mdBuilder.setTable(
                table.name(),
                table.columns(),
                table.settings(),
                table.routingColumn(),
                table.columnPolicy(),
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                State.OPEN,
                table.indexUUIDs(),
                table.tableVersion() + 1
            );
        } else if (closedIndices.isEmpty()) {
            return currentState;
        }
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());
        final Version minIndexCompatibilityVersion = currentState.nodes().getMaxNodeVersion()
            .minimumIndexCompatibilityVersion();
        for (IndexMetadata closedMetadata : closedIndices) {
            final String indexUUID = closedMetadata.getIndex().getUUID();
            blocksBuilder.removeIndexBlockWithId(indexUUID, TransportCloseTable.INDEX_CLOSED_BLOCK_ID);

            if (closedMetadata.getState() == IndexMetadata.State.OPEN) {
                continue;
            }
            final Settings.Builder updatedSettings = Settings.builder().put(closedMetadata.getSettings());
            updatedSettings.remove(IndexMetadata.VERIFIED_BEFORE_CLOSE_SETTING.getKey());

            IndexMetadata updatedIndexMetadata = IndexMetadata.builder(closedMetadata)
                .state(IndexMetadata.State.OPEN)
                .settingsVersion(closedMetadata.getSettingsVersion() + 1)
                .settings(updatedSettings)
                .build();

            // The index might be closed because we couldn't import it due to old incompatible version
            // We need to check that this index can be upgraded to the current version
            updatedIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(updatedIndexMetadata, null, minIndexCompatibilityVersion);
            try {
                indicesService.verifyIndexMetadata(updatedIndexMetadata, updatedIndexMetadata);
            } catch (Exception e) {
                throw new ElasticsearchException("Failed to verify index " + indexUUID, e);
            }

            mdBuilder.put(updatedIndexMetadata, true);
        }

        // The Metadata will always be overridden (and not merged!) when applying it on a cluster state builder.
        // So we must re-build the state with the latest modifications before we pass this state to possible modifiers.
        // Otherwise they would operate on the old Metadata and would just ignore any modifications.
        ClusterState updatedState = ClusterState.builder(currentState).metadata(mdBuilder).blocks(blocksBuilder).build();

        // call possible registered modifiers
        if (context.partitionName() != null) {
            updatedState = ddlClusterStateService.onOpenTablePartition(updatedState, context.partitionName());
        } else {
            updatedState = ddlClusterStateService.onOpenTable(updatedState, request.relation());
        }

        RoutingTable.Builder rtBuilder = RoutingTable.builder(updatedState.routingTable());
        for (IndexMetadata index : closedIndices) {
            rtBuilder.addAsFromCloseToOpen(updatedState.metadata().getIndexSafe(index.getIndex()));
        }

        //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
        return allocationService.reroute(
            ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
            "indices opened " + closedIndices);
    }

    private Context prepare(ClusterState currentState, OpenTableRequest request) {
        RelationName relationName = request.relation();
        List<String> partitionValues = request.partitionValues();
        Metadata metadata = currentState.metadata();
        List<IndexMetadata> closedIndices = metadata.getIndices(
            relationName,
            partitionValues,
            false,
            idx -> idx.getState() == State.CLOSE ? idx : null
        );
        PartitionName partitionName = partitionValues.isEmpty() ? null : new PartitionName(relationName, partitionValues);
        return new Context(closedIndices, partitionName);
    }
}
