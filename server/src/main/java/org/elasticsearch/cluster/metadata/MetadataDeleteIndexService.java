/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;

/**
 * Deletes indices.
 */
public class MetadataDeleteIndexService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataDeleteIndexService.class);

    private final Settings settings;
    private final AllocationService allocationService;

    @Inject
    public MetadataDeleteIndexService(Settings settings,
                                      AllocationService allocationService) {
        this.settings = settings;
        this.allocationService = allocationService;
    }

    /**
     * Delete some indices from the cluster state.
     */
    public ClusterState deleteIndices(ClusterState currentState, Collection<Index> indices) {
        return deleteIndices(currentState, settings, allocationService, indices);
    }

    /**
     * Delete some indices from the cluster state.
     */
    public static ClusterState deleteIndices(ClusterState currentState,
                                             Settings settings,
                                             AllocationService allocationService,
                                             Collection<Index> indices) {
        final Metadata meta = currentState.metadata();
        final Set<Index> indicesToDelete = indices.stream().map(i -> meta.getIndexSafe(i).getIndex()).collect(toSet());

        // Check if index deletion conflicts with any running snapshots
        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, indicesToDelete);
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException("Cannot delete indices that are being snapshotted: " + snapshottingIndices +
                ". Try again after snapshot finishes or cancel the currently running snapshot.");
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(meta);
        ClusterBlocks.Builder clusterBlocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

        final IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metadataBuilder.indexGraveyard());
        final int previousGraveyardSize = graveyardBuilder.tombstones().size();
        Map<RelationMetadata.Table, List<String>> relationIndexUUIDs = new HashMap<>();
        for (final Index index : indicesToDelete) {
            String indexUUID = index.getUUID();
            LOGGER.info("{} deleting index", index);
            routingTableBuilder.remove(indexUUID);
            clusterBlocksBuilder.removeIndexBlocks(indexUUID);
            metadataBuilder.remove(indexUUID);

            RelationMetadata relation = meta.getRelation(indexUUID);
            if (relation instanceof RelationMetadata.Table table) {
                List<String> indexUUIDs = relationIndexUUIDs.computeIfAbsent(table, k -> new ArrayList<>());
                indexUUIDs.add(index.getUUID());
            } else {
                LOGGER.debug("No table relation found for index [{}] while deleting it", index);
            }
        }
        for (var entry : relationIndexUUIDs.entrySet()) {
            RelationMetadata.Table table = entry.getKey();
            List<String> indexUUIDs = entry.getValue();
            List<String> newIndexUUIDs = table.indexUUIDs().stream()
                .filter(x -> !indexUUIDs.contains(x))
                .toList();
            metadataBuilder.setTable(
                table.name(),
                table.columns(),
                table.settings(),
                table.routingColumn(),
                table.columnPolicy(),
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                table.state(),
                newIndexUUIDs,
                table.tableVersion() + 1
            );
        }

        // add tombstones to the cluster state for each deleted index
        final IndexGraveyard currentGraveyard = graveyardBuilder.addTombstones(indicesToDelete).build(settings);
        metadataBuilder.indexGraveyard(currentGraveyard); // the new graveyard set on the metadata
        LOGGER.trace("{} tombstones purged from the cluster state. Previous tombstone size: {}. Current tombstone size: {}.",
            graveyardBuilder.getNumPurged(), previousGraveyardSize, currentGraveyard.getTombstones().size());

        Metadata newMetadata = metadataBuilder.build();
        ClusterBlocks blocks = clusterBlocksBuilder.build();

        // update snapshot restore entries
        ImmutableOpenMap<String, ClusterState.Custom> customs = currentState.customs();
        final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
        RestoreInProgress updatedRestoreInProgress = RestoreService.updateRestoreStateWithDeletedIndices(restoreInProgress, indicesToDelete);
        if (updatedRestoreInProgress != restoreInProgress) {
            ImmutableOpenMap.Builder<String, ClusterState.Custom> builder = ImmutableOpenMap.builder(customs);
            builder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
            customs = builder.build();
        }

        return allocationService.reroute(
                ClusterState.builder(currentState)
                    .routingTable(routingTableBuilder.build())
                    .metadata(newMetadata)
                    .blocks(blocks)
                    .customs(customs)
                    .build(),
                "deleted indices [" + indicesToDelete + "]");
    }
}
