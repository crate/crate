/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata.upgrade;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;

import io.crate.metadata.IndexUUID;

/**
 * Utility class to upgrade or downgrade the cluster state while reading/writing from/to older nodes
 * in a mixed version cluster.
 */
public class ClusterStateUpgrader {

    private static final Logger LOGGER = LogManager.getLogger(ClusterStateUpgrader.class);

    /**
     * Upgrades the cluster state from an indexName based format, migrating metadata, blocks and routing table.
     */
    public static ClusterState upgrade(ClusterState clusterState,
                                       Version sourceVersion,
                                       MetadataUpgradeService metadataUpgradeService) {
        if (sourceVersion.onOrAfter(IndexUUID.INDICES_RESOLVED_BY_UUID_VERSION)) {
            // No upgrade needed, the cluster state is already in the correct format
            return clusterState;
        }
        LOGGER.debug("Received state from version {}, upgrading cluster state to the current format",
            sourceVersion);
        return ClusterState.builder(clusterState)
            .metadata(metadataUpgradeService.upgradeMetadata(clusterState.metadata()))
            .blocks(upgradeClusterBlocks(clusterState))
            .routingTable(upgradeRoutingTable(clusterState.routingTable()))
            .build();
    }

    /**
     * Downgrades the cluster state to an indexName based format, migrating metadata, blocks and routing table.
     */
    public static ClusterState downgrade(ClusterState state, Version targetVersion) {
        if (targetVersion.onOrAfter(IndexUUID.INDICES_RESOLVED_BY_UUID_VERSION)) {
            return state;
        }
        LOGGER.debug("Sending state to version {}, downgrading the cluster state to the required format",
            targetVersion);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (IndexRoutingTable indexRoutingTable : state.routingTable()) {
            routingTableBuilder.addWithIndexName(indexRoutingTable);
        }
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder();
        for (ClusterBlock block : state.blocks().global()) {
            blocksBuilder.addGlobalBlock(block);
        }
        for (final IndexMetadata indexMetadata : state.metadata()) {
            blocksBuilder.addBlocksWithIndexName(indexMetadata);
        }

        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
        for (IndexMetadata indexMetadata : state.metadata()) {
            metadataBuilder.remove(indexMetadata.getIndexUUID());
            metadataBuilder.putWithIndexName(indexMetadata);
        }

        return ClusterState.builder(state)
            .metadata(metadataBuilder.build())
            .blocks(blocksBuilder.build())
            .routingTable(routingTableBuilder.build())
            .build();
    }

    public static Diff<ClusterState> createDiff(ClusterState previousState,
                                                ClusterState newState,
                                                Version targetVersion) {
        if (targetVersion.onOrAfter(IndexUUID.INDICES_RESOLVED_BY_UUID_VERSION)) {
            return newState.diff(targetVersion, previousState);
        }
        // we need to downgrade both states before creating the diff
        ClusterState downgradedPrevious = downgrade(previousState, targetVersion);
        ClusterState downgradedCurrent = downgrade(newState, targetVersion);
        return downgradedCurrent.diff(targetVersion, downgradedPrevious);
    }

    public static ClusterState applyDiff(ClusterState currentState,
                                         Diff<ClusterState> diff,
                                         Version sourceVersion,
                                         MetadataUpgradeService metadataUpgradeService) {
        if (sourceVersion.onOrAfter(IndexUUID.INDICES_RESOLVED_BY_UUID_VERSION)) {
            return diff.apply(currentState);
        }
        // We must downgrade the last seen cluster state to be able to apply the diff
        ClusterState lastStateIn6Format = downgrade(currentState, sourceVersion);
        ClusterState tempState = diff.apply(lastStateIn6Format);
        return upgrade(tempState, sourceVersion, metadataUpgradeService);
    }

    /**
     * Re-build all cluster blocks using the related builder, this will migrate all blocks to the current format
     * (e.g. index blocks will be migrated to use the index UUID instead of the index name).
     */
    private static ClusterBlocks upgradeClusterBlocks(final ClusterState state) {
        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        for (ClusterBlock block : state.blocks().global()) {
            builder.addGlobalBlock(block);
        }
        for (final IndexMetadata indexMetadata : state.metadata()) {
            builder.addBlocks(indexMetadata);
        }
        return builder.build();
    }

    /**
     * Re-build the routing table using the related builder, this will migrate all index routing tables to the current format
     * (e.g. index routing tables will be migrated to use the index UUID instead of the index name).
     */
    private static RoutingTable upgradeRoutingTable(RoutingTable routingTable) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (IndexRoutingTable indexRoutingTable : routingTable) {
            routingTableBuilder.add(indexRoutingTable);
        }
        return routingTableBuilder.build();
    }

}
