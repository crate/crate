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

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_CLOSED_BLOCK;

import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;

import io.crate.execution.ddl.tables.OpenTableRequest;


public class CloseTableClusterStateTaskExecutor extends AbstractOpenCloseTableClusterStateTaskExecutor {

    public CloseTableClusterStateTaskExecutor(AllocationService allocationService,
                                              DDLClusterStateService ddlClusterStateService) {
        super(allocationService, ddlClusterStateService);
    }

    @Override
    protected IndexMetadata.State indexState() {
        return IndexMetadata.State.CLOSE;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, OpenTableRequest request) throws Exception {
        Context context = prepare(currentState, request);

        Set<Index> indicesToClose = context.indicesMetadata().stream()
            .map(IndexMetadata::getIndex)
            .collect(Collectors.toSet());
        IndexTemplateMetadata templateMetadata = context.templateMetadata();

        if (indicesToClose.isEmpty() && templateMetadata == null) {
            return currentState;
        }

        // Check if index closing conflicts with any running restores
        Set<Index> restoringIndices = RestoreService.restoringIndices(currentState, indicesToClose);
        if (restoringIndices.isEmpty() == false) {
            throw new IllegalArgumentException("Cannot close indices that are being restored: " + restoringIndices);
        }
        // Check if index closing conflicts with any running snapshots
        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, indicesToClose);
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException("Cannot close indices that are being snapshotted: " + snapshottingIndices +
                ". Try again after snapshot finishes or cancel the currently running snapshot.");
        }

        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());
        for (IndexMetadata openIndexMetadata : context.indicesMetadata()) {
            final String indexName = openIndexMetadata.getIndex().getName();
            mdBuilder.put(IndexMetadata.builder(openIndexMetadata).state(IndexMetadata.State.CLOSE));
            blocksBuilder.addIndexBlock(indexName, INDEX_CLOSED_BLOCK);
        }

        // mark closed at possible partitioned table template
        if (templateMetadata != null) {
            mdBuilder.put(updateOpenCloseOnPartitionTemplate(templateMetadata, false));
        }

        // The Metadata will always be overridden (and not merged!) when applying it on a cluster state builder.
        // So we must re-build the state with the latest modifications before we pass this state to possible modifiers.
        // Otherwise they would operate on the old Metadata and would just ignore any modifications.
        ClusterState updatedState = ClusterState.builder(currentState).metadata(mdBuilder).blocks(blocksBuilder).build();

        // call possible registered modifiers
        if (context.partitionName() != null) {
            updatedState = ddlClusterStateService.onCloseTablePartition(updatedState, context.partitionName());
        } else {
            updatedState = ddlClusterStateService.onCloseTable(updatedState, request.relation());
        }

        RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
        for (Index index : indicesToClose) {
            rtBuilder.remove(index.getName());
        }

        //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
        return allocationService.reroute(
            ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
            "indices closed " + indicesToClose);
    }
}
