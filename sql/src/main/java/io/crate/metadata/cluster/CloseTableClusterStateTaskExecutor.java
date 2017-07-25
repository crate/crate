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

import io.crate.executor.transport.ddl.OpenCloseTableOrPartitionRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.Set;

import static org.elasticsearch.cluster.metadata.MetaDataIndexStateService.INDEX_CLOSED_BLOCK;

public class CloseTableClusterStateTaskExecutor extends AbstractOpenCloseTableClusterStateTaskExecutor {

    public CloseTableClusterStateTaskExecutor(IndexNameExpressionResolver indexNameExpressionResolver,
                                       AllocationService allocationService,
                                       DDLClusterStateService ddlClusterStateService) {
        super(indexNameExpressionResolver, allocationService, ddlClusterStateService);
    }

    @Override
    protected IndexMetaData.State indexState() {
        return IndexMetaData.State.CLOSE;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, OpenCloseTableOrPartitionRequest request) {
        Context context = prepare(currentState, request);

        Set<IndexMetaData> indicesToClose = context.indicesMetaData();
        IndexTemplateMetaData templateMetaData = context.templateMetaData();

        if (indicesToClose.isEmpty() && templateMetaData == null) {
            return currentState;
        }

        // Check if index closing conflicts with any running restores
        RestoreService.checkIndexClosing(currentState, indicesToClose);
        // Check if index closing conflicts with any running snapshots
        SnapshotsService.checkIndexClosing(currentState, indicesToClose);

        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());
        for (IndexMetaData openIndexMetadata : indicesToClose) {
            final String indexName = openIndexMetadata.getIndex().getName();
            mdBuilder.put(IndexMetaData.builder(openIndexMetadata).state(IndexMetaData.State.CLOSE));
            blocksBuilder.addIndexBlock(indexName, INDEX_CLOSED_BLOCK);
        }

        // mark closed at possible partitioned table template
        if (templateMetaData != null) {
            mdBuilder.put(updateOpenCloseOnPartitionTemplate(templateMetaData, false));
        }

        // call possible registered modifiers
        if (context.partitionName() != null) {
            currentState = ddlClusterStateService.onCloseTablePartition(currentState, context.partitionName());
        } else {
            currentState = ddlClusterStateService.onCloseTable(currentState, request.tableIdent());
        }

        ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

        RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
        for (IndexMetaData index : indicesToClose) {
            rtBuilder.remove(index.getIndex().getName());
        }

        //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
        return allocationService.reroute(
            ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
            "indices closed " + indicesToClose);
    }
}
