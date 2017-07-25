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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.indices.IndicesService;

import java.util.Set;

import static org.elasticsearch.cluster.metadata.MetaDataIndexStateService.INDEX_CLOSED_BLOCK;

public class OpenTableClusterStateTaskExecutor extends AbstractOpenCloseTableClusterStateTaskExecutor {

    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;
    private final IndicesService indicesService;


    public OpenTableClusterStateTaskExecutor(IndexNameExpressionResolver indexNameExpressionResolver,
                                      AllocationService allocationService,
                                      DDLClusterStateService ddlClusterStateService,
                                      MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                                      IndicesService indexServices) {
        super(indexNameExpressionResolver, allocationService, ddlClusterStateService);
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
        this.indicesService = indexServices;
    }

    @Override
    protected IndexMetaData.State indexState() {
        return IndexMetaData.State.OPEN;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, OpenCloseTableOrPartitionRequest request) {
        Context context = prepare(currentState, request);
        Set<IndexMetaData> indicesToOpen = context.indicesMetaData();
        IndexTemplateMetaData templateMetaData = context.templateMetaData();

        if (indicesToOpen.isEmpty() && templateMetaData == null) {
            return currentState;
        }

        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());
        final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion()
            .minimumIndexCompatibilityVersion();
        for (IndexMetaData closedMetaData : indicesToOpen) {
            final String indexName = closedMetaData.getIndex().getName();
            IndexMetaData indexMetaData = IndexMetaData.builder(closedMetaData).state(IndexMetaData.State.OPEN).build();
            // The index might be closed because we couldn't import it due to old incompatible version
            // We need to check that this index can be upgraded to the current version
            indexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData, minIndexCompatibilityVersion);
            try {
                indicesService.verifyIndexMetadata(indexMetaData, indexMetaData);
            } catch (Exception e) {
                throw new ElasticsearchException("Failed to verify index " + indexMetaData.getIndex(), e);
            }

            mdBuilder.put(indexMetaData, true);
            blocksBuilder.removeIndexBlock(indexName, INDEX_CLOSED_BLOCK);
        }

        // remove closed flag at possible partitioned table template
        if (templateMetaData != null) {
            mdBuilder.put(updateOpenCloseOnPartitionTemplate(templateMetaData, true));
        }

        // call possible registered modifiers
        if (context.partitionName() != null) {
            currentState = ddlClusterStateService.onOpenTablePartition(currentState, context.partitionName());
        } else {
            currentState = ddlClusterStateService.onOpenTable(currentState, request.tableIdent());
        }

        ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

        RoutingTable.Builder rtBuilder = RoutingTable.builder(updatedState.routingTable());
        for (IndexMetaData index : indicesToOpen) {
            rtBuilder.addAsFromCloseToOpen(updatedState.metaData().getIndexSafe(index.getIndex()));
        }

        //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
        return allocationService.reroute(
            ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
            "indices opened " + indicesToOpen);
    }
}
