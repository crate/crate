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

import io.crate.execution.ddl.index.BulkRenameIndexRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;

import java.util.List;

public class RenameIndexClusterStateExecutor extends DDLClusterStateTaskExecutor<BulkRenameIndexRequest> {

    private final Logger logger;
    private final AllocationService allocationService;

    public RenameIndexClusterStateExecutor(AllocationService allocationService) {
        logger = LogManager.getLogger(getClass());
        this.allocationService = allocationService;
    }

    @Override
    public ClusterState execute(ClusterState currentState, BulkRenameIndexRequest request) throws Exception {

        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

        MetaData metaData = currentState.getMetaData();
        MetaData.Builder mdBuilder = MetaData.builder(metaData);
        RoutingTable.Builder routingBuilder = RoutingTable.builder(currentState.routingTable());

        renameIndices(metaData, mdBuilder, routingBuilder, blocksBuilder, request.renameIndexActions());
        return allocationService.reroute(
            ClusterState.builder(currentState)
                .metaData(mdBuilder)
                .routingTable(routingBuilder.build())
                .blocks(blocksBuilder)
                .build(),
            "indices bulk rename"
        );
    }

    void renameIndices(MetaData metaData,
                       MetaData.Builder metaDataBuilder,
                       RoutingTable.Builder routingBuilder,
                       ClusterBlocks.Builder blocksBuilder,
                       List<BulkRenameIndexRequest.RenameIndexAction> renameIndexActions) {
        for (BulkRenameIndexRequest.RenameIndexAction action : renameIndexActions) {
            String sourceIndex = action.sourceIndexName();
            String targetIndex = action.targetIndexName();
            logger.info("rename index '{}' to '{}'", sourceIndex, targetIndex);
            IndexMetaData indexMetaData = metaData.index(sourceIndex);
            IndexMetaData targetIndexMetadata = IndexMetaData.builder(indexMetaData).index(targetIndex).build();
            metaDataBuilder.remove(sourceIndex);
            metaDataBuilder.put(targetIndexMetadata, true);
            blocksBuilder.removeIndexBlocks(sourceIndex);
            blocksBuilder.addBlocks(targetIndexMetadata);
            routingBuilder.remove(sourceIndex);
            routingBuilder.addAsFromCloseToOpen(targetIndexMetadata);
        }
    }
}
