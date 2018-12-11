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

import io.crate.execution.ddl.index.SwapAndDropIndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;

public class SwapAndDropIndexExecutor extends DDLClusterStateTaskExecutor<SwapAndDropIndexRequest> {

    private final AllocationService allocationService;

    public SwapAndDropIndexExecutor(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    public ClusterState execute(ClusterState currentState, SwapAndDropIndexRequest request) throws Exception {
        final MetaData metaData = currentState.getMetaData();
        final ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
        final MetaData.Builder mdBuilder = MetaData.builder(metaData);
        final RoutingTable.Builder routingBuilder = RoutingTable.builder(currentState.routingTable());

        String sourceIndexName = request.source();
        String targetIndexName = request.target();

        mdBuilder.remove(sourceIndexName);
        mdBuilder.remove(targetIndexName);
        routingBuilder.remove(sourceIndexName);
        routingBuilder.remove(targetIndexName);
        blocksBuilder.removeIndexBlocks(sourceIndexName);
        blocksBuilder.removeIndexBlocks(targetIndexName);

        IndexMetaData sourceIndex = metaData.index(sourceIndexName);
        if (sourceIndex == null) {
            throw new IllegalArgumentException("Source index must exist: " + sourceIndexName);
        }

        IndexMetaData newIndexMetaData = IndexMetaData.builder(sourceIndex).index(targetIndexName).build();
        mdBuilder.put(newIndexMetaData, true);
        routingBuilder.addAsFromCloseToOpen(newIndexMetaData);
        blocksBuilder.addBlocks(newIndexMetaData);

        return allocationService.reroute(
            ClusterState.builder(currentState)
                .metaData(mdBuilder)
                .routingTable(routingBuilder.build())
                .blocks(blocksBuilder)
                .build(),
            "swap and drop index"
        );
    }
}
