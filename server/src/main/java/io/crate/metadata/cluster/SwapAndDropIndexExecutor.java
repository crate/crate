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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;

import io.crate.execution.ddl.index.SwapAndDropIndexRequest;

public class SwapAndDropIndexExecutor extends DDLClusterStateTaskExecutor<SwapAndDropIndexRequest> {

    private final AllocationService allocationService;

    public SwapAndDropIndexExecutor(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    public ClusterState execute(ClusterState currentState, SwapAndDropIndexRequest request) throws Exception {
        final Metadata metadata = currentState.metadata();
        final ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
        final Metadata.Builder mdBuilder = Metadata.builder(metadata);
        final RoutingTable.Builder routingBuilder = RoutingTable.builder(currentState.routingTable());

        String sourceIndexName = request.source();
        String targetIndexName = request.target();

        mdBuilder.remove(sourceIndexName);
        mdBuilder.remove(targetIndexName);
        routingBuilder.remove(sourceIndexName);
        routingBuilder.remove(targetIndexName);
        blocksBuilder.removeIndexBlocks(sourceIndexName);
        blocksBuilder.removeIndexBlocks(targetIndexName);

        IndexMetadata sourceIndex = metadata.index(sourceIndexName);
        if (sourceIndex == null) {
            throw new IllegalArgumentException("Source index must exist: " + sourceIndexName);
        }

        IndexRoutingTable index = currentState.routingTable().index(sourceIndex.getIndex());
        if (!index.allPrimaryShardsActive()) {
            throw new UnsupportedOperationException(
                "Cannot swap and drop index if source '" + sourceIndexName + "' has unallocated primaries");
        }

        IndexMetadata newIndexMetadata = IndexMetadata.builder(sourceIndex).index(targetIndexName).build();
        mdBuilder.put(newIndexMetadata, true);
        routingBuilder.addAsFromCloseToOpen(newIndexMetadata);
        blocksBuilder.addBlocks(newIndexMetadata);

        return allocationService.reroute(
            ClusterState.builder(currentState)
                .metadata(mdBuilder)
                .routingTable(routingBuilder.build())
                .blocks(blocksBuilder)
                .build(),
            "swap and drop index"
        );
    }
}
